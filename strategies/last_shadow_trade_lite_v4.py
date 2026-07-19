import logging
import os
import time
import uuid
import json
import asyncio
from datetime import datetime, timezone
from .utils import create_base_signal_direct
from dashboard import state_provider
from clob_executor import ClobExecutor
import execution_journal
from clob_redeemer import auto_redeem

logger = logging.getLogger(__name__)
# Strategy is enabled directly in signal_engine.py — no monkeypatching needed.

# Shared execution layer. Paper mode simulates fills; live posts real FAK orders and
# records fill-vs-expected / fee / slippage to the execution journal.
_executor = ClobExecutor()
#
# ARCHITECTURE (v4 time-driven):
#   The previous version drove its observation/evaluation/execution phases from
#   classify(), which only ran when a Polymarket WebSocket tick happened to land
#   in the right 5-second sub-window. In practice ~71% of windows were missed
#   because no BTC tick arrived in the final 10 seconds (logged as "no_signal").
#
#   This version is TIME-DRIVEN: last_shadow_driver() polls the live BTC feed
#   every poll_interval seconds and runs the lifecycle off the market's TRUE
#   time-to-resolution (TTR, derived from resolution_time_ms), keyed by
#   market_id. classify() is now a no-op that always returns SKIP so the signal
#   engine flow is unchanged.

# Global configuration defaults.  Thresholds are expressed in TTR seconds.
CONFIG = {
    "enabled": True,
    "min_price_threshold": 0.94,   # winning side must be >= this at observation
    "position_size_usdc": 50.0,
    "obs_ttr_high": 15,            # begin observing when TTR drops to 15s
    "obs_ttr_low": 10,             # stop observing / evaluate when TTR drops below 10s
    "exec_ttr_floor": 4,          # do not enter with fewer than this many seconds left
}

# ── Bounded per-market state, keyed by market_id ─────────────────────────────
WINDOW_STATE: dict = {}
_MAX_TRACKED_MARKETS = 18          # 3 assets × several in-flight/resolving markets
# SOL excluded by default: thin book + frequent last-second reversals make it
# structurally -EV for a penny-carry strategy (one -$50 reversal erases ~100 SOL
# wins). Re-enable via LS_ASSETS=BTC,ETH,SOL once the reversal_log data justifies it.
_ASSETS = tuple(a.strip().upper() for a in os.getenv("LS_ASSETS", "BTC,ETH").split(",") if a.strip())
_current_market_id: dict = {}      # asset -> current market_id


# ── Live risk controls (env-driven; defaults preserve paper behaviour) ────────
# These gate real orders. When the env vars are unset the caps are effectively
# infinite and the kill switch is off, so paper mode behaves exactly as before.
_daily_pnl_cache = {"day": None, "pnl": 0.0, "ts": 0.0}


def _position_size() -> float:
    """Per-trade size: PER_TRADE_USDC env overrides the CONFIG default."""
    v = os.getenv("PER_TRADE_USDC")
    if v:
        try:
            return float(v)
        except ValueError:
            pass
    return CONFIG["position_size_usdc"]


def _kill_switch_active() -> bool:
    """Honour both the KILL_SWITCH env var and the DB system_config flag."""
    if str(os.getenv("KILL_SWITCH", "false")).lower() == "true":
        return True
    try:
        if str(state_provider.get_config("kill_switch", False)).lower() == "true":
            return True
    except Exception:
        pass
    return False


def _open_exposure_usdc() -> float:
    """Sum of filled, not-yet-resolved Last Shadow positions (from in-memory state,
    since these positions bypass the shared open_positions list).

    Self-healing: a position only counts toward the concurrency cap for a bounded
    lifetime. A 5-min market plus resolution settles well within EXPOSURE_MAX_AGE_SEC;
    anything older is either already resolved or a zombie whose resolve_market() never
    fired (e.g. its market stopped rolling over). Counting zombies forever would pin the
    MAX_CONCURRENT_USDC cap and silently block ALL new entries, so we age them out."""
    max_age = float(os.getenv("EXPOSURE_MAX_AGE_SEC", "360"))   # 6 min: 5-min window + buffer
    now = time.time()
    total = 0.0
    for st in WINDOW_STATE.values():
        if st.get("order_filled") and not st.get("logged"):
            filled_at = st.get("filled_at") or st.get("created_at") or now
            if now - filled_at > max_age:
                continue   # stale/zombie — do not let it block new entries
            total += float(st.get("size_usdc") or CONFIG["position_size_usdc"])
    return total


async def _daily_realized_pnl() -> float:
    """Today's realized Last Shadow PnL, cached for 30s to keep the hot path light."""
    now = time.time()
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _daily_pnl_cache["day"] == day and (now - _daily_pnl_cache["ts"]) < 30:
        return _daily_pnl_cache["pnl"]
    pool = getattr(state_provider.db_manager, "pool", None) if state_provider.db_manager else None
    if pool is None:
        return _daily_pnl_cache["pnl"]
    try:
        async with pool.acquire(timeout=5.0) as conn:
            v = await conn.fetchval(
                "SELECT COALESCE(SUM(pnl),0) FROM positions "
                "WHERE strategy_type='LAST_SHADOW_TRADE_LITE_V4' AND status='CLOSED' "
                "AND closed_at >= CURRENT_DATE"
            )
        _daily_pnl_cache.update(day=day, pnl=float(v or 0.0), ts=now)
    except Exception as e:
        logger.error(f"[LAST_SHADOW] daily pnl query failed: {e}")
    return _daily_pnl_cache["pnl"]


# ── Reversal circuit-breaker ─────────────────────────────────────────────────
# Last Shadow is a fat-tail carry trade: a cluster of reversals (a choppy regime
# sitting on the candle open) can cascade. If N reversals happen within a rolling
# window, pause new entries for a cooldown so a bad regime can't compound.
_reversal_times: list = []
_breaker_until: float = 0.0


def _record_reversal():
    """Called when a Last Shadow trade resolves as a LOSS (a reversal)."""
    _reversal_times.append(time.time())


def _circuit_breaker_active() -> bool:
    global _breaker_until
    now = time.time()
    window = float(os.getenv("CIRCUIT_BREAKER_WINDOW_SEC", "1800"))       # 30 min
    threshold = int(os.getenv("CIRCUIT_BREAKER_REVERSALS", "2"))
    cooldown = float(os.getenv("CIRCUIT_BREAKER_COOLDOWN_SEC", "3600"))   # 1 h

    # Drop reversals outside the rolling window.
    cutoff = now - window
    while _reversal_times and _reversal_times[0] < cutoff:
        _reversal_times.pop(0)

    if len(_reversal_times) >= threshold:
        newly = _breaker_until <= now
        _breaker_until = max(_breaker_until, _reversal_times[-1] + cooldown)
        if newly:
            logger.warning(
                f"[LAST_SHADOW] CIRCUIT BREAKER TRIPPED — {len(_reversal_times)} reversals "
                f"in {window/60:.0f}m; pausing entries for {cooldown/60:.0f}m"
            )
            tg = getattr(state_provider, "telegram_callback", None)
            if tg:
                msg = f"🛑 [LAST_SHADOW] CIRCUIT BREAKER — {len(_reversal_times)} reversals, entries paused"
                if asyncio.iscoroutinefunction(tg):
                    asyncio.create_task(tg(msg))
                else:
                    tg(msg)
        return True

    return now < _breaker_until


async def _risk_blocks_entry(asset: str, size_usdc: float):
    """Return a reason string if an entry must be blocked, else None."""
    if _kill_switch_active():
        return "kill_switch"

    if _circuit_breaker_active():
        return "circuit_breaker"

    max_conc = os.getenv("MAX_CONCURRENT_USDC")
    if max_conc:
        try:
            if _open_exposure_usdc() + size_usdc > float(max_conc):
                return "max_concurrent_usdc"
        except ValueError:
            pass

    daily_limit = os.getenv("DAILY_LOSS_LIMIT_USDC")
    if daily_limit:
        try:
            if await _daily_realized_pnl() <= -abs(float(daily_limit)):
                return "daily_loss_limit"
        except ValueError:
            pass

    return None


def _prune_market_state():
    """Keep only the most-recent markets so WINDOW_STATE can't grow unbounded."""
    if len(WINDOW_STATE) <= _MAX_TRACKED_MARKETS:
        return
    # Drop the oldest entries by first-seen order (insertion order is preserved in dicts).
    for mid in list(WINDOW_STATE)[:-_MAX_TRACKED_MARKETS]:
        WINDOW_STATE.pop(mid, None)


def _new_state(market_id: str, asset: str) -> dict:
    return {
        "market_id": market_id,
        "asset": asset,
        "entry_side": None,
        "entry_price": None,
        "winning_side_price_at_obs": 0.0,
        "signal_valid": False,
        "order_placed": False,
        "order_filled": False,
        "fill_price": None,
        "resolved_winner": None,
        "pnl": 0.0,
        "entry_correct": False,
        "skipped_reason": None,
        "logged": False,
        "observation_evaluated": False,
        "latest_yes_price": None,
        "latest_no_price": None,
        "sig_id": None,
        "pos_id": None,
        "yes_token_id": None,   # outcome tokens (from feed) — needed for LIVE orders
        "no_token_id": None,
        "condition_id": None,  # Polymarket conditionId — needed for auto-redeem
        "size_usdc": None,     # actual size used for this position (PER_TRADE_USDC-aware)
        "window_ts": None,     # 5-min window open boundary (for spot-based resolution)
        "filled_at": None,     # wall-clock of the fill (for exposure aging)
        "created_at": time.time(),
    }


# ── DB helpers ────────────────────────────────────────────────────────────────

async def db_insert_signal(db_manager, sig: dict):
    query = """
    INSERT INTO signals (
        id, asset, strategy_type, direction, grade, yes_price, no_price,
        market_id, outcome, skip_reason, fired_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    """
    async with db_manager.pool.acquire(timeout=5.0) as conn:
        await conn.execute(
            query,
            uuid.UUID(sig["id"]), sig["asset"], sig["strategy_type"],
            sig["direction"], sig["grade"], sig["yes_price"], sig["no_price"],
            sig["market_id"], sig["outcome"], sig["skip_reason"], sig["fired_at"],
        )


_condition_id_col_ensured = False


async def _ensure_condition_id_column(db_manager):
    """Run the condition_id migration ONCE per process. Repeating ALTER TABLE on every
    fill takes a brief exclusive lock each time — costly on a DB shared with other systems."""
    global _condition_id_col_ensured
    if _condition_id_col_ensured:
        return
    try:
        async with db_manager.pool.acquire(timeout=5.0) as conn:
            await conn.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS condition_id TEXT")
        _condition_id_col_ensured = True
    except Exception as e:
        logger.error(f"[LAST_SHADOW] condition_id column migration failed: {e}")


async def db_insert_position(db_manager, pos: dict):
    await _ensure_condition_id_column(db_manager)
    async with db_manager.pool.acquire(timeout=5.0) as conn:
        await conn.execute(
            """
            INSERT INTO positions (
                id, signal_id, strategy_type, market_id, asset, direction, entry_price,
                size_usdc, entry_mode, dca_rounds_completed, status, exit_price, pnl,
                close_reason, is_paper, opened_at, closed_at, condition_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
            """,
            uuid.UUID(pos["id"]),
            uuid.UUID(pos["signal_id"]) if pos.get("signal_id") else None,
            pos["strategy_type"], pos["market_id"], pos["asset"], pos["direction"],
            pos["entry_price"], pos["size_usdc"], pos["entry_mode"],
            pos["dca_rounds_completed"], pos["status"],
            pos.get("exit_price"), pos.get("pnl"), pos.get("close_reason"),
            pos["is_paper"], pos["opened_at"], pos.get("closed_at"),
            pos.get("condition_id"),
        )


async def update_strategy_stats_in_db(db_manager, pnl: float = None, won: bool = None):
    """
    Recompute LAST_SHADOW_TRADE_LITE_V4 stats authoritatively from the positions
    table and upsert them. Recomputing (rather than incrementing) means the row is
    always correct regardless of restarts or other writers — it cannot drift to 0.
    The pnl/won args are accepted for backward compatibility but are not used.
    """
    query_compute = """
        SELECT
            COUNT(*)                                AS trades_total,
            COUNT(*) FILTER (WHERE pnl > 0)         AS wins,
            COUNT(*) FILTER (WHERE pnl < 0)         AS losses,
            COALESCE(SUM(pnl), 0)                   AS total_pnl
        FROM positions
        WHERE strategy_type = 'LAST_SHADOW_TRADE_LITE_V4' AND status = 'CLOSED'
    """
    query_upsert = """
    INSERT INTO strategy_stats (
        strategy_type, trades_total, wins, losses, win_rate, total_pnl,
        avg_pnl_per_trade, last_updated
    ) VALUES ('LAST_SHADOW_TRADE_LITE_V4',$1,$2,$3,$4,$5,$6,NOW())
    ON CONFLICT (strategy_type) DO UPDATE SET
        trades_total     = EXCLUDED.trades_total,
        wins             = EXCLUDED.wins,
        losses           = EXCLUDED.losses,
        win_rate         = EXCLUDED.win_rate,
        total_pnl        = EXCLUDED.total_pnl,
        avg_pnl_per_trade= EXCLUDED.avg_pnl_per_trade,
        last_updated     = NOW()
    """
    try:
        async with db_manager.pool.acquire(timeout=5.0) as conn:
            row = await conn.fetchrow(query_compute)
            trades_total = row["trades_total"] or 0
            wins   = row["wins"] or 0
            losses = row["losses"] or 0
            total_pnl = float(row["total_pnl"] or 0.0)
            win_rate = wins / trades_total if trades_total else 0.0
            avg_pnl  = total_pnl / trades_total if trades_total else 0.0
            await conn.execute(
                query_upsert, trades_total, wins, losses, win_rate, total_pnl, avg_pnl
            )
    except Exception as e:
        logger.error(f"[LAST_SHADOW] Failed to update strategy stats: {e}")


# ── Order fill (via CLOB executor — paper simulates, live posts real order) ───

async def _fill_order(state: dict, decision_staleness_ms: int = 0):
    """Place the entry order through the execution layer, then persist the signal,
    the (filled) position at the ACTUAL fill price, and the execution-cost journal row.
    `decision_staleness_ms` = feed staleness at decision time (tick->decision latency)."""
    market_id  = state["market_id"]
    asset      = state["asset"]
    entry_side = state["entry_side"]
    entry_price = state["entry_price"]
    win_obs    = state["winning_side_price_at_obs"]
    size_usdc  = float(state.get("size_usdc") or _position_size())

    # token_id of the outcome we are buying: YES token for BUY_YES, NO token for BUY_NO.
    # None in paper (simulated); populated for live from the feed's clobTokenIds.
    token_id = state.get("yes_token_id") if entry_side == "BUY_YES" else state.get("no_token_id")

    # LIVE guard: a real order needs a real outcome token. If the feed never surfaced it
    # (sparse ticks in the observation window), don't fire a doomed token_id=None order —
    # skip this window cleanly so we never send a malformed live order.
    if str(os.getenv("PAPER_MODE", "true")).lower() == "false" and not token_id:
        state["order_placed"] = False
        state["skipped_reason"] = "no_token_id"
        logger.warning(f"[LAST_SHADOW] {asset} live entry skipped — outcome token_id unavailable")
        return

    fill = await _executor.place_order(
        token_id=token_id, requested_price=entry_price, size_usdc=size_usdc, asset=asset
    )
    if not fill.filled:
        # Idempotency / double-submit safety: only allow a retry when the exchange
        # DEFINITIVELY did not execute (UNFILLED/REJECTED). On ERROR the outcome is
        # ambiguous — the order may have executed but the response was lost — so we do
        # NOT retry, or we could open a duplicate position.
        if fill.status in ("UNFILLED", "REJECTED"):
            state["order_placed"] = False                     # safe to retry this window
            state["skipped_reason"] = f"order_{fill.status.lower()}"
        else:                                                 # ERROR / unknown → terminal
            state["skipped_reason"] = f"order_{fill.status.lower()}_no_retry"
            logger.error(f"[LAST_SHADOW] {asset} order {fill.status} — NOT retrying "
                         f"(ambiguous execution state) — market {market_id}")
        logger.warning(f"[LAST_SHADOW] {asset} order not filled ({fill.status}) — market {market_id}")
        return

    fill_price = fill.fill_price          # == requested in paper; real fill in live
    state["order_filled"] = True
    state["fill_price"]   = fill_price
    state["filled_at"]    = time.time()   # for exposure aging (self-healing concurrency cap)

    sig_id = str(uuid.uuid4())
    state["sig_id"] = sig_id
    now_utc = datetime.now(timezone.utc)
    sig = {
        "id": sig_id,
        "asset": asset,
        "strategy_type": "LAST_SHADOW_TRADE_LITE_V4",
        "direction": entry_side,
        "grade": "A",
        "yes_price": win_obs if entry_side == "BUY_YES" else round(1.0 - win_obs, 4),
        "no_price":  round(1.0 - win_obs, 4) if entry_side == "BUY_YES" else win_obs,
        "market_id": market_id,
        "outcome": "EXECUTED",
        "skip_reason": None,
        "fired_at": now_utc,
    }

    pos_id = str(uuid.uuid4())
    state["pos_id"] = pos_id
    pos = {
        "id": pos_id,
        "signal_id": sig_id,
        "strategy_type": "LAST_SHADOW_TRADE_LITE_V4",
        "market_id": market_id,
        "asset": asset,
        "direction": entry_side,
        "entry_price": fill_price,
        "size_usdc": size_usdc,
        "entry_mode": "SINGLE",
        "dca_rounds_completed": 1,
        "status": "OPEN",
        "is_paper": fill.is_paper,
        "opened_at": now_utc,
        "closed_at": None,
        "condition_id": state.get("condition_id"),
    }

    # Chainlink-proxy conviction data (Phase 2a instrumentation — logged, not gated).
    # margin_pct > 0 => spot above window-open => favors Up/BUY_YES.
    margin = None
    spot_agrees = None
    sf = getattr(state_provider, "spot_feed", None)
    if sf is not None:
        margin = sf.get_margin(asset)
        if margin is not None:
            mp = margin["margin_pct"]
            spot_agrees = (entry_side == "BUY_YES" and mp > 0) or (entry_side == "BUY_NO" and mp < 0)
            state["window_ts"] = margin["window_ts"]
    # Fallback window boundary if the spot feed wasn't ready (fill is ~final seconds,
    # so floor(now/300) is this window's open).
    if state.get("window_ts") is None:
        state["window_ts"] = int(time.time() - (time.time() % 300))

    if state_provider.db_manager:
        await db_insert_signal(state_provider.db_manager, sig)
        await db_insert_position(state_provider.db_manager, pos)
        await execution_journal.record_fill(
            state_provider.db_manager.pool,
            position_id=pos_id, signal_id=sig_id,
            strategy_type="LAST_SHADOW_TRADE_LITE_V4",
            asset=asset, direction=entry_side, fill=fill,
            decision_staleness_ms=decision_staleness_ms,
            margin=margin, spot_agrees=spot_agrees,
        )

    logger.info(
        f"[LAST_SHADOW] {asset} {'PAPER' if fill.is_paper else 'LIVE'} FILLED — "
        f"{entry_side} req={entry_price} fill={fill_price} slip={fill.slippage_usdc} "
        f"fee={fill.fee_usdc} for market {market_id}"
    )


# ── Resolution ────────────────────────────────────────────────────────────────

async def resolve_market(market_id: str):
    """Resolve a finished 5-minute market and book PnL / log the skip."""
    try:
        state = WINDOW_STATE.get(market_id)
        if state is None or state["logged"]:
            return

        if not state["order_filled"]:
            # No trade taken — log the skip so the strategy is visible in the DB.
            sig_id = str(uuid.uuid4())
            sig = {
                "id": sig_id,
                "asset": state.get("asset", "BTC"),
                "strategy_type": "LAST_SHADOW_TRADE_LITE_V4",
                "direction": "NONE",
                "grade": "SKIP",
                "yes_price": state["winning_side_price_at_obs"],
                "no_price":  round(1.0 - (state["winning_side_price_at_obs"] or 0.0), 4),
                "market_id": market_id,
                "outcome": "SKIPPED",
                "skip_reason": state["skipped_reason"] or "no_signal",
                "fired_at": datetime.now(timezone.utc),
            }
            if state_provider.db_manager:
                await db_insert_signal(state_provider.db_manager, sig)
            logger.info(
                f"[LAST_SHADOW] Market {market_id} skipped — {sig['skip_reason']}"
            )
            state["logged"] = True
            return

        entry_side = state["entry_side"]
        fill_price = state["fill_price"]
        resolved_winner = None

        # Poll up to ~4 minutes (24 × 10s) — UMA settlement of a 5-min market can
        # take a couple of minutes after the market ends.
        async def _poll_resolution():
            nonlocal resolved_winner
            for _ in range(24):
                try:
                    if await state_provider.is_market_resolved(market_id):
                        # Resolved YES (Up) price. Real settled prices are ~0.999/~0.001,
                        # never exactly 1.0/0.0, so use thresholds not equality.
                        res_price = await state_provider.get_resolution_price(market_id, "BUY_YES")
                        if res_price >= 0.9:
                            resolved_winner = "UP"
                        elif res_price <= 0.1:
                            resolved_winner = "DOWN"
                        if resolved_winner is not None:
                            return
                except Exception as e:
                    logger.error(f"[LAST_SHADOW] Resolution check error: {e}")
                await asyncio.sleep(10)

        try:
            await asyncio.wait_for(_poll_resolution(), timeout=250.0)
        except asyncio.TimeoutError:
            logger.warning(f"[LAST_SHADOW] Resolution poll timed out for {market_id}")

        # ── Spot fallback: Gamma is slow/flaky, but the market resolves on Chainlink
        # (Up if close >= open). Derive the winner from our own spot feed so we never
        # book a false $0 timeout on a binary market. ─────────────────────────────
        if resolved_winner is None:
            sf = getattr(state_provider, "spot_feed", None)
            wts = state.get("window_ts")
            if sf is not None and wts:
                resolved_winner = sf.get_window_resolution(state["asset"], wts)
                if resolved_winner is not None:
                    logger.info(
                        f"[LAST_SHADOW] {market_id} resolved via SPOT fallback: "
                        f"{resolved_winner} (Gamma unavailable)"
                    )

        if resolved_winner is None:
            # Both Gamma and spot unavailable — close at break-even, record truthfully.
            logger.warning(
                f"[LAST_SHADOW] Could not determine winner for {market_id} — pnl=0"
            )
            if state_provider.db_manager and state.get("pos_id"):
                try:
                    async with state_provider.db_manager.pool.acquire(timeout=5.0) as conn:
                        await conn.execute(
                            "UPDATE positions SET status='CLOSED', exit_price=0.0, pnl=0.0, "
                            "close_reason='RESOLUTION_TIMEOUT', closed_at=NOW() WHERE id=$1",
                            uuid.UUID(state["pos_id"]),
                        )
                except Exception as e:
                    logger.error(f"[LAST_SHADOW] Failed to close timed-out position: {e}")
            state["logged"] = True
            return

        entry_correct = (
            (entry_side == "BUY_YES" and resolved_winner == "UP") or
            (entry_side == "BUY_NO"  and resolved_winner == "DOWN")
        )
        size = float(state.get("size_usdc") or CONFIG["position_size_usdc"])
        pnl = round(((1.0 - fill_price) * size) / fill_price, 4) if entry_correct else -size
        if not entry_correct:
            _record_reversal()

        if state_provider.db_manager and state.get("pos_id"):
            try:
                async with state_provider.db_manager.pool.acquire(timeout=5.0) as conn:
                    await conn.execute(
                        "UPDATE positions SET status='CLOSED', exit_price=$2, pnl=$3, "
                        "close_reason='RESOLUTION', closed_at=NOW() WHERE id=$1",
                        uuid.UUID(state["pos_id"]),
                        1.0 if entry_correct else 0.0,
                        pnl,
                    )
                await update_strategy_stats_in_db(state_provider.db_manager, pnl, entry_correct)
            except Exception as e:
                logger.error(f"[LAST_SHADOW] Failed to close position in DB: {e}")

        # Auto-redeem winning position so USDC returns to the wallet immediately.
        if entry_correct:
            asyncio.create_task(auto_redeem(
                condition_id=state.get("condition_id"),
                asset=state["asset"],
                pnl=pnl,
            ))

        if state_provider.telegram_callback:
            msg = (
                f"⚡ [LAST_SHADOW] TRADE RESOLVED — "
                f"{'WIN' if entry_correct else 'LOSS'} BTC {entry_side} PnL: {pnl:+.2f} USDC"
            )
            if asyncio.iscoroutinefunction(state_provider.telegram_callback):
                asyncio.create_task(state_provider.telegram_callback(msg))
            else:
                state_provider.telegram_callback(msg)

        logger.info(
            f"[LAST_SHADOW] Market {market_id} resolved — "
            f"{'WIN' if entry_correct else 'LOSS'} pnl={pnl:+.2f}"
        )
        state["logged"] = True

    except Exception as e:
        logger.error(f"[LAST_SHADOW] Resolution processing failed: {e}", exc_info=True)


# ── Time-driven driver (replaces tick-driven phases) ─────────────────────────

async def last_shadow_settlement_sweep(interval: float = 60.0):
    """
    Safety net launched from main.py. Handles two failure modes:
      1. RESOLUTION_TIMEOUT — the live 250s resolution poll gave up (pnl=0) even
         though the market actually won/lost; re-settle once resolvable.
      2. Stuck OPEN zombies — a filled position whose resolve_market() never fired
         (e.g. its market stopped rolling over) and is still OPEN after >7 min.
         Left alone these linger OPEN forever and (pre-fix) pinned the concurrency
         cap. Here we force-settle them via Gamma → spot fallback.
    """
    logger.info("[LAST_SHADOW] Settlement sweep started")
    while True:
        try:
            await asyncio.sleep(interval)
            db = state_provider.db_manager
            if not db or not getattr(db, "pool", None):
                continue

            async with db.pool.acquire(timeout=5.0) as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, market_id, asset, direction, entry_price, size_usdc,
                           opened_at, condition_id
                    FROM positions
                    WHERE strategy_type = 'LAST_SHADOW_TRADE_LITE_V4'
                      AND opened_at > NOW() - INTERVAL '60 minutes'
                      AND (
                        close_reason = 'RESOLUTION_TIMEOUT'
                        OR (status = 'OPEN' AND opened_at < NOW() - INTERVAL '7 minutes')
                      )
                    """
                )

            sf = getattr(state_provider, "spot_feed", None)
            settled = 0
            for r in rows:
                mid = r["market_id"]
                try:
                    state_provider._resolution_cache.pop(mid, None)
                except Exception:
                    pass
                try:
                    # Try Gamma first, then the spot fallback (Chainlink rule).
                    winner = None
                    if await state_provider.is_market_resolved(mid):
                        yes_p = await state_provider.get_resolution_price(mid, "BUY_YES")
                        winner = "UP" if yes_p >= 0.9 else ("DOWN" if yes_p <= 0.1 else None)
                    if winner is None and sf is not None and r["opened_at"]:
                        wts = int(r["opened_at"].timestamp() - (r["opened_at"].timestamp() % 300))
                        winner = sf.get_window_resolution(r["asset"], wts)
                    if winner is None:
                        continue
                    entry_correct = (
                        (r["direction"] == "BUY_YES" and winner == "UP") or
                        (r["direction"] == "BUY_NO" and winner == "DOWN")
                    )
                    fill = float(r["entry_price"])
                    size = float(r["size_usdc"])
                    pnl = round((1.0 - fill) / fill * size, 4) if entry_correct else -size
                    if not entry_correct:
                        _record_reversal()
                    async with db.pool.acquire(timeout=5.0) as conn:
                        await conn.execute(
                            "UPDATE positions SET status='CLOSED', exit_price=$2, pnl=$3, "
                            "close_reason='RESOLUTION', "
                            "closed_at=COALESCE(closed_at, NOW()) WHERE id=$1",
                            r["id"], 1.0 if entry_correct else 0.0, pnl,
                        )
                    if entry_correct:
                        asyncio.create_task(auto_redeem(
                            condition_id=r.get("condition_id"),
                            asset=r["asset"],
                            pnl=pnl,
                        ))
                    # Mark the in-memory state resolved so a late-firing resolve_market()
                    # can't re-process this position (double reversal / double redeem).
                    st = WINDOW_STATE.get(mid)
                    if st is not None:
                        st["logged"] = True
                    settled += 1
                except Exception as e:
                    logger.error(f"[LAST_SHADOW] Sweep settle error for {mid}: {e}")

            if settled:
                await update_strategy_stats_in_db(db)
                logger.info(f"[LAST_SHADOW] Settlement sweep re-settled {settled} position(s)")

        except asyncio.CancelledError:
            logger.info("[LAST_SHADOW] Settlement sweep cancelled — shutting down")
            raise
        except Exception as e:
            logger.error(f"[LAST_SHADOW] Settlement sweep error: {e}", exc_info=True)


async def _drive_asset(asset: str, feed):
    """Run the observation → evaluation → execution lifecycle for one asset's
    current 5-minute market. Called once per poll for each of BTC/ETH/SOL."""
    poly = feed.get_market_state(asset)
    if not poly or not poly.get("has_live_data"):
        return

    market_id = poly.get("market_id")
    if not market_id:
        return

    ttr   = poly.get("time_to_resolution_seconds", 999)
    yes_p = poly.get("yes_price")
    no_p  = poly.get("no_price")

    # ── Market rollover: a new market_id for this asset means the previous one ended ──
    if market_id != _current_market_id.get(asset):
        prev_id = _current_market_id.get(asset)
        _current_market_id[asset] = market_id
        if market_id not in WINDOW_STATE:
            WINDOW_STATE[market_id] = _new_state(market_id, asset)
            _prune_market_state()
        if prev_id and prev_id in WINDOW_STATE:
            asyncio.create_task(resolve_market(prev_id))

    state = WINDOW_STATE.get(market_id)
    if state is None:
        state = _new_state(market_id, asset)
        WINDOW_STATE[market_id] = state

    # ── Phase 1: Observation — record prices while TTR in [low, high] ────
    if CONFIG["obs_ttr_low"] <= ttr <= CONFIG["obs_ttr_high"]:
        if yes_p is not None:
            state["latest_yes_price"] = yes_p
        if no_p is not None:
            state["latest_no_price"] = no_p
        # Capture the outcome tokens if the feed exposes them (needed for LIVE orders).
        if poly.get("yes_token_id"):
            state["yes_token_id"] = poly.get("yes_token_id")
        if poly.get("no_token_id"):
            state["no_token_id"] = poly.get("no_token_id")
        if poly.get("condition_id"):
            state["condition_id"] = poly.get("condition_id")

    # ── Phase 2: Evaluation — first poll after TTR drops below obs_ttr_low ─
    if ttr < CONFIG["obs_ttr_low"] and not state["observation_evaluated"]:
        yp, np_ = state["latest_yes_price"], state["latest_no_price"]
        if yp is not None and np_ is not None:
            winning = max(yp, np_)
            state["winning_side_price_at_obs"] = winning
            # Fee/slippage-adjusted EV gate: a win pays only (1 - price). If that reward is
            # thinner than the estimated round-trip execution cost, the trade is -EV before
            # it even starts — skip it. MIN_EDGE_BUFFER=0 (default) makes this a no-op until
            # you set it from real execution_journal slippage data. Note: this only blocks
            # *thin-reward* entries; it does not fix a strategy whose loss RATE is too high.
            min_edge = float(os.getenv("MIN_EDGE_BUFFER", "0.0"))
            # Displacement gate: a 0.99 favorite is only trustworthy when spot has
            # actually displaced from the window open. A flat window (|disp| < gate)
            # is a coin flip being priced like 99/2 - that subset carries nearly all
            # of Last Shadow's -$50 reversals. Skip flat windows; also skip when the
            # spot feed can't tell us (conservative: no reading = no trade).
            min_disp_bp = float(os.getenv("LS_MIN_DISPLACEMENT_BP", "3.0"))
            disp_pct = None
            _sf = getattr(state_provider, "spot_feed", None)
            if _sf is not None:
                _m = _sf.get_margin(asset)
                if _m is not None:
                    disp_pct = _m["margin_pct"]
            if winning < CONFIG["min_price_threshold"]:
                state["skipped_reason"] = "price_below_threshold"
            elif (1.0 - winning) < min_edge:
                state["skipped_reason"] = "thin_edge_ev_gate"
            elif disp_pct is None:
                state["skipped_reason"] = "no_spot_margin"
            elif abs(disp_pct) < min_disp_bp / 100.0:
                state["skipped_reason"] = "flat_displacement"
            elif ((disp_pct > 0) != (yp >= np_)):
                state["skipped_reason"] = "spot_book_disagree"
            else:
                state["signal_valid"] = True
                if yp >= np_:
                    state["entry_side"], state["entry_price"] = "BUY_YES", yp
                else:
                    state["entry_side"], state["entry_price"] = "BUY_NO", np_
        else:
            state["skipped_reason"] = "no_obs_data"
        state["observation_evaluated"] = True

    # ── Phase 3: Execution — enter while there is still time on the clock ─
    if (state["signal_valid"] and not state["order_placed"]
            and CONFIG["exec_ttr_floor"] <= ttr < CONFIG["obs_ttr_low"]):
        size = _position_size()
        block = await _risk_blocks_entry(asset, size)
        if block:
            # Don't retry this window; record why the risk layer stopped it.
            state["order_placed"] = True
            state["skipped_reason"] = f"risk_{block}"
            logger.warning(f"[LAST_SHADOW] {asset} entry blocked by risk gate: {block}")
        else:
            state["order_placed"] = True
            state["size_usdc"] = size
            staleness_ms = int(float(poly.get("staleness_seconds") or 0.0) * 1000)
            await _fill_order(state, decision_staleness_ms=staleness_ms)

    # ── Phase 4: Blackout — too little time left, abandon unfilled entry ──
    if ttr < CONFIG["exec_ttr_floor"] and state["signal_valid"] and not state["order_filled"]:
        if state["skipped_reason"] is None:
            state["skipped_reason"] = "blackout_no_fill"


async def last_shadow_driver(poll_interval: float = 0.5):
    """
    Background loop launched from main.py. Polls the live BTC/ETH/SOL feeds and
    drives the observation → evaluation → execution → blackout lifecycle off each
    market's true TTR, keyed by market_id. Robust to sparse WebSocket ticks.
    """
    logger.info("[LAST_SHADOW] Time-driven driver started (BTC/ETH/SOL)")

    while True:
        try:
            await asyncio.sleep(poll_interval)

            # Live config override (dashboard can tune thresholds at runtime).
            if state_provider and hasattr(state_provider, "config"):
                cfg = state_provider.config.get("last_shadow_trade_lite")
                if isinstance(cfg, dict):
                    CONFIG.update(cfg)

            if not CONFIG.get("enabled", True):
                continue

            feed = getattr(state_provider, "poly_feed", None)
            if feed is None:
                continue

            for asset in _ASSETS:
                try:
                    await _drive_asset(asset, feed)
                except Exception as e:
                    logger.error(f"[LAST_SHADOW] Driver error for {asset}: {e}", exc_info=True)

        except asyncio.CancelledError:
            logger.info("[LAST_SHADOW] Driver cancelled — shutting down")
            raise
        except Exception as e:
            logger.error(f"[LAST_SHADOW] Driver loop error: {e}", exc_info=True)


# ── classify() — now a no-op; lifecycle handled by last_shadow_driver() ──────

def classify(poly_state: dict) -> dict:
    """
    LAST_SHADOW_TRADE_LITE_V4 is time-driven (see last_shadow_driver). On the
    signal-engine tick path it always returns SKIP. The PHANTOM_TEST_MODE branch
    is retained so the existing unit tests keep exercising the entry logic.
    """
    if os.getenv("PHANTOM_TEST_MODE"):
        ttr = poly_state.get("time_to_resolution_seconds", 0)
        if not (5 <= ttr <= 15):
            sig = create_base_signal_direct("LAST_SHADOW_TRADE_LITE_V4", poly_state, "NONE")
            sig["grade"] = "SKIP"
            sig["skip_reason"] = f"TTR ({ttr}s) not in test window (5-15s)"
            return sig
        yes_price = poly_state.get("yes_price")
        no_price  = poly_state.get("no_price")
        if yes_price is not None and 0.94 <= yes_price <= 0.99:
            sig = create_base_signal_direct("LAST_SHADOW_TRADE_LITE_V4", poly_state, "BUY_YES")
            sig["grade"] = "A"
            return sig
        if no_price is not None and 0.94 <= no_price <= 0.99:
            sig = create_base_signal_direct("LAST_SHADOW_TRADE_LITE_V4", poly_state, "BUY_NO")
            sig["grade"] = "A"
            return sig
        sig = create_base_signal_direct("LAST_SHADOW_TRADE_LITE_V4", poly_state, "NONE")
        sig["grade"] = "SKIP"
        sig["skip_reason"] = "No momentum triggers"
        return sig

    sig = create_base_signal_direct("LAST_SHADOW_TRADE_LITE_V4", poly_state, "NONE")
    sig["grade"] = "SKIP"
    sig["skip_reason"] = "Self-contained strategy (time-driven)"
    return sig
