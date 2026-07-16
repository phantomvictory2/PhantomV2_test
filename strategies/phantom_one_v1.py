"""
phantom_one_v1.py — PHANTOM-1: volatility-gated favorite entry.

Derived from the July 2026 seven-wallet study of the Research_DB trade data
(2,259 markets / 444,857 fills). The evidence-backed rules implemented here:

  ENTRY (all must hold, evaluated every poll while in the entry window):
    1. Time:   30–120s elapsed in the 5-min window  →  TTR in [180, 270].
               (Confirmation zone: 0x3b43 won 93–94% there; first-15s edge
               belongs to faster bots, later entries have no liquidity.)
    2. Price:  leading side trades 0.55–0.70 (the only band that beat its
               implied probability for 5 of 7 profitable wallets).
    3. Volatility gate: spot has already moved ≥ MIN_DISPLACEMENT_BP from the
               window open, in the SAME direction as the side being bought
               (>10bp windows carried 63–87% of sniper PnL; early displacement
               is the ex-ante proxy).
    4. Skips:  leader > 0.85 (overpay), no clear leader (< 0.55), stale feed.

  SIZING:    full size in the 00:00–04:00 UTC block (where the sniper cohort's
             entire hour-block edge sat), half size otherwise.

  EXIT:      hold to resolution (≤ 4.5 min from entry). No in-market stop is
             possible on a 5-min binary with a thin book — risk is defined at
             entry (max loss = premium paid). Structural stops instead:
               • one entry per market, single fill
               • daily loss limit (PHANTOM1_DAILY_LOSS_USDC, default $75)
               • loss-cluster circuit breaker (default 6 losses / 30 min → 60 min pause)
               • shared kill switch (env + DB system_config)

Architecture is identical to last_shadow_trade_lite_v4: a TIME-DRIVEN driver
polled off the market's true TTR, self-contained execution/resolution, and a
settlement sweep as a safety net. Paper mode via ClobExecutor(PAPER_MODE=true).
"""

import logging
import os
import time
import uuid
import asyncio
from datetime import datetime, timezone

from dashboard import state_provider
from clob_executor import ClobExecutor
import execution_journal
from clob_redeemer import auto_redeem

# Reuse the generic DB writers from Last Shadow (they are strategy-agnostic:
# strategy_type is a field on the dict).
from .last_shadow_trade_lite_v4 import db_insert_signal, db_insert_position

logger = logging.getLogger(__name__)

STRATEGY = "PHANTOM_ONE_V1"

_executor = ClobExecutor()

# ── Configuration (env-overridable; defaults = paper-trade spec) ──────────────
CONFIG = {
    "enabled": True,
    "band_low": 0.55,            # leader price band (inclusive)
    "band_high": 0.70,
    "hard_price_cap": 0.85,      # never buy above this, ever
    "entry_ttr_high": 270,       # TTR 270s == 30s elapsed
    "entry_ttr_low": 180,        # TTR 180s == 120s elapsed
    "min_displacement_bp": 5.0,  # spot must have moved ≥ 5bp from window open
    "size_full_usdc": 25.0,      # 00:00–04:00 UTC block
    "size_half_usdc": 12.5,      # all other hours
    "prime_hours_utc": (0, 4),   # [start, end) of the full-size session
    "max_staleness_s": 5.0,      # skip if the Polymarket feed is stale
}

# BTC only by default — the wallet evidence is BTC; ETH/SOL were never studied.
_ASSETS = tuple(
    a.strip().upper()
    for a in os.getenv("PHANTOM1_ASSETS", "BTC").split(",")
    if a.strip()
)

WINDOW_STATE: dict = {}
_MAX_TRACKED_MARKETS = 18
_current_market_id: dict = {}

_daily_pnl_cache = {"day": None, "pnl": 0.0, "ts": 0.0}


# ── Sizing ────────────────────────────────────────────────────────────────────

def _position_size(now: float = None) -> float:
    """Session-weighted size. PHANTOM1_SIZE_USDC overrides the full size; the
    off-session size is always half of whatever the full size is."""
    full = CONFIG["size_full_usdc"]
    v = os.getenv("PHANTOM1_SIZE_USDC")
    if v:
        try:
            full = float(v)
        except ValueError:
            pass
    hour = datetime.now(timezone.utc).hour if now is None else \
        datetime.fromtimestamp(now, tz=timezone.utc).hour
    lo, hi = CONFIG["prime_hours_utc"]
    return full if lo <= hour < hi else round(full / 2.0, 2)


# ── Risk gates ────────────────────────────────────────────────────────────────

def _kill_switch_active() -> bool:
    if str(os.getenv("KILL_SWITCH", "false")).lower() == "true":
        return True
    try:
        if str(state_provider.get_config("kill_switch", False)).lower() == "true":
            return True
    except Exception:
        pass
    return False


def _open_exposure_usdc() -> float:
    """Filled, unresolved PHANTOM-1 exposure, with zombie aging (see LS v4)."""
    max_age = float(os.getenv("EXPOSURE_MAX_AGE_SEC", "360"))
    now = time.time()
    total = 0.0
    for st in WINDOW_STATE.values():
        if st.get("order_filled") and not st.get("logged"):
            filled_at = st.get("filled_at") or st.get("created_at") or now
            if now - filled_at > max_age:
                continue
            total += float(st.get("size_usdc") or 0.0)
    return total


async def _daily_realized_pnl() -> float:
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
                "WHERE strategy_type=$1 AND status='CLOSED' AND closed_at >= CURRENT_DATE",
                STRATEGY,
            )
        _daily_pnl_cache.update(day=day, pnl=float(v or 0.0), ts=now)
    except Exception as e:
        logger.error(f"[PHANTOM1] daily pnl query failed: {e}")
    return _daily_pnl_cache["pnl"]


# Loss-cluster circuit breaker. PHANTOM-1 expects ~35–40% losers even when
# healthy, so the threshold is a CLUSTER (default 6 in 30 min ≈ losing 6 of
# ~10 consecutive qualified windows), not the 2-loss trigger Last Shadow uses.
_loss_times: list = []
_breaker_until: float = 0.0


def _record_loss():
    _loss_times.append(time.time())


def _circuit_breaker_active() -> bool:
    global _breaker_until
    now = time.time()
    window = float(os.getenv("PHANTOM1_BREAKER_WINDOW_SEC", "1800"))
    threshold = int(os.getenv("PHANTOM1_BREAKER_LOSSES", "6"))
    cooldown = float(os.getenv("PHANTOM1_BREAKER_COOLDOWN_SEC", "3600"))

    cutoff = now - window
    while _loss_times and _loss_times[0] < cutoff:
        _loss_times.pop(0)

    if len(_loss_times) >= threshold:
        newly = _breaker_until <= now
        _breaker_until = max(_breaker_until, _loss_times[-1] + cooldown)
        if newly:
            logger.warning(
                f"[PHANTOM1] CIRCUIT BREAKER — {len(_loss_times)} losses in "
                f"{window/60:.0f}m; pausing entries for {cooldown/60:.0f}m"
            )
            tg = getattr(state_provider, "telegram_callback", None)
            if tg:
                msg = f"🛑 [PHANTOM1] CIRCUIT BREAKER — {len(_loss_times)} losses, entries paused"
                if asyncio.iscoroutinefunction(tg):
                    asyncio.create_task(tg(msg))
                else:
                    tg(msg)
        return True
    return now < _breaker_until


async def _risk_blocks_entry(size_usdc: float):
    if _kill_switch_active():
        return "kill_switch"
    if _circuit_breaker_active():
        return "circuit_breaker"

    max_conc = os.getenv("PHANTOM1_MAX_CONCURRENT_USDC", "100")
    try:
        if _open_exposure_usdc() + size_usdc > float(max_conc):
            return "max_concurrent_usdc"
    except ValueError:
        pass

    daily_limit = os.getenv("PHANTOM1_DAILY_LOSS_USDC", "75")
    try:
        if await _daily_realized_pnl() <= -abs(float(daily_limit)):
            return "daily_loss_limit"
    except ValueError:
        pass
    return None


# ── Per-market state ──────────────────────────────────────────────────────────

def _prune_market_state():
    if len(WINDOW_STATE) <= _MAX_TRACKED_MARKETS:
        return
    for mid in list(WINDOW_STATE)[:-_MAX_TRACKED_MARKETS]:
        WINDOW_STATE.pop(mid, None)


def _new_state(market_id: str, asset: str) -> dict:
    return {
        "market_id": market_id,
        "asset": asset,
        "entry_side": None,
        "entry_price": None,
        "order_filled": False,
        "fill_price": None,
        "entry_attempted": False,   # one entry attempt per market, ever
        "skipped_reason": None,
        "logged": False,
        "sig_id": None,
        "pos_id": None,
        "yes_token_id": None,
        "no_token_id": None,
        "condition_id": None,
        "size_usdc": None,
        "window_ts": None,
        "filled_at": None,
        "created_at": time.time(),
        # diagnostics for later calibration
        "displacement_bp_at_entry": None,
        "leader_price_at_entry": None,
    }


# ── Entry evaluation (pure function — unit-testable) ─────────────────────────

def evaluate_entry(ttr: float, yes_p, no_p, margin_pct, staleness_s: float = 0.0,
                   cfg: dict = None):
    """
    Return ("BUY_YES"|"BUY_NO", entry_price) if all PHANTOM-1 gates pass,
    else (None, skip_reason).

    margin_pct: spot displacement from window open in PERCENT (SpotFeed.get_margin);
                5bp == 0.05.
    """
    c = cfg or CONFIG

    if not (c["entry_ttr_low"] <= ttr <= c["entry_ttr_high"]):
        return None, "outside_entry_window"
    if yes_p is None or no_p is None:
        return None, "no_price_data"
    if staleness_s and staleness_s > c["max_staleness_s"]:
        return None, "stale_feed"

    leader_is_yes = yes_p >= no_p
    leader_price = yes_p if leader_is_yes else no_p

    if leader_price > c["hard_price_cap"]:
        return None, "leader_above_hard_cap"
    if leader_price > c["band_high"]:
        return None, "leader_above_band"
    if leader_price < c["band_low"]:
        return None, "no_clear_leader"

    if margin_pct is None:
        return None, "no_spot_margin"
    min_disp_pct = c["min_displacement_bp"] / 100.0   # bp → percent
    if abs(margin_pct) < min_disp_pct:
        return None, "displacement_below_gate"

    spot_favors_yes = margin_pct > 0
    if spot_favors_yes != leader_is_yes:
        return None, "spot_disagrees_with_leader"

    side = "BUY_YES" if leader_is_yes else "BUY_NO"
    return side, leader_price


# ── Order fill ────────────────────────────────────────────────────────────────

async def _fill_order(state: dict, decision_staleness_ms: int = 0,
                      displacement_bp: float = None):
    market_id = state["market_id"]
    asset = state["asset"]
    entry_side = state["entry_side"]
    entry_price = state["entry_price"]
    size_usdc = float(state["size_usdc"])

    token_id = state.get("yes_token_id") if entry_side == "BUY_YES" else state.get("no_token_id")
    if str(os.getenv("PAPER_MODE", "true")).lower() == "false" and not token_id:
        state["skipped_reason"] = "no_token_id"
        logger.warning(f"[PHANTOM1] {asset} live entry skipped — outcome token_id unavailable")
        return

    fill = await _executor.place_order(
        token_id=token_id, requested_price=entry_price, size_usdc=size_usdc, asset=asset
    )
    if not fill.filled:
        state["skipped_reason"] = f"order_{fill.status.lower()}"
        logger.warning(f"[PHANTOM1] {asset} order not filled ({fill.status}) — market {market_id}")
        return

    state["order_filled"] = True
    state["fill_price"] = fill.fill_price
    state["filled_at"] = time.time()
    state["displacement_bp_at_entry"] = displacement_bp
    state["leader_price_at_entry"] = entry_price

    sig_id = str(uuid.uuid4())
    state["sig_id"] = sig_id
    now_utc = datetime.now(timezone.utc)
    win_p = entry_price
    sig = {
        "id": sig_id,
        "asset": asset,
        "strategy_type": STRATEGY,
        "direction": entry_side,
        "grade": "A",
        "yes_price": win_p if entry_side == "BUY_YES" else round(1.0 - win_p, 4),
        "no_price": round(1.0 - win_p, 4) if entry_side == "BUY_YES" else win_p,
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
        "strategy_type": STRATEGY,
        "market_id": market_id,
        "asset": asset,
        "direction": entry_side,
        "entry_price": fill.fill_price,
        "size_usdc": size_usdc,
        "entry_mode": "SINGLE",
        "dca_rounds_completed": 1,
        "status": "OPEN",
        "is_paper": fill.is_paper,
        "opened_at": now_utc,
        "closed_at": None,
        "condition_id": state.get("condition_id"),
    }

    margin = None
    spot_agrees = True   # gated on agreement by construction
    sf = getattr(state_provider, "spot_feed", None)
    if sf is not None:
        margin = sf.get_margin(asset)
        if margin is not None:
            state["window_ts"] = margin["window_ts"]
    if state.get("window_ts") is None:
        state["window_ts"] = int(time.time() - (time.time() % 300))

    if state_provider.db_manager:
        await db_insert_signal(state_provider.db_manager, sig)
        await db_insert_position(state_provider.db_manager, pos)
        await execution_journal.record_fill(
            state_provider.db_manager.pool,
            position_id=pos_id, signal_id=sig_id,
            strategy_type=STRATEGY,
            asset=asset, direction=entry_side, fill=fill,
            decision_staleness_ms=decision_staleness_ms,
            margin=margin, spot_agrees=spot_agrees,
        )

    logger.info(
        f"[PHANTOM1] {asset} {'PAPER' if fill.is_paper else 'LIVE'} FILLED — "
        f"{entry_side} @ {fill.fill_price} size=${size_usdc} disp={displacement_bp}bp "
        f"market {market_id}"
    )


# ── Stats upsert (authoritative recompute, same pattern as LS v4) ─────────────

async def update_strategy_stats_in_db(db_manager):
    query_compute = """
        SELECT COUNT(*) AS trades_total,
               COUNT(*) FILTER (WHERE pnl > 0) AS wins,
               COUNT(*) FILTER (WHERE pnl < 0) AS losses,
               COALESCE(SUM(pnl), 0) AS total_pnl
        FROM positions
        WHERE strategy_type = $1 AND status = 'CLOSED'
    """
    query_upsert = """
    INSERT INTO strategy_stats (
        strategy_type, trades_total, wins, losses, win_rate, total_pnl,
        avg_pnl_per_trade, last_updated
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
    ON CONFLICT (strategy_type) DO UPDATE SET
        trades_total = EXCLUDED.trades_total,
        wins = EXCLUDED.wins,
        losses = EXCLUDED.losses,
        win_rate = EXCLUDED.win_rate,
        total_pnl = EXCLUDED.total_pnl,
        avg_pnl_per_trade = EXCLUDED.avg_pnl_per_trade,
        last_updated = NOW()
    """
    try:
        async with db_manager.pool.acquire(timeout=5.0) as conn:
            row = await conn.fetchrow(query_compute, STRATEGY)
            total = row["trades_total"] or 0
            wins = row["wins"] or 0
            losses = row["losses"] or 0
            total_pnl = float(row["total_pnl"] or 0.0)
            await conn.execute(
                query_upsert, STRATEGY, total, wins, losses,
                wins / total if total else 0.0, total_pnl,
                total_pnl / total if total else 0.0,
            )
    except Exception as e:
        logger.error(f"[PHANTOM1] Failed to update strategy stats: {e}")


# ── Resolution (hold-to-resolution exit) ──────────────────────────────────────

async def resolve_market(market_id: str):
    try:
        state = WINDOW_STATE.get(market_id)
        if state is None or state["logged"]:
            return

        if not state["order_filled"]:
            sig = {
                "id": str(uuid.uuid4()),
                "asset": state.get("asset", "BTC"),
                "strategy_type": STRATEGY,
                "direction": "NONE",
                "grade": "SKIP",
                "yes_price": state.get("leader_price_at_entry") or 0.0,
                "no_price": round(1.0 - (state.get("leader_price_at_entry") or 0.0), 4),
                "market_id": market_id,
                "outcome": "SKIPPED",
                "skip_reason": state["skipped_reason"] or "no_qualifying_entry",
                "fired_at": datetime.now(timezone.utc),
            }
            if state_provider.db_manager:
                await db_insert_signal(state_provider.db_manager, sig)
            state["logged"] = True
            return

        entry_side = state["entry_side"]
        fill_price = state["fill_price"]
        resolved_winner = None

        async def _poll_resolution():
            nonlocal resolved_winner
            for _ in range(24):
                try:
                    if await state_provider.is_market_resolved(market_id):
                        res_price = await state_provider.get_resolution_price(market_id, "BUY_YES")
                        if res_price >= 0.9:
                            resolved_winner = "UP"
                        elif res_price <= 0.1:
                            resolved_winner = "DOWN"
                        if resolved_winner is not None:
                            return
                except Exception as e:
                    logger.error(f"[PHANTOM1] Resolution check error: {e}")
                await asyncio.sleep(10)

        try:
            await asyncio.wait_for(_poll_resolution(), timeout=250.0)
        except asyncio.TimeoutError:
            logger.warning(f"[PHANTOM1] Resolution poll timed out for {market_id}")

        if resolved_winner is None:
            sf = getattr(state_provider, "spot_feed", None)
            wts = state.get("window_ts")
            if sf is not None and wts:
                resolved_winner = sf.get_window_resolution(state["asset"], wts)

        if resolved_winner is None:
            logger.warning(f"[PHANTOM1] Could not determine winner for {market_id} — pnl=0")
            if state_provider.db_manager and state.get("pos_id"):
                try:
                    async with state_provider.db_manager.pool.acquire(timeout=5.0) as conn:
                        await conn.execute(
                            "UPDATE positions SET status='CLOSED', exit_price=0.0, pnl=0.0, "
                            "close_reason='RESOLUTION_TIMEOUT', closed_at=NOW() WHERE id=$1",
                            uuid.UUID(state["pos_id"]),
                        )
                except Exception as e:
                    logger.error(f"[PHANTOM1] Failed to close timed-out position: {e}")
            state["logged"] = True
            return

        entry_correct = (
            (entry_side == "BUY_YES" and resolved_winner == "UP") or
            (entry_side == "BUY_NO" and resolved_winner == "DOWN")
        )
        size = float(state["size_usdc"])
        pnl = round(((1.0 - fill_price) * size) / fill_price, 4) if entry_correct else -size
        if not entry_correct:
            _record_loss()

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
                await update_strategy_stats_in_db(state_provider.db_manager)
            except Exception as e:
                logger.error(f"[PHANTOM1] Failed to close position in DB: {e}")

        if entry_correct:
            asyncio.create_task(auto_redeem(
                condition_id=state.get("condition_id"),
                asset=state["asset"],
                pnl=pnl,
            ))

        if state_provider.telegram_callback:
            msg = (
                f"🎯 [PHANTOM1] RESOLVED — {'WIN' if entry_correct else 'LOSS'} "
                f"{state['asset']} {entry_side} @ {fill_price} PnL: {pnl:+.2f} USDC"
            )
            if asyncio.iscoroutinefunction(state_provider.telegram_callback):
                asyncio.create_task(state_provider.telegram_callback(msg))
            else:
                state_provider.telegram_callback(msg)

        logger.info(f"[PHANTOM1] {market_id} resolved — {'WIN' if entry_correct else 'LOSS'} pnl={pnl:+.2f}")
        state["logged"] = True

    except Exception as e:
        logger.error(f"[PHANTOM1] Resolution processing failed: {e}", exc_info=True)


# ── Settlement sweep (safety net, same failure modes as LS v4) ────────────────

async def phantom_one_settlement_sweep(interval: float = 60.0):
    logger.info("[PHANTOM1] Settlement sweep started")
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
                    WHERE strategy_type = $1
                      AND opened_at > NOW() - INTERVAL '60 minutes'
                      AND (
                        close_reason = 'RESOLUTION_TIMEOUT'
                        OR (status = 'OPEN' AND opened_at < NOW() - INTERVAL '7 minutes')
                      )
                    """,
                    STRATEGY,
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
                        _record_loss()
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
                            asset=r["asset"], pnl=pnl,
                        ))
                    st = WINDOW_STATE.get(mid)
                    if st is not None:
                        st["logged"] = True
                    settled += 1
                except Exception as e:
                    logger.error(f"[PHANTOM1] Sweep settle error for {mid}: {e}")

            if settled:
                await update_strategy_stats_in_db(db)
                logger.info(f"[PHANTOM1] Settlement sweep re-settled {settled} position(s)")

        except asyncio.CancelledError:
            logger.info("[PHANTOM1] Settlement sweep cancelled — shutting down")
            raise
        except Exception as e:
            logger.error(f"[PHANTOM1] Settlement sweep error: {e}", exc_info=True)


# ── Time-driven driver ────────────────────────────────────────────────────────

async def _drive_asset(asset: str, feed):
    poly = feed.get_market_state(asset)
    if not poly or not poly.get("has_live_data"):
        return

    market_id = poly.get("market_id")
    if not market_id:
        return

    ttr = poly.get("time_to_resolution_seconds", 999)
    yes_p = poly.get("yes_price")
    no_p = poly.get("no_price")

    # Rollover: new market_id for this asset → previous market ended, resolve it.
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

    # Capture outcome tokens whenever the feed exposes them (needed for live).
    if poly.get("yes_token_id"):
        state["yes_token_id"] = poly.get("yes_token_id")
    if poly.get("no_token_id"):
        state["no_token_id"] = poly.get("no_token_id")
    if poly.get("condition_id"):
        state["condition_id"] = poly.get("condition_id")

    if state["entry_attempted"] or state["order_filled"]:
        return

    # Only evaluate inside the entry window; record why we never entered otherwise.
    if ttr < CONFIG["entry_ttr_low"]:
        if state["skipped_reason"] is None:
            state["skipped_reason"] = "entry_window_passed"
        return
    if ttr > CONFIG["entry_ttr_high"]:
        return   # too early — keep polling

    sf = getattr(state_provider, "spot_feed", None)
    margin = sf.get_margin(asset) if sf is not None else None
    margin_pct = margin["margin_pct"] if margin else None

    staleness = float(poly.get("staleness_seconds") or 0.0)
    side, result = evaluate_entry(ttr, yes_p, no_p, margin_pct, staleness)

    if side is None:
        # Not a terminal skip — conditions may become true later in the window.
        # Remember the latest reason for the end-of-window skip log.
        state["skipped_reason"] = result
        state["leader_price_at_entry"] = max(yes_p or 0.0, no_p or 0.0) or None
        return

    # All gates passed — attempt the single entry for this market.
    state["entry_attempted"] = True
    state["entry_side"] = side
    state["entry_price"] = result
    size = _position_size()
    block = await _risk_blocks_entry(size)
    if block:
        state["skipped_reason"] = f"risk_{block}"
        logger.warning(f"[PHANTOM1] {asset} entry blocked by risk gate: {block}")
        return
    state["size_usdc"] = size
    state["skipped_reason"] = None
    disp_bp = round(abs(margin_pct) * 100.0, 2) if margin_pct is not None else None
    await _fill_order(
        state,
        decision_staleness_ms=int(staleness * 1000),
        displacement_bp=disp_bp,
    )


async def phantom_one_driver(poll_interval: float = 0.5):
    """Background loop launched from main.py (paper trading: PAPER_MODE=true)."""
    logger.info(f"[PHANTOM1] Time-driven driver started (assets: {', '.join(_ASSETS)})")
    while True:
        try:
            await asyncio.sleep(poll_interval)

            if state_provider and hasattr(state_provider, "config"):
                cfg = state_provider.config.get("phantom_one")
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
                    logger.error(f"[PHANTOM1] Driver error for {asset}: {e}", exc_info=True)

        except asyncio.CancelledError:
            logger.info("[PHANTOM1] Driver cancelled — shutting down")
            raise
        except Exception as e:
            logger.error(f"[PHANTOM1] Driver loop error: {e}", exc_info=True)
