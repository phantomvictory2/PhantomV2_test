"""
orbit_a_240_v2.py — ORBIT A 240 V2: trend-confirmed momentum with trailing
reversal exit. Replaces ORBIT_A_240 (disabled after losing $0.60/trade over
825 paper trades at 55.6% win rate — payoff asymmetry TP7/SL12 needs ~63%).

User spec + wallet-study evidence:

  ENTRY (all required, evaluated every poll):
    • Elapsed 90–280s in the 5-min window (TTR 20–210).
    • Side price in 0.62–0.82.
    • SUSTAINED momentum: side gained >= +0.03 cumulative over the last ~15s
      (not a single 2c tick — that was V1's churn bug; 825 trades/10h).
    • Spot confirmation: BTC displaced >= 3bp from window open in the SAME
      direction (fake moves = book momentum without spot; real trends carried
      63–87% of the profitable wallets' PnL in displaced windows).
    • One entry per market, 60s cooldown after any losing exit, BTC only.

  EXIT (whichever fires first):
    • TAKE_PROFIT: +7% on entry price.
    • TRAILING_REVERSAL: price drops 4.5c off its peak since entry
      (tightens to 2c once trade is +4% — lock profit on giveback).
    • SPOT_FLIP: spot displacement flips against the side (underlying
      reversed; the book will follow).
    • STOP_LOSS: -15% hard stop (disaster insurance — the trailing exit
      should fire long before this in normal chop).
    • RESOLUTION: window ends with position still open -> resolve at 1/0.

  Instrumentation: entry displacement, momentum, peak, and exit reason are
  persisted (close_reason + journal) so filters can be tuned from data.

Self-contained time-driven driver (same architecture as PHANTOM_ONE_V1):
does not go through signal_engine/risk_engine/monitor. Paper exits are
simulated at the observed book price.
"""

import logging
import os
import time
import uuid
import asyncio
from collections import deque
from datetime import datetime, timezone

from dashboard import state_provider
from clob_executor import ClobExecutor
import execution_journal
from clob_redeemer import auto_redeem
from .last_shadow_trade_lite_v4 import db_insert_signal, db_insert_position

logger = logging.getLogger(__name__)

STRATEGY = "ORBIT_A_240_V2"

_executor = ClobExecutor()

CONFIG = {
    "enabled": True,
    "band_low": 0.62,
    "band_high": 0.82,
    "elapsed_min": 90,            # no entries before 90s elapsed  (TTR <= 210)
    "elapsed_max": 280,           # no entries after 280s elapsed  (TTR >= 20)
    "momentum_lookback_s": 15.0,
    "momentum_min": 0.03,         # cumulative side gain over lookback
    "min_displacement_bp": 3.0,   # spot must agree
    "take_profit_pct": 0.07,
    "trail_gap": 0.045,           # off-peak drop that exits
    "trail_gap_tight": 0.02,      # once >= +4%, protect profit
    "tighten_at_pct": 0.04,
    "stop_loss_pct": 0.15,        # hard stop (user spec)
    "size_usdc": 30.0,
    "loss_cooldown_s": 60.0,
    "min_hold_s": 3.0,            # ignore spot-flip in the first seconds
    "max_staleness_s": 5.0,
}

_ASSETS = tuple(a.strip().upper() for a in os.getenv("ORBITV2_ASSETS", "BTC").split(",") if a.strip())

WINDOW_STATE: dict = {}
_MAX_TRACKED = 12
_current_market_id: dict = {}
_price_hist: dict = {}            # asset -> deque[(ts, yes_price)]
_cooldown_until: dict = {}        # asset -> ts
_daily_pnl_cache = {"day": None, "pnl": 0.0, "ts": 0.0}


def _kill_switch_active() -> bool:
    if str(os.getenv("KILL_SWITCH", "false")).lower() == "true":
        return True
    try:
        if str(state_provider.get_config("kill_switch", False)).lower() == "true":
            return True
    except Exception:
        pass
    return False


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
        logger.error(f"[ORBITV2] daily pnl query failed: {e}")
    return _daily_pnl_cache["pnl"]


def _new_state(market_id: str, asset: str) -> dict:
    return {
        "market_id": market_id, "asset": asset,
        "entry_side": None, "entry_price": None, "size_usdc": None,
        "order_filled": False, "entry_attempted": False,
        "peak": None, "exited": False, "logged": False,
        "sig_id": None, "pos_id": None,
        "yes_token_id": None, "no_token_id": None, "condition_id": None,
        "window_ts": None, "filled_at": None,
        "disp_bp_entry": None, "momentum_entry": None,
        "skipped_reason": None, "created_at": time.time(),
    }


def _prune():
    if len(WINDOW_STATE) > _MAX_TRACKED:
        for mid in list(WINDOW_STATE)[:-_MAX_TRACKED]:
            WINDOW_STATE.pop(mid, None)


# ── Pure decision functions (unit-testable) ──────────────────────────────────

def evaluate_entry(elapsed: float, yes_p, no_p, mom_yes, margin_pct,
                   staleness_s: float = 0.0, cfg: dict = None):
    """mom_yes: cumulative YES change over the lookback (signed).
    Returns (side, price) or (None, reason)."""
    c = cfg or CONFIG
    if not (c["elapsed_min"] <= elapsed <= c["elapsed_max"]):
        return None, "outside_entry_window"
    if yes_p is None or no_p is None:
        return None, "no_price_data"
    if staleness_s and staleness_s > c["max_staleness_s"]:
        return None, "stale_feed"
    if mom_yes is None:
        return None, "no_momentum_history"

    if mom_yes >= c["momentum_min"]:
        side, price = "BUY_YES", yes_p
    elif mom_yes <= -c["momentum_min"]:
        side, price = "BUY_NO", no_p
    else:
        return None, "no_sustained_momentum"

    if not (c["band_low"] <= price <= c["band_high"]):
        return None, "price_outside_band"

    if margin_pct is None:
        return None, "no_spot_margin"
    if abs(margin_pct) < c["min_displacement_bp"] / 100.0:
        return None, "displacement_below_gate"
    if (margin_pct > 0) != (side == "BUY_YES"):
        return None, "spot_disagrees"

    return side, price


def evaluate_exit(entry_price: float, cur: float, peak: float, held_s: float,
                  margin_pct, side: str, cfg: dict = None):
    """Returns close_reason string or None (keep holding)."""
    c = cfg or CONFIG
    if cur is None:
        return None
    eps = 1e-9
    if cur >= entry_price * (1.0 + c["take_profit_pct"]) - eps:
        return "TAKE_PROFIT"
    if cur <= entry_price * (1.0 - c["stop_loss_pct"]) + eps:
        return "STOP_LOSS"
    gain = (cur - entry_price) / entry_price
    gap = c["trail_gap_tight"] if (peak - entry_price) / entry_price >= c["tighten_at_pct"] else c["trail_gap"]
    if peak is not None and cur <= peak - gap:
        return "TRAILING_REVERSAL"
    if held_s >= c["min_hold_s"] and margin_pct is not None:
        if (margin_pct > 0) != (side == "BUY_YES") and abs(margin_pct) >= 0.01:
            return "SPOT_FLIP"
    return None


# ── DB close helper ──────────────────────────────────────────────────────────

async def _close_position(state: dict, exit_price: float, pnl: float, reason: str):
    if state_provider.db_manager and state.get("pos_id"):
        try:
            async with state_provider.db_manager.pool.acquire(timeout=5.0) as conn:
                await conn.execute(
                    "UPDATE positions SET status='CLOSED', exit_price=$2, pnl=$3, "
                    "close_reason=$4, closed_at=NOW() WHERE id=$1",
                    uuid.UUID(state["pos_id"]), exit_price, pnl, reason,
                )
            await update_strategy_stats_in_db(state_provider.db_manager)
        except Exception as e:
            logger.error(f"[ORBITV2] close failed: {e}")
    state["exited"] = True
    state["logged"] = True
    if pnl < 0:
        _cooldown_until[state["asset"]] = time.time() + CONFIG["loss_cooldown_s"]
    tg = getattr(state_provider, "telegram_callback", None)
    if tg:
        msg = (f"🛰 [ORBITV2] {reason} — {state['asset']} {state['entry_side']} "
               f"in {state['entry_price']} out {exit_price} PnL {pnl:+.2f} USDC")
        if asyncio.iscoroutinefunction(tg):
            asyncio.create_task(tg(msg))
        else:
            tg(msg)
    logger.info(f"[ORBITV2] {state['market_id']} {reason} pnl={pnl:+.2f} "
                f"(disp={state.get('disp_bp_entry')}bp mom={state.get('momentum_entry')})")


async def update_strategy_stats_in_db(db_manager):
    try:
        async with db_manager.pool.acquire(timeout=5.0) as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) t, COUNT(*) FILTER (WHERE pnl>0) w, "
                "COUNT(*) FILTER (WHERE pnl<0) l, COALESCE(SUM(pnl),0) p "
                "FROM positions WHERE strategy_type=$1 AND status='CLOSED'", STRATEGY)
            t, w, l, p = row["t"] or 0, row["w"] or 0, row["l"] or 0, float(row["p"] or 0.0)
            await conn.execute(
                """INSERT INTO strategy_stats (strategy_type, trades_total, wins, losses,
                   win_rate, total_pnl, avg_pnl_per_trade, last_updated)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
                   ON CONFLICT (strategy_type) DO UPDATE SET
                   trades_total=EXCLUDED.trades_total, wins=EXCLUDED.wins,
                   losses=EXCLUDED.losses, win_rate=EXCLUDED.win_rate,
                   total_pnl=EXCLUDED.total_pnl,
                   avg_pnl_per_trade=EXCLUDED.avg_pnl_per_trade, last_updated=NOW()""",
                STRATEGY, t, w, l, w / t if t else 0.0, p, p / t if t else 0.0)
    except Exception as e:
        logger.error(f"[ORBITV2] stats update failed: {e}")


# ── Entry fill ───────────────────────────────────────────────────────────────

async def _fill_order(state: dict, disp_bp, momentum):
    token_id = state["yes_token_id"] if state["entry_side"] == "BUY_YES" else state["no_token_id"]
    if str(os.getenv("PAPER_MODE", "true")).lower() == "false" and not token_id:
        state["skipped_reason"] = "no_token_id"
        return
    size = CONFIG["size_usdc"]
    fill = await _executor.place_order(
        token_id=token_id, requested_price=state["entry_price"],
        size_usdc=size, asset=state["asset"])
    if not fill.filled:
        state["skipped_reason"] = f"order_{fill.status.lower()}"
        return
    state["order_filled"] = True
    state["size_usdc"] = size
    state["entry_price"] = fill.fill_price
    state["peak"] = fill.fill_price
    state["filled_at"] = time.time()
    state["disp_bp_entry"] = disp_bp
    state["momentum_entry"] = momentum
    state["window_ts"] = int(time.time() - (time.time() % 300))

    sig_id, pos_id = str(uuid.uuid4()), str(uuid.uuid4())
    state["sig_id"], state["pos_id"] = sig_id, pos_id
    now_utc = datetime.now(timezone.utc)
    yes_p = state["entry_price"] if state["entry_side"] == "BUY_YES" else round(1 - state["entry_price"], 4)
    sig = {"id": sig_id, "asset": state["asset"], "strategy_type": STRATEGY,
           "direction": state["entry_side"], "grade": "A",
           "yes_price": yes_p, "no_price": round(1 - yes_p, 4),
           "market_id": state["market_id"], "outcome": "EXECUTED",
           "skip_reason": None, "fired_at": now_utc}
    pos = {"id": pos_id, "signal_id": sig_id, "strategy_type": STRATEGY,
           "market_id": state["market_id"], "asset": state["asset"],
           "direction": state["entry_side"], "entry_price": state["entry_price"],
           "size_usdc": size, "entry_mode": "SINGLE", "dca_rounds_completed": 1,
           "status": "OPEN", "is_paper": fill.is_paper, "opened_at": now_utc,
           "closed_at": None, "condition_id": state.get("condition_id")}
    if state_provider.db_manager:
        await db_insert_signal(state_provider.db_manager, sig)
        await db_insert_position(state_provider.db_manager, pos)
        await execution_journal.record_fill(
            state_provider.db_manager.pool, position_id=pos_id, signal_id=sig_id,
            strategy_type=STRATEGY, asset=state["asset"],
            direction=state["entry_side"], fill=fill,
            decision_staleness_ms=0, margin=None, spot_agrees=True)
    logger.info(f"[ORBITV2] {state['asset']} PAPER FILLED {state['entry_side']} "
                f"@ {state['entry_price']} disp={disp_bp}bp mom={momentum}")


# ── Resolution fallback (position still open at window end) ──────────────────

async def resolve_market(market_id: str):
    state = WINDOW_STATE.get(market_id)
    if state is None or state["logged"]:
        return
    if not state["order_filled"] or state["exited"]:
        state["logged"] = True
        return
    winner = None
    try:
        for _ in range(24):
            if await state_provider.is_market_resolved(market_id):
                yp = await state_provider.get_resolution_price(market_id, "BUY_YES")
                winner = "UP" if yp >= 0.9 else ("DOWN" if yp <= 0.1 else None)
                if winner:
                    break
            await asyncio.sleep(10)
    except Exception as e:
        logger.error(f"[ORBITV2] resolution poll error: {e}")
    if winner is None:
        sf = getattr(state_provider, "spot_feed", None)
        if sf is not None and state.get("window_ts"):
            winner = sf.get_window_resolution(state["asset"], state["window_ts"])
    if winner is None:
        await _close_position(state, 0.0, 0.0, "RESOLUTION_TIMEOUT")
        return
    correct = ((state["entry_side"] == "BUY_YES" and winner == "UP") or
               (state["entry_side"] == "BUY_NO" and winner == "DOWN"))
    size = float(state["size_usdc"])
    entry = float(state["entry_price"])
    pnl = round((1.0 - entry) / entry * size, 4) if correct else -size
    if correct:
        asyncio.create_task(auto_redeem(condition_id=state.get("condition_id"),
                                        asset=state["asset"], pnl=pnl))
    await _close_position(state, 1.0 if correct else 0.0, pnl, "RESOLUTION")


# ── Driver ───────────────────────────────────────────────────────────────────

async def _drive_asset(asset: str, feed):
    poly = feed.get_market_state(asset)
    if not poly or not poly.get("has_live_data"):
        return
    market_id = poly.get("market_id")
    if not market_id:
        return

    ttr = poly.get("time_to_resolution_seconds", 999)
    yes_p, no_p = poly.get("yes_price"), poly.get("no_price")
    now = time.time()

    # Price history for momentum lookback
    if yes_p is not None:
        h = _price_hist.setdefault(asset, deque())
        h.append((now, yes_p))
        cutoff = now - CONFIG["momentum_lookback_s"] - 5
        while h and h[0][0] < cutoff:
            h.popleft()

    if market_id != _current_market_id.get(asset):
        prev = _current_market_id.get(asset)
        _current_market_id[asset] = market_id
        if market_id not in WINDOW_STATE:
            WINDOW_STATE[market_id] = _new_state(market_id, asset)
            _prune()
        if prev and prev in WINDOW_STATE:
            asyncio.create_task(resolve_market(prev))

    state = WINDOW_STATE.get(market_id)
    if state is None:
        state = _new_state(market_id, asset)
        WINDOW_STATE[market_id] = state

    for k in ("yes_token_id", "no_token_id", "condition_id"):
        if poly.get(k):
            state[k] = poly.get(k)

    sf = getattr(state_provider, "spot_feed", None)
    margin = sf.get_margin(asset) if sf is not None else None
    margin_pct = margin["margin_pct"] if margin else None

    # ── Manage open position ──
    if state["order_filled"] and not state["exited"]:
        cur = yes_p if state["entry_side"] == "BUY_YES" else no_p
        if cur is not None:
            state["peak"] = max(state["peak"] or cur, cur)
            reason = evaluate_exit(state["entry_price"], cur, state["peak"],
                                   now - (state["filled_at"] or now), margin_pct,
                                   state["entry_side"])
            if reason:
                size = float(state["size_usdc"])
                pnl = round((cur - state["entry_price"]) / state["entry_price"] * size, 4)
                await _close_position(state, cur, pnl, reason)
        return

    if state["entry_attempted"]:
        return

    # ── Entry evaluation ──
    if _kill_switch_active():
        return
    if now < _cooldown_until.get(asset, 0):
        return
    daily_limit = float(os.getenv("ORBITV2_DAILY_LOSS_USDC", "75"))
    if await _daily_realized_pnl() <= -abs(daily_limit):
        state["skipped_reason"] = "daily_loss_limit"
        return

    elapsed = 300 - ttr if ttr <= 300 else -1
    h = _price_hist.get(asset) or deque()
    mom_yes = None
    target_t = now - CONFIG["momentum_lookback_s"]
    old = [p for (t, p) in h if t <= target_t]
    if old and yes_p is not None:
        mom_yes = round(yes_p - old[-1], 4)

    side, result = evaluate_entry(elapsed, yes_p, no_p, mom_yes, margin_pct,
                                  float(poly.get("staleness_seconds") or 0.0))
    if side is None:
        state["skipped_reason"] = result
        return

    state["entry_attempted"] = True
    state["entry_side"] = side
    state["entry_price"] = result
    disp = round(abs(margin_pct) * 100.0, 2) if margin_pct is not None else None
    await _fill_order(state, disp, mom_yes)


async def orbit_v2_driver(poll_interval: float = 0.5):
    logger.info(f"[ORBITV2] Time-driven driver started (assets: {', '.join(_ASSETS)})")
    while True:
        try:
            await asyncio.sleep(poll_interval)
            if state_provider and hasattr(state_provider, "config"):
                cfg = state_provider.config.get("orbit_v2")
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
                    logger.error(f"[ORBITV2] driver error for {asset}: {e}", exc_info=True)
        except asyncio.CancelledError:
            logger.info("[ORBITV2] Driver cancelled — shutting down")
            raise
        except Exception as e:
            logger.error(f"[ORBITV2] Driver loop error: {e}", exc_info=True)
