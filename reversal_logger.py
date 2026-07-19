"""
reversal_logger.py — per-window reversal instrumentation (all assets).

Motivation: Last Shadow's rare -$50 losses and the proposed REVERSAL_TAIL
strategy both hinge on one number we do not have: the CONDITIONAL probability
that a near-settled favorite flips, given how far spot is from the window open.
This task measures it. Every 5-min window, per asset, it snapshots the book
favorite and the spot displacement at T-60s / T-30s / T-10s, then records the
resolved winner and whether the T-10 favorite lost (a reversal).

SOL is logged even though Last Shadow no longer trades it — SOL's reversal
frequency is exactly the data needed to price the tail play.

Table is created on first use (same pattern as execution_journal).
"""

import logging
import time
import asyncio
from datetime import datetime, timezone

from dashboard import state_provider

logger = logging.getLogger(__name__)

_ASSETS = ("BTC", "ETH", "SOL")

_DDL = """
CREATE TABLE IF NOT EXISTS reversal_log (
    id BIGSERIAL PRIMARY KEY,
    asset VARCHAR(10) NOT NULL,
    window_ts BIGINT NOT NULL,
    market_id VARCHAR(100),
    fav_side_t60 VARCHAR(4), fav_price_t60 FLOAT, disp_bp_t60 FLOAT,
    fav_side_t30 VARCHAR(4), fav_price_t30 FLOAT, disp_bp_t30 FLOAT,
    fav_side_t10 VARCHAR(4), fav_price_t10 FLOAT, disp_bp_t10 FLOAT,
    winner VARCHAR(4),
    reversed BOOLEAN,
    logged_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (asset, window_ts)
);
"""

_ddl_done = False
_pending: dict = {}   # asset -> snapshot dict for the current window


def is_reversal(fav_side_t10, winner):
    """True when the T-10s book favorite did not win. Pure — unit-testable."""
    if fav_side_t10 is None or winner is None:
        return None
    return (fav_side_t10 == "YES") != (winner == "UP")


def _snap(poly, sf, asset):
    yes_p, no_p = poly.get("yes_price"), poly.get("no_price")
    if yes_p is None or no_p is None:
        return None
    fav = "YES" if yes_p >= no_p else "NO"
    price = max(yes_p, no_p)
    disp = None
    if sf is not None:
        m = sf.get_margin(asset)
        if m is not None:
            disp = round(m["margin_pct"] * 100.0, 2)   # bp
    return {"side": fav, "price": price, "disp": disp}


async def _ensure_table(db):
    global _ddl_done
    if _ddl_done:
        return
    async with db.pool.acquire(timeout=5.0) as conn:
        await conn.execute(_DDL)
    _ddl_done = True


async def _flush(asset: str, rec: dict, winner):
    db = state_provider.db_manager
    if not db or not getattr(db, "pool", None):
        return
    try:
        await _ensure_table(db)
        t60, t30, t10 = rec.get("t60") or {}, rec.get("t30") or {}, rec.get("t10") or {}
        rev = is_reversal(t10.get("side"), winner)
        async with db.pool.acquire(timeout=5.0) as conn:
            await conn.execute(
                """INSERT INTO reversal_log (asset, window_ts, market_id,
                     fav_side_t60, fav_price_t60, disp_bp_t60,
                     fav_side_t30, fav_price_t30, disp_bp_t30,
                     fav_side_t10, fav_price_t10, disp_bp_t10,
                     winner, reversed)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                   ON CONFLICT (asset, window_ts) DO NOTHING""",
                asset, rec["window_ts"], rec.get("market_id"),
                t60.get("side"), t60.get("price"), t60.get("disp"),
                t30.get("side"), t30.get("price"), t30.get("disp"),
                t10.get("side"), t10.get("price"), t10.get("disp"),
                winner, rev,
            )
        if rev:
            logger.warning(
                f"[REVERSAL] {asset} window {rec['window_ts']} REVERSED — "
                f"T-10 fav {t10.get('side')}@{t10.get('price')} disp={t10.get('disp')}bp -> winner {winner}"
            )
    except Exception as e:
        logger.error(f"[REVERSAL] flush failed for {asset}: {e}")


async def reversal_logger_task(poll_interval: float = 1.0):
    logger.info("[REVERSAL] Logger started (BTC/ETH/SOL, snapshots T-60/30/10)")
    while True:
        try:
            await asyncio.sleep(poll_interval)
            feed = getattr(state_provider, "poly_feed", None)
            sf = getattr(state_provider, "spot_feed", None)
            if feed is None:
                continue
            for asset in _ASSETS:
                try:
                    poly = feed.get_market_state(asset)
                    if not poly or not poly.get("has_live_data"):
                        continue
                    ttr = poly.get("time_to_resolution_seconds", 999)
                    wts = int(time.time() - (time.time() % 300))

                    rec = _pending.get(asset)
                    if rec is None or rec["window_ts"] != wts:
                        # New window began: resolve and flush the previous one.
                        if rec is not None and sf is not None:
                            winner = sf.get_window_resolution(asset, rec["window_ts"])
                            asyncio.create_task(_flush(asset, rec, winner))
                        rec = {"window_ts": wts, "market_id": poly.get("market_id")}
                        _pending[asset] = rec

                    if ttr <= 65 and "t60" not in rec:
                        rec["t60"] = _snap(poly, sf, asset)
                    if ttr <= 33 and "t30" not in rec:
                        rec["t30"] = _snap(poly, sf, asset)
                    if ttr <= 12 and "t10" not in rec:
                        rec["t10"] = _snap(poly, sf, asset)
                except Exception as e:
                    logger.error(f"[REVERSAL] {asset} error: {e}")
        except asyncio.CancelledError:
            logger.info("[REVERSAL] Logger cancelled — shutting down")
            raise
        except Exception as e:
            logger.error(f"[REVERSAL] loop error: {e}", exc_info=True)
