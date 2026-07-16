"""
execution_journal.py — persists per-trade execution cost data.

Every fill (paper or live) records what we *expected* to pay vs what we *actually* paid,
plus fee and slippage. This is the dataset that answers the Phase-1 question: does a
strategy's paper edge survive real execution costs? Analyse with:

    SELECT strategy_type, is_paper,
           COUNT(*), AVG(slippage_usdc), AVG(fee_usdc),
           AVG(fill_price - requested_price) AS avg_price_drift
    FROM execution_journal GROUP BY 1, 2;
"""

import uuid
import logging

logger = logging.getLogger("ExecutionJournal")

_CREATE = """
CREATE TABLE IF NOT EXISTS execution_journal (
    id              UUID PRIMARY KEY,
    position_id     UUID,
    signal_id       UUID,
    strategy_type   TEXT,
    asset           TEXT,
    direction       TEXT,
    requested_price DOUBLE PRECISION,
    fill_price      DOUBLE PRECISION,
    size_usdc       DOUBLE PRECISION,
    filled_usdc     DOUBLE PRECISION,
    fee_usdc        DOUBLE PRECISION,
    slippage_usdc   DOUBLE PRECISION,
    latency_ms      INTEGER,
    decision_staleness_ms INTEGER,
    spot_open       DOUBLE PRECISION,
    spot_now        DOUBLE PRECISION,
    margin_pct      DOUBLE PRECISION,
    spot_agrees     BOOLEAN,
    status          TEXT,
    is_paper        BOOLEAN,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_exec_journal_strategy ON execution_journal (strategy_type, is_paper);
-- migrate already-deployed tables (idempotent)
ALTER TABLE execution_journal ADD COLUMN IF NOT EXISTS decision_staleness_ms INTEGER;
ALTER TABLE execution_journal ADD COLUMN IF NOT EXISTS spot_open   DOUBLE PRECISION;
ALTER TABLE execution_journal ADD COLUMN IF NOT EXISTS spot_now    DOUBLE PRECISION;
ALTER TABLE execution_journal ADD COLUMN IF NOT EXISTS margin_pct  DOUBLE PRECISION;
ALTER TABLE execution_journal ADD COLUMN IF NOT EXISTS spot_agrees BOOLEAN;
"""

_INSERT = """
INSERT INTO execution_journal (
    id, position_id, signal_id, strategy_type, asset, direction,
    requested_price, fill_price, size_usdc, filled_usdc, fee_usdc,
    slippage_usdc, latency_ms, decision_staleness_ms,
    spot_open, spot_now, margin_pct, spot_agrees, status, is_paper
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
"""

_ensured = False


async def ensure_table(pool):
    global _ensured
    if _ensured:
        return
    try:
        async with pool.acquire(timeout=5.0) as conn:
            await conn.execute(_CREATE)
        _ensured = True
    except Exception as e:
        logger.error(f"[EXEC_JOURNAL] ensure_table failed: {e}")


async def record_fill(pool, *, position_id, signal_id, strategy_type, asset,
                      direction, fill, decision_staleness_ms=0, margin=None,
                      spot_agrees=None):
    """`fill` is a clob_executor.FillResult. `margin` is a spot_feed.get_margin() dict
    (or None). Records execution cost + the Chainlink-proxy conviction data per trade so
    we can later correlate margin with reversals (Phase 2a instrumentation)."""
    if pool is None:
        return
    m = margin or {}
    try:
        await ensure_table(pool)
        async with pool.acquire(timeout=5.0) as conn:
            await conn.execute(
                _INSERT,
                uuid.uuid4(),
                uuid.UUID(position_id) if position_id else None,
                uuid.UUID(signal_id) if signal_id else None,
                strategy_type, asset, direction,
                fill.requested_price, fill.fill_price, fill.size_usdc,
                fill.filled_usdc, fill.fee_usdc, fill.slippage_usdc,
                fill.latency_ms, int(decision_staleness_ms),
                m.get("open"), m.get("now"), m.get("margin_pct"), spot_agrees,
                fill.status, fill.is_paper,
            )
    except Exception as e:
        logger.error(f"[EXEC_JOURNAL] record_fill failed: {e}")
