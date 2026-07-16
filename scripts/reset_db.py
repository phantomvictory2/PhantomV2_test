"""
Fresh-start database reset for Phantom V2.
Clears all trading data (signals, positions, stats, journals, risk events).
Resets system_config to clean paper-mode defaults.
Run once before starting a new trading session.
"""
import asyncio
import os
from dotenv import load_dotenv
import asyncpg

load_dotenv()

RESET_SQL = """
-- Disable FK checks by truncating in dependency order (CASCADE handles any remnants)
TRUNCATE TABLE
    dca_execution_journal,
    phase0_edge_log,
    risk_events,
    positions,
    signals,
    daily_stats,
    asset_lag_stats,
    strategy_stats
CASCADE;

-- Reset system_config to clean paper-mode defaults
DELETE FROM system_config;
INSERT INTO system_config (key, value, updated_at) VALUES
    ('kill_switch',                'false',  NOW()),
    ('paper_mode',                 'true',   NOW()),
    ('daily_loss_limit_pct',       '0.05',   NOW()),
    ('regime_stop_loss_pct',       '0.03',   NOW()),
    ('regime_stop_winrate',        '0.40',   NOW()),
    ('max_open_positions',         '2',      NOW()),
    ('max_trade_usdc',             '100',    NOW()),
    ('consecutive_loss_limit',     '3',      NOW()),
    ('signal_velocity_limit',      '5',      NOW()),
    ('oracle_lag_minimum_seconds', '5',      NOW()),
    ('dca_adverse_price_threshold','0.10',   NOW());
"""

async def main():
    url = os.getenv("DATABASE_URL")
    if not url:
        print("ERROR: DATABASE_URL not set in .env")
        return

    print("Connecting to database...")
    conn = await asyncpg.connect(url)
    try:
        print("Executing fresh-start reset...")
        await conn.execute(RESET_SQL)
        print("OK: All trading tables cleared.")
        print("OK: system_config reset to paper-mode defaults.")

        # Verify row counts
        tables = [
            "signals", "positions", "daily_stats", "risk_events",
            "asset_lag_stats", "strategy_stats",
            "phase0_edge_log", "dca_execution_journal",
        ]
        print("\nPost-reset row counts:")
        for t in tables:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {t}")
            print(f"  {t}: {count} rows")

        cfg_rows = await conn.fetch("SELECT key, value FROM system_config ORDER BY key")
        print("\nsystem_config:")
        for r in cfg_rows:
            print(f"  {r['key']} = {r['value']}")

    finally:
        await conn.close()
    print("\nDatabase reset complete. Ready for fresh trading session.")

if __name__ == "__main__":
    asyncio.run(main())
