import asyncio
import os
import sys
import uuid
import logging
from dotenv import load_dotenv

from database import DatabaseManager
from executor import Executor
from database_writer import DatabaseWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class MockRiskEngine:
    def __init__(self):
        self.config = {"paper_mode": "true", "kill_switch": "false"}
        self.regime_stopped = False
    def get_config(self, key, default):
        return self.config.get(key, default)

class MockStateProvider:
    def __init__(self):
        self.market_state = {"yes_price": 0.54, "no_price": 0.52}
        self.open_positions = []
        self.pending_assets = set()
    def get_market_state(self, asset):
        return self.market_state
    def get_open_positions(self):
        return self.open_positions

async def main():
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    if not url:
        logger.error("DATABASE_URL not set in environment.")
        return
        
    logger.info(f"Connecting to database: {url}")
    db = DatabaseManager(url)
    await db.initialize()
    
    # Run migrations just in case
    await db.run_migrations("schema.sql")
    
    # Initialize the writer
    db_writer = DatabaseWriter(db)
    db_writer.start()
    
    # 1. Create a mock signal
    signal_id = str(uuid.uuid4())
    mock_signal = {
        "signal_id": signal_id,
        "asset": "BTC",
        "strategy_type": "MOMENTUM_RIDE_5M",
        "direction": "BUY_YES",
        "magnitude_pct": 0.5,
        "magnitude_usd": 150.0,
        "duration_seconds": 300,
        "grade": "A",
        "poly_staleness_seconds": 1.2,
        "spread": 0.01,
        "yes_price": 0.54,
        "no_price": 0.52,
        "market_id": "0x_mock_market_recovery_test",
        "time_to_resolution_seconds": 3600,
        "liquidity_usdc": 50000.0,
        "velocity_count": 2,
        "outcome": "EXECUTED",
        "skip_reason": None,
        "entry_mode": "DCA",
        "dca_config": {
            "rounds": 5,
            "per_round_usdc": 2.0,
            "limit_price": 0.55,
            "interval_seconds": 1  # Short interval for testing
        }
    }
    
    # Save the signal to the database first
    logger.info("Inserting mock signal...")
    await db.save_signal(mock_signal)
    
    # 2. Setup mock risk engine, state provider, and executor
    re = MockRiskEngine()
    sp = MockStateProvider()
    
    # Callback to database writer
    async def db_callback(table, data):
        await db_writer.write(table, data)
        
    executor = Executor(
        risk_engine=re,
        db_callback=db_callback,
        telegram_callback=lambda msg: logger.info(f"Mock TG Alert: {msg}"),
        state_provider=sp
    )
    await executor.initialize()
    
    # 3. Simulate first execution
    logger.info("Starting initial DCA execution (simulating crash after 2 rounds)...")
    # Launch _execute_dca task
    dca_task = asyncio.create_task(executor._execute_dca(mock_signal))
    
    # Let it run for 2 rounds
    # Round 1 at 0s, sleep 1s, Round 2 at 1s, sleep 1s.
    # If we cancel at 1.8s, it will have completed 2 rounds.
    await asyncio.sleep(1.8)
    
    logger.info("Simulating crash/restart by cancelling DCA task...")
    dca_task.cancel()
    try:
        await dca_task
    except asyncio.CancelledError:
        logger.info("DCA task successfully cancelled.")
        
    # Drain DB write queue to ensure state is flushed to DB. Wait up to 8s since DB connection might be slow.
    logger.info("Waiting for DatabaseWriter queue to drain...")
    for _ in range(16):
        if db_writer._queue.empty():
            await asyncio.sleep(0.5)  # give it one extra brief sleep to ensure the last query is finished executing
            break
        await asyncio.sleep(0.5)
    
    # 4. Verify DB State after "crash"
    logger.info("Verifying database journal state after crash...")
    active_journals = await db.load_active_dca_journals()
    target_journal = None
    for j in active_journals:
        if j["signal_id"] == signal_id:
            target_journal = j
            break
            
    assert target_journal is not None, "Active DCA journal not found in DB!"
    logger.info(f"Found active journal: {target_journal}")
    assert target_journal["rounds_completed"] == 2, f"Expected 2 rounds completed, got {target_journal['rounds_completed']}"
    assert target_journal["status"] == "ACTIVE", f"Expected ACTIVE status, got {target_journal['status']}"
    
    # 5. Boot Recovery Integration Simulation
    logger.info("Simulating boot-time recovery hook...")
    
    # Clear state provider pending assets to simulate clean boot
    sp.pending_assets.clear()
    
    # Run recovery code block
    recovered_count = 0
    active_journals_boot = await db.load_active_dca_journals()
    for journal in active_journals_boot:
        if journal["signal_id"] == signal_id:
            sig = await db.load_signal_by_id(journal["signal_id"])
            if sig:
                # Add dca_config back to signal (since load_signal_by_id doesn't reconstruct the nested dca_config directly)
                sig["dca_config"] = mock_signal["dca_config"]
                # Override no_price to bypass the 0.50 mid-price guard in paper mode
                sig["no_price"] = 0.52
                # Resume dca
                await executor.resume_dca(sig, journal)
                recovered_count += 1
                
    assert recovered_count == 1, "Failed to initiate recovery for the journal!"
    
    # 6. Verify that it completes the remaining 3 rounds (rounds 3, 4, 5)
    # Recovered DCA starts, completes round 3 at 0s, sleeps 1s, round 4 at 1s, sleeps 1s, round 5 at 2s, completes loop.
    logger.info("Waiting for recovered DCA to complete remaining rounds...")
    await asyncio.sleep(3.5)
    
    # Drain queue
    await asyncio.sleep(0.5)
    
    # 7. Verify DB state after recovery completion
    async with db.pool.acquire() as conn:
        journal_row = await conn.fetchrow("SELECT * FROM dca_execution_journal WHERE signal_id = $1", uuid.UUID(signal_id))
        
    assert journal_row is not None, "DCA journal missing after completion!"
    logger.info(f"Final journal row state: {dict(journal_row)}")
    assert journal_row["rounds_completed"] == 5, f"Expected 5 rounds completed, got {journal_row['rounds_completed']}"
    assert journal_row["status"] == "COMPLETED", f"Expected COMPLETED status, got {journal_row['status']}"
    
    # 8. Clean up test data
    logger.info("Cleaning up mock test rows from database...")
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM positions WHERE signal_id = $1", uuid.UUID(signal_id))
        await conn.execute("DELETE FROM dca_execution_journal WHERE signal_id = $1", uuid.UUID(signal_id))
        await conn.execute("DELETE FROM signals WHERE id = $1", uuid.UUID(signal_id))
        
    await db_writer.shutdown()
    await db.close()
    logger.info("✅ DCA Recovery Test PASSED successfully!")

if __name__ == "__main__":
    asyncio.run(main())
