import asyncio
import os
import sys
import time
import uuid
import logging
from dotenv import load_dotenv

from database import DatabaseManager
from database_writer import DatabaseWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def run_stress_test(db, count):
    logger.info(f"\n--- Starting Stress Test with {count} Writes ---")
    
    # Initialize the writer with a small queue to trigger drops easily
    # e.g., max_size = 100
    max_queue_size = 100
    writer = DatabaseWriter(db, max_size=max_queue_size)
    writer.start()
    
    start_time = time.perf_counter()
    
    # Generate mix of critical and non-critical writes
    tasks = []
    
    logger.info(f"Injecting {count} writes into queue of size {max_queue_size}...")
    for i in range(count):
        # Every 3rd write is critical
        is_critical = (i % 3 == 0)
        
        if is_critical:
            # Generate a signal first to satisfy the foreign key constraint
            sig_id = str(uuid.uuid4())
            signal = {
                "signal_id": sig_id,
                "asset": "BTC",
                "strategy_type": "MOMENTUM_RIDE_5M",
                "direction": "BUY_YES",
                "magnitude_pct": 0.5,
                "magnitude_usd": 250.0,
                "duration_seconds": 10,
                "grade": "A",
                "poly_staleness_seconds": 1.0,
                "spread": 0.01,
                "yes_price": 0.50,
                "market_id": "0x_stress_market",
                "time_to_resolution_seconds": 200,
                "liquidity_usdc": 5000.0,
                "velocity_count": 1,
                "outcome": "APPROVED"
            }
            pos_id = str(uuid.uuid4())
            position = {
                "id": pos_id,
                "signal_id": sig_id,
                "strategy_type": "MOMENTUM_RIDE_5M",
                "market_id": "0x_stress_market",
                "asset": "BTC",
                "direction": "BUY_YES",
                "entry_price": 0.50,
                "size_usdc": 10.0,
                "entry_mode": "SINGLE",
                "dca_rounds_completed": 1,
                "status": "OPEN",
                "is_paper": True,
                "signal_to_fill_ms": 10,
                "opened_at": int(time.time() * 1000)
            }
            tasks.append(writer.write("signals", signal))
            tasks.append(writer.write("positions", position))
        else:
            # Non-critical skipped signal write
            signal = {
                "signal_id": str(uuid.uuid4()),
                "asset": "BTC",
                "strategy_type": "MOMENTUM_RIDE_5M",
                "direction": "BUY_YES",
                "magnitude_pct": 0.1,
                "magnitude_usd": 10.0,
                "duration_seconds": 10,
                "grade": "SKIP",
                "poly_staleness_seconds": 1.0,
                "spread": 0.01,
                "yes_price": 0.50,
                "market_id": "0x_stress_market",
                "time_to_resolution_seconds": 3600,
                "liquidity_usdc": 5000.0,
                "velocity_count": 1,
                "outcome": "SKIPPED",
                "skip_reason": "below threshold"
            }
            tasks.append(writer.write("signals", signal))
            
    # Run the enqueues concurrently
    await asyncio.gather(*[t for t in tasks if t is not None])
    
    enqueue_time = (time.perf_counter() - start_time) * 1000
    logger.info(f"Finished enqueuing in {enqueue_time:.2f}ms")
    
    # Wait for queue to drain
    logger.info("Waiting for queue to drain...")
    await writer.shutdown()
    
    total_time = (time.perf_counter() - start_time) * 1000
    metrics = writer.get_metrics()
    
    logger.info(f"Stress Test Results ({count} total writes):")
    logger.info(f"  Total processing time: {total_time:.2f}ms")
    logger.info(f"  Average queue depth: {metrics['avg_queue_depth']}")
    logger.info(f"  Peak queue depth: {metrics['peak_queue_depth']}")
    logger.info(f"  95th percentile queue depth: {metrics['p95_queue_depth']}")
    logger.info(f"  Queued: {metrics['total_queued']}")
    logger.info(f"  Written: {metrics['total_written']}")
    logger.info(f"  Dropped: {metrics['total_dropped']}")
    logger.info(f"  Failed: {metrics['total_failed']}")
    
    # Cleanup test data in database
    logger.info("Cleaning up stress test positions...")
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM positions WHERE market_id = '0x_stress_market'")
        await conn.execute("DELETE FROM signals WHERE market_id = '0x_stress_market'")

async def main():
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    if not url:
        logger.error("DATABASE_URL not set in environment.")
        return
        
    db = DatabaseManager(url)
    await db.initialize()
    await db.run_migrations("schema.sql")
    
    # Run stress tests for different volumes
    await run_stress_test(db, 100)
    await run_stress_test(db, 500)
    await run_stress_test(db, 1000)
    
    await db.close()

if __name__ == "__main__":
    asyncio.run(main())
