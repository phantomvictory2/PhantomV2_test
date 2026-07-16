import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from database import DatabaseManager

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def test_recovery():
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    if not url:
        print("DATABASE_URL not set in environment. Skipping DB test.")
        return
        
    db = DatabaseManager(url)
    await db.initialize()
    await db.run_migrations("schema.sql")
    
    # Clean up any existing tests
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM positions WHERE market_id LIKE 'mock_stats_rec%'")
        await conn.execute("DELETE FROM signals WHERE market_id LIKE 'mock_stats_rec%'")
        
    print("Testing Daily Stats and Consecutive Loss Recovery...")
    
    # 1. Create signals and positions
    sig_id1 = uuid.uuid4()
    sig_id2 = uuid.uuid4()
    sig_id3 = uuid.uuid4()
    
    # Insert signals
    async with db.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO signals (id, asset, strategy_type, direction, yes_price, no_price, market_id, outcome)
            VALUES 
                ($1, 'BTC', 'ORBIT_A', 'BUY_YES', 0.70, 0.30, 'mock_stats_rec_1', 'EXECUTED'),
                ($2, 'BTC', 'ORBIT_A', 'BUY_YES', 0.72, 0.28, 'mock_stats_rec_2', 'EXECUTED'),
                ($3, 'BTC', 'ORBIT_A', 'BUY_YES', 0.75, 0.25, 'mock_stats_rec_3', 'EXECUTED')
        """, sig_id1, sig_id2, sig_id3)
        
        # Insert positions (some won, some lost today, some in the past)
        # Position 1: Won today
        now = datetime.now(timezone.utc)
        await conn.execute("""
            INSERT INTO positions (id, signal_id, strategy_type, market_id, asset, direction, entry_price, size_usdc, status, exit_price, pnl, closed_at, is_paper)
            VALUES (gen_random_uuid(), $1, 'ORBIT_A', 'mock_stats_rec_1', 'BTC', 'BUY_YES', 0.70, 10.0, 'CLOSED', 0.85, 2.14, $2, TRUE)
        """, sig_id1, now - timedelta(hours=1))
        
        # Position 2: Lost today (most recent trade)
        await conn.execute("""
            INSERT INTO positions (id, signal_id, strategy_type, market_id, asset, direction, entry_price, size_usdc, status, exit_price, pnl, closed_at, is_paper)
            VALUES (gen_random_uuid(), $1, 'ORBIT_A', 'mock_stats_rec_2', 'BTC', 'BUY_YES', 0.72, 10.0, 'CLOSED', 0.60, -1.67, $2, TRUE)
        """, sig_id2, now)
        
        # Position 3: Lost yesterday
        await conn.execute("""
            INSERT INTO positions (id, signal_id, strategy_type, market_id, asset, direction, entry_price, size_usdc, status, exit_price, pnl, closed_at, is_paper)
            VALUES (gen_random_uuid(), $1, 'ORBIT_A', 'mock_stats_rec_3', 'BTC', 'BUY_YES', 0.75, 10.0, 'CLOSED', 0.60, -2.00, $2, TRUE)
        """, sig_id3, now - timedelta(days=1))

    # Fetch daily stats filtered for our test records to check logic correctness
    async with db.pool.acquire() as conn:
        from datetime import date, time as dt_time
        local_today = date.today()
        start_of_today = datetime.combine(local_today, dt_time.min).astimezone()
        
        stats_row = await conn.fetchrow("""
            SELECT 
                COUNT(*) as today_trades,
                COALESCE(SUM(pnl), 0.0) as today_pnl,
                COUNT(CASE WHEN pnl > 0 THEN 1 END) as today_wins,
                COUNT(CASE WHEN pnl < 0 THEN 1 END) as today_losses
            FROM positions
            WHERE status IN ('CLOSED', 'STOPPED') AND closed_at >= $1 AND market_id LIKE 'mock_stats_rec%'
        """, start_of_today)
        
        recent_rows = await conn.fetch("""
            SELECT pnl, closed_at
            FROM positions
            WHERE status IN ('CLOSED', 'STOPPED') AND market_id LIKE 'mock_stats_rec%'
            ORDER BY closed_at DESC
            LIMIT 50
        """)
        
        consecutive_losses = 0
        last_loss_time = 0.0
        for r in recent_rows:
            pnl = r["pnl"]
            if pnl is not None and pnl < 0:
                consecutive_losses += 1
                if consecutive_losses == 1:
                    closed_at = r["closed_at"]
                    if closed_at:
                        last_loss_time = closed_at.timestamp()
            elif pnl is not None and (pnl > 0 or pnl == 0):
                break
                
        stats = {
            "today_trades": stats_row["today_trades"],
            "today_pnl": float(stats_row["today_pnl"]),
            "today_wins": stats_row["today_wins"],
            "today_losses": stats_row["today_losses"],
            "consecutive_losses": consecutive_losses,
            "last_loss_time": last_loss_time
        }
        
    print("Fetched filtered test stats from DB:")
    print(stats)
    
    assert stats["today_trades"] == 2, f"Expected 2 today trades, got {stats['today_trades']}"
    assert stats["today_wins"] == 1, f"Expected 1 today win, got {stats['today_wins']}"
    assert stats["today_losses"] == 1, f"Expected 1 today loss, got {stats['today_losses']}"
    assert abs(stats["today_pnl"] - 0.47) < 1e-4, f"Expected today pnl 0.47, got {stats['today_pnl']}"
    assert stats["consecutive_losses"] == 1, f"Expected 1 consecutive loss, got {stats['consecutive_losses']}"
    assert stats["last_loss_time"] > 0, "Expected last loss time to be set"
    
    # Clean up test data
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM positions WHERE market_id LIKE 'mock_stats_rec%'")
        await conn.execute("DELETE FROM signals WHERE market_id LIKE 'mock_stats_rec%'")
        
    print("[PASS] test_daily_stats_recovery passed successfully!")
    await db.pool.close()

if __name__ == "__main__":
    asyncio.run(test_recovery())
