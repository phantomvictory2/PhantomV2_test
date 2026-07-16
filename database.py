import asyncio
import logging
import uuid
import json
import time
import copy
from datetime import datetime, timezone
import asyncpg

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None
        self._pool_lock = asyncio.Lock()
        
        # Dashboard state cache to prevent connection pool exhaustion from concurrent polling
        self._dashboard_state_cache = None
        self._dashboard_state_cache_time = 0.0
        self._dashboard_state_cache_lock = asyncio.Lock()

    async def initialize(self):
        logger.info("Initializing database connection pool...")
        self.pool = await asyncpg.create_pool(
            dsn=self.dsn,
            statement_cache_size=0,
            min_size=1,
            max_size=10,
            timeout=10.0,
            command_timeout=15.0,
            max_inactive_connection_lifetime=30.0,
        )
        logger.info("Database pool initialized successfully.")

    async def ensure_pool(self):
        """Recreate the connection pool if it is closed or unhealthy."""
        async with self._pool_lock:
            needs_rebuild = False
            if self.pool is None:
                needs_rebuild = True
            else:
                try:
                    # Quick health-check: acquire a connection and run SELECT 1
                    async with asyncio.timeout(5.0):
                        async with self.pool.acquire(timeout=5.0) as conn:
                            await conn.fetchval("SELECT 1")
                except Exception as hc_err:
                    logger.warning(f"DB pool health-check failed ({hc_err}), rebuilding pool...")
                    needs_rebuild = True

            if needs_rebuild:
                logger.info("Rebuilding database connection pool...")
                new_pool = await asyncpg.create_pool(
                    dsn=self.dsn,
                    statement_cache_size=0,
                    min_size=1,
                    max_size=10,
                    timeout=10.0,
                    command_timeout=15.0,
                    max_inactive_connection_lifetime=30.0,
                )
                old_pool = self.pool
                self.pool = new_pool
                if old_pool:
                    try:
                        await old_pool.close()
                    except Exception:
                        pass
                logger.info("Database pool rebuilt successfully.")

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed.")

    async def run_migrations(self, schema_file: str):
        # Check if system_config table exists
        async with self.pool.acquire(timeout=5.0) as conn:
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'system_config'
                )
            """)
            if not table_exists:
                logger.info(f"Database tables do not exist. Running {schema_file}...")
                with open(schema_file, 'r') as f:
                    schema_sql = f.read()
                # Run the sql schema
                await conn.execute(schema_sql)
                logger.info("Database schema applied successfully.")
            else:
                logger.info("Database tables already exist. Checking for newer tables...")
                
                # Check for phase0_edge_log
                phase0_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'phase0_edge_log'
                    )
                """)
                if not phase0_exists:
                    logger.info("phase0_edge_log table does not exist. Creating it...")
                    create_phase0_sql = """
                    CREATE TABLE IF NOT EXISTS phase0_edge_log (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        strategy_type VARCHAR(30),
                        grade VARCHAR(10),
                        asset VARCHAR(10),
                        market_id VARCHAR(100),
                        direction VARCHAR(10),
                        signal_time TIMESTAMP,
                        entry_price_would_have_been FLOAT,
                        resolution_price FLOAT,
                        resolution_time TIMESTAMP,
                        raw_pnl_per_dollar FLOAT,
                        fee_pct_at_entry FLOAT,
                        fee_adjusted_pnl_per_dollar FLOAT,
                        won BOOLEAN,
                        logged_at TIMESTAMP DEFAULT NOW()
                    );
                    """
                    await conn.execute(create_phase0_sql)
                    logger.info("phase0_edge_log table created successfully.")
                
                # Check for dca_execution_journal
                journal_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'dca_execution_journal'
                    )
                """)
                if not journal_exists:
                    logger.info("dca_execution_journal table does not exist. Creating it...")
                    create_journal_sql = """
                    CREATE TABLE IF NOT EXISTS dca_execution_journal (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        signal_id UUID NOT NULL REFERENCES signals(id),
                        asset VARCHAR(10) NOT NULL,
                        direction VARCHAR(10) NOT NULL,
                        rounds_total INT NOT NULL,
                        rounds_completed INT NOT NULL DEFAULT 0,
                        per_round_usdc FLOAT NOT NULL,
                        limit_price FLOAT NOT NULL,
                        interval_seconds INT NOT NULL,
                        total_size_filled FLOAT NOT NULL DEFAULT 0.0,
                        status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
                        journal_version INT NOT NULL DEFAULT 1,
                        created_at TIMESTAMP DEFAULT NOW(),
                        last_updated TIMESTAMP DEFAULT NOW()
                    );
                    """
                    await conn.execute(create_journal_sql)
                    logger.info("dca_execution_journal table created successfully.")
                else:
                    # Check if journal_version column exists
                    col_exists = await conn.fetchval("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.columns
                            WHERE table_name = 'dca_execution_journal' AND column_name = 'journal_version'
                        )
                    """)
                    if not col_exists:
                        logger.info("Adding journal_version column to dca_execution_journal...")
                        await conn.execute("ALTER TABLE dca_execution_journal ADD COLUMN journal_version INT NOT NULL DEFAULT 1")
                        logger.info("dca_execution_journal table migrated: added journal_version column.")
                
                # Check and add new columns to signals table if not exist
                new_cols = {
                    "no_price": "FLOAT",
                    "rejection_reason": "VARCHAR(50)",
                    "ttr_at_rejection": "INT",
                    "oracle_lag_at_rejection": "FLOAT",
                    "signal_velocity_at_rejection": "INT",
                    "spread_at_rejection": "FLOAT"
                }
                for col_name, col_type in new_cols.items():
                    col_exists = await conn.fetchval(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.columns
                            WHERE table_name = 'signals' AND column_name = '{col_name}'
                        )
                    """)
                    if not col_exists:
                        logger.info(f"Adding {col_name} column to signals table...")
                        await conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_type}")
                        logger.info(f"signals table migrated: added {col_name} column.")

                # Check and add columns to positions table
                pos_new_cols = {
                    "would_new_safeguard_have_blocked": "BOOLEAN DEFAULT FALSE",
                    "safeguard_replay_confidence": "VARCHAR(20) DEFAULT NULL"
                }
                for col_name, col_type in pos_new_cols.items():
                    col_exists = await conn.fetchval(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.columns
                            WHERE table_name = 'positions' AND column_name = '{col_name}'
                        )
                    """)
                    if not col_exists:
                        logger.info(f"Adding {col_name} column to positions table...")
                        await conn.execute(f"ALTER TABLE positions ADD COLUMN {col_name} {col_type}")
                        logger.info(f"positions table migrated: added {col_name} column.")

                # Check for trade_log table
                trade_log_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'trade_log'
                    )
                """)
                if not trade_log_exists:
                    logger.info("trade_log table does not exist. Creating it...")
                    create_trade_log_sql = """
                    CREATE TABLE IF NOT EXISTS trade_log (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        signal_id VARCHAR(100),
                        asset VARCHAR(10),
                        strategy_type VARCHAR(30),
                        direction VARCHAR(10),
                        gate_blocked_at VARCHAR(50),
                        block_reason TEXT,
                        confidence_score INT,
                        logged_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    """
                    await conn.execute(create_trade_log_sql)
                    logger.info("trade_log table created successfully.")

    async def save_signal(self, sig: dict):
        query = """
        INSERT INTO signals (
            id, asset, strategy_type, direction, magnitude_pct, magnitude_usd, 
            duration_seconds, grade, poly_staleness_seconds, spread, yes_price, no_price,
            market_id, time_to_resolution_seconds, liquidity_usdc, velocity_count, 
            outcome, skip_reason, rejection_reason, ttr_at_rejection, 
            oracle_lag_at_rejection, signal_velocity_at_rejection, spread_at_rejection, 
            fired_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 
            $18, $19, $20, $21, $22, $23, NOW()
        )
        ON CONFLICT (id) DO NOTHING
        """
        sig_id = sig.get("signal_id")
        if not sig_id:
            sig_id = str(uuid.uuid4())
        
        # Determine outcome
        outcome = sig.get("outcome")
        if not outcome:
            outcome = "SKIPPED" if sig.get("grade") == "SKIP" else "APPROVED"

        async with self.pool.acquire(timeout=5.0) as conn:
            await conn.execute(
                query,
                uuid.UUID(sig_id),
                sig.get("asset"),
                sig.get("strategy_type", "TICK_EVALUATION"),
                sig.get("direction"),
                sig.get("magnitude_pct"),
                sig.get("magnitude_usd"),
                sig.get("duration_seconds"),
                sig.get("grade"),
                sig.get("poly_staleness_seconds"),
                sig.get("spread"),
                sig.get("yes_price"),
                sig.get("no_price"),
                sig.get("market_id"),
                sig.get("time_to_resolution_seconds"),
                sig.get("liquidity_usdc"),
                sig.get("velocity_count"),
                outcome,
                sig.get("skip_reason"),
                sig.get("rejection_reason"),
                sig.get("ttr_at_rejection"),
                sig.get("oracle_lag_at_rejection"),
                sig.get("signal_velocity_at_rejection"),
                sig.get("spread_at_rejection")
            )

    async def save_position(self, pos: dict):
        query = """
        INSERT INTO positions (
            id, signal_id, strategy_type, market_id, asset, direction, entry_price, 
            size_usdc, entry_mode, dca_rounds_completed, status, is_paper, signal_to_fill_ms, opened_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
        )
        """
        opened_at = pos.get("opened_at", int(time.time() * 1000))
        opened_at_dt = datetime.fromtimestamp(opened_at / 1000.0, tz=timezone.utc)
        
        async with self.pool.acquire(timeout=5.0) as conn:
            await conn.execute(
                query,
                uuid.UUID(pos["id"]),
                uuid.UUID(pos["signal_id"]) if pos.get("signal_id") else None,
                pos.get("strategy_type"),
                pos.get("market_id"),
                pos.get("asset"),
                pos.get("direction"),
                pos.get("entry_price"),
                pos.get("size_usdc"),
                pos.get("entry_mode", "SINGLE"),
                pos.get("dca_rounds_completed", 1),
                pos.get("status", "OPEN"),
                pos.get("is_paper", True),
                pos.get("signal_to_fill_ms", 0),
                opened_at_dt
            )

    async def update_position(self, pos: dict):
        query = """
        UPDATE positions SET
            status = $2,
            exit_price = $3,
            pnl = $4,
            close_reason = $5,
            actual_lag_seconds = $6,
            closed_at = $7
        WHERE id = $1
        """
        closed_at = pos.get("closed_at", int(time.time() * 1000))
        closed_at_dt = datetime.fromtimestamp(closed_at / 1000.0, tz=timezone.utc)
        
        async with self.pool.acquire(timeout=5.0) as conn:
            await conn.execute(
                query,
                uuid.UUID(pos["id"]),
                pos.get("status"),
                pos.get("exit_price"),
                pos.get("pnl"),
                pos.get("close_reason"),
                pos.get("actual_lag_seconds"),
                closed_at_dt
            )

    async def update_position_transaction(self, pos: dict):
        query = """
        UPDATE positions SET
            status = $2,
            exit_price = $3,
            pnl = $4,
            close_reason = $5,
            actual_lag_seconds = $6,
            closed_at = $7
        WHERE id = $1
        """
        closed_at = pos.get("closed_at", int(time.time() * 1000))
        closed_at_dt = datetime.fromtimestamp(closed_at / 1000.0, tz=timezone.utc)
        
        async with self.pool.acquire(timeout=5.0) as conn:
            async with conn.transaction():
                await conn.execute(
                    query,
                    uuid.UUID(pos["id"]),
                    pos.get("status"),
                    pos.get("exit_price"),
                    pos.get("pnl"),
                    pos.get("close_reason"),
                    pos.get("actual_lag_seconds"),
                    closed_at_dt
                )

    async def save_risk_event(self, event: dict):
        query = """
        INSERT INTO risk_events (
            id, event_type, signal_id, asset, strategy_type, trigger_value, action_taken, timestamp
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, NOW()
        )
        """
        async with self.pool.acquire(timeout=5.0) as conn:
            await conn.execute(
                query,
                uuid.uuid4(),
                event.get("event_type"),
                uuid.UUID(event["signal_id"]) if event.get("signal_id") else None,
                event.get("asset"),
                event.get("strategy_type"),
                str(event.get("trigger_value")),
                event.get("action_taken")
            )

    async def upsert_strategy_stats(self, stats: dict):
        query = """
        INSERT INTO strategy_stats (
            strategy_type, trades_total, wins, losses, win_rate, avg_pnl_per_trade, 
            total_pnl, profit_factor, max_drawdown, avg_execution_ms, capital_weight, status, last_updated
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW()
        )
        ON CONFLICT (strategy_type) DO UPDATE SET
            trades_total = EXCLUDED.trades_total,
            wins = EXCLUDED.wins,
            losses = EXCLUDED.losses,
            win_rate = EXCLUDED.win_rate,
            avg_pnl_per_trade = EXCLUDED.avg_pnl_per_trade,
            total_pnl = EXCLUDED.total_pnl,
            profit_factor = EXCLUDED.profit_factor,
            max_drawdown = EXCLUDED.max_drawdown,
            avg_execution_ms = EXCLUDED.avg_execution_ms,
            capital_weight = EXCLUDED.capital_weight,
            status = EXCLUDED.status,
            last_updated = NOW()
        """
        pf = stats.get("profit_factor", 0.0)
        if pf == float('inf'):
            pf = 999.0
        elif pf == float('-inf'):
            pf = -999.0
            
        async with self.pool.acquire(timeout=5.0) as conn:
            await conn.execute(
                query,
                stats.get("strategy_type"),
                stats.get("trades_total"),
                stats.get("wins"),
                stats.get("losses"),
                stats.get("win_rate"),
                stats.get("avg_pnl_per_trade"),
                stats.get("total_pnl"),
                pf,
                stats.get("max_drawdown"),
                stats.get("avg_execution_ms"),
                stats.get("capital_weight", 0.20),
                stats.get("status", "ACTIVE")
            )

    async def upsert_asset_lag(self, asset: str, lag: float):
        query = """
        INSERT INTO asset_lag_stats (
            asset, avg_lag_seconds, sample_size, min_lag_seconds, max_lag_seconds, status, last_updated
        ) VALUES (
            $1, $2, 1, $2, $2, 'ACTIVE', NOW()
        )
        ON CONFLICT (asset) DO UPDATE SET
            avg_lag_seconds = (asset_lag_stats.avg_lag_seconds * asset_lag_stats.sample_size + EXCLUDED.avg_lag_seconds) / (asset_lag_stats.sample_size + 1),
            min_lag_seconds = LEAST(asset_lag_stats.min_lag_seconds, EXCLUDED.min_lag_seconds),
            max_lag_seconds = GREATEST(asset_lag_stats.max_lag_seconds, EXCLUDED.max_lag_seconds),
            sample_size = asset_lag_stats.sample_size + 1,
            last_updated = NOW()
        """
        async with self.pool.acquire(timeout=5.0) as conn:
            await conn.execute(query, asset, lag)

    async def insert_phase0_log(self, log_entry: dict):
        query = """
        INSERT INTO phase0_edge_log (
            strategy_type, grade, asset, market_id, direction, signal_time, entry_price_would_have_been, fee_pct_at_entry
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8
        )
        RETURNING id
        """
        signal_time_dt = datetime.fromtimestamp(log_entry.get("signal_time", time.time() * 1000) / 1000.0, tz=timezone.utc)
        async with self.pool.acquire(timeout=5.0) as conn:
            return await conn.fetchval(
                query,
                log_entry.get("strategy_type"),
                log_entry.get("grade"),
                log_entry.get("asset"),
                log_entry.get("market_id"),
                log_entry.get("direction"),
                signal_time_dt,
                log_entry.get("entry_price_would_have_been"),
                log_entry.get("fee_pct_at_entry")
            )

    async def update_phase0_resolution(self, log_id, resolution_price: float, won: bool, raw_pnl: float, fee_adj_pnl: float):
        query = """
        UPDATE phase0_edge_log SET
            resolution_price = $2,
            resolution_time = NOW(),
            raw_pnl_per_dollar = $3,
            fee_adjusted_pnl_per_dollar = $4,
            won = $5
        WHERE id = $1
        """
        async with self.pool.acquire(timeout=5.0) as conn:
            if isinstance(log_id, str):
                log_id = uuid.UUID(log_id)
            await conn.execute(query, log_id, resolution_price, raw_pnl, fee_adj_pnl, won)

    async def load_system_config(self) -> dict:
        async with self.pool.acquire(timeout=5.0) as conn:
            rows = await conn.fetch("SELECT key, value FROM system_config")
            return {r["key"]: r["value"] for r in rows}

    async def load_open_positions(self) -> list:
        query = """
        SELECT id, signal_id, strategy_type, market_id, asset, direction, entry_price, 
               size_usdc, entry_mode, dca_rounds_completed, status, is_paper, signal_to_fill_ms, opened_at
        FROM positions
        WHERE status = 'OPEN'
        """
        async with self.pool.acquire(timeout=5.0) as conn:
            rows = await conn.fetch(query)
            positions = []
            for r in rows:
                opened_at_val = r["opened_at"]
                opened_at_ts = 0
                if opened_at_val:
                    opened_at_ts = int(opened_at_val.timestamp() * 1000)
                positions.append({
                    "id": str(r["id"]),
                    "signal_id": str(r["signal_id"]) if r["signal_id"] else None,
                    "strategy_type": r["strategy_type"],
                    "market_id": r["market_id"],
                    "asset": r["asset"],
                    "direction": r["direction"],
                    "entry_price": r["entry_price"],
                    "size_usdc": r["size_usdc"],
                    "entry_mode": r["entry_mode"],
                    "dca_rounds_completed": r["dca_rounds_completed"],
                    "status": r["status"],
                    "is_paper": r["is_paper"],
                    "signal_to_fill_ms": r["signal_to_fill_ms"],
                    "opened_at": opened_at_ts
                })
            return positions

    async def load_daily_stats_and_losses(self) -> dict:
        from datetime import date, time as dt_time
        local_today = date.today()
        start_of_today = datetime.combine(local_today, dt_time.min).astimezone()
        
        stats_query = """
        SELECT 
            COUNT(*) as today_trades,
            COALESCE(SUM(pnl), 0.0) as today_pnl,
            COUNT(CASE WHEN pnl > 0 THEN 1 END) as today_wins,
            COUNT(CASE WHEN pnl < 0 THEN 1 END) as today_losses
        FROM positions
        WHERE status IN ('CLOSED', 'STOPPED') AND closed_at >= $1
        """
        
        recent_query = """
        SELECT pnl, closed_at
        FROM positions
        WHERE status IN ('CLOSED', 'STOPPED')
        ORDER BY closed_at DESC
        LIMIT 50
        """
        
        async with self.pool.acquire(timeout=5.0) as conn:
            stats_row = await conn.fetchrow(stats_query, start_of_today)
            recent_rows = await conn.fetch(recent_query)
            
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
                    
            return {
                "today_trades": stats_row["today_trades"] if stats_row and stats_row["today_trades"] is not None else 0,
                "today_pnl": float(stats_row["today_pnl"]) if stats_row and stats_row["today_pnl"] is not None else 0.0,
                "today_wins": stats_row["today_wins"] if stats_row and stats_row["today_wins"] is not None else 0,
                "today_losses": stats_row["today_losses"] if stats_row and stats_row["today_losses"] is not None else 0,
                "consecutive_losses": consecutive_losses,
                "last_loss_time": last_loss_time
            }

    async def update_system_config(self, key: str, value: str):
        query = """
        INSERT INTO system_config (key, value, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = NOW()
        """
        async with self.pool.acquire(timeout=5.0) as conn:
            await conn.execute(query, key, str(value))

    async def get_dashboard_state(self, is_paper: bool = True, _retry: bool = True) -> dict:
        """Read all dashboard data from DB. Returns stale cache immediately while refreshing in background."""
        now = time.time()
        
        # If cache is fresh (< 10 seconds), return it immediately
        if self._dashboard_state_cache and (now - self._dashboard_state_cache_time) < 10.0:
            return copy.deepcopy(self._dashboard_state_cache)
            
        # If cache is stale or missing, check if a refresh is already in progress
        if not getattr(self, '_dashboard_refreshing', False):
            self._dashboard_refreshing = True
            
            # Start background refresh task
            async def refresh():
                try:
                    # Run DB fetch
                    result = await self._fetch_dashboard_state_from_db(is_paper=is_paper)
                    self._dashboard_state_cache = result
                    self._dashboard_state_cache_time = time.time()
                except Exception as e:
                    logger.warning(f"Background refresh of dashboard state failed: {e}")
                    # If connection lost, try to ensure pool for the next attempts
                    try:
                        await self.ensure_pool()
                    except Exception:
                        pass
                finally:
                    self._dashboard_refreshing = False
                    
            asyncio.create_task(refresh())
            
        # Return stale cache if available
        if self._dashboard_state_cache:
            return copy.deepcopy(self._dashboard_state_cache)
            
        # If no cache exists yet (first boot), do a quick blocking fetch with 30.0s timeout
        try:
            async with asyncio.timeout(30.0):
                result = await self._fetch_dashboard_state_from_db(is_paper=is_paper)
                self._dashboard_state_cache = result
                self._dashboard_state_cache_time = time.time()
                return copy.deepcopy(result)
        except Exception as e:
            logger.warning(f"First-boot blocking fetch of dashboard state failed: {e!r}")
            # Return empty/fallback state
            return {
                "bankroll": 1000.0,
                "today_pnl": 0.0,
                "kill_switch": False,
                "paper_mode": True,
                "strategies": [],
                "oracles": [],
                "positions": [],
                "risk_events": [],
                "signals": [],
                "trade_history": [],
                "system_stats": {"evaluated": 0, "triggered": 0, "won": 0, "lost": 0},
                "funnel": {"evaluated": 0, "classified": 0, "approved": 0, "attempted": 0, "filled": 0},
                "no_poly_state_count": 0,
                "skips_by_reason": {}
            }

    async def _fetch_dashboard_state_from_db(self, is_paper: bool = True) -> dict:
        try:
            async with self.pool.acquire(timeout=10.0) as conn:
                configs = await conn.fetch("SELECT key, value FROM system_config WHERE key IN ('kill_switch', 'paper_mode', 'bankroll')")
                scalars = await conn.fetchrow("""
                    SELECT
                        (SELECT SUM(pnl) FROM positions WHERE status = 'CLOSED') as pnl_sum,
                        (SELECT SUM(pnl) FROM positions WHERE closed_at >= CURRENT_DATE) as today_pnl,
                        (SELECT COUNT(*) FROM signals WHERE fired_at >= CURRENT_DATE) as evaluated,
                        (SELECT COUNT(*) FROM signals WHERE strategy_type NOT IN ('TICK_EVALUATION', 'UNKNOWN') AND fired_at >= CURRENT_DATE) as classified,
                        (SELECT COUNT(*) FROM signals WHERE outcome IN ('APPROVED', 'EXECUTED') AND fired_at >= CURRENT_DATE) as approved,
                        (SELECT COUNT(*) FROM positions WHERE opened_at >= CURRENT_DATE) as attempted,
                        (SELECT COUNT(*) FROM positions WHERE status IN ('OPEN', 'CLOSED', 'STOPPED') AND opened_at >= CURRENT_DATE) as filled,
                        (SELECT COUNT(*) FROM signals WHERE skip_reason = 'NO_POLY_STATE' AND fired_at >= CURRENT_DATE) as no_poly_state_count,
                        (SELECT COUNT(*) FROM positions WHERE pnl > 0 AND status IN ('CLOSED', 'STOPPED') AND closed_at >= CURRENT_DATE) as total_won,
                        (SELECT COUNT(*) FROM positions WHERE pnl <= 0 AND status IN ('CLOSED', 'STOPPED') AND closed_at >= CURRENT_DATE) as total_lost
                """)
                
                strategies_rows = await conn.fetch("""
                    SELECT strategy_type, trades_total, wins, losses, win_rate, avg_pnl_per_trade, total_pnl, capital_weight, status
                    FROM strategy_stats
                    WHERE strategy_type IN ('ORBIT_A_240', 'PHANTOM_MOMENTUM_V1', 'LAST_SHADOW_TRADE_LITE_V4', 'PHANTOM_ONE_V1')
                """)
                oracles_rows = await conn.fetch("SELECT asset, avg_lag_seconds, sample_size, status FROM asset_lag_stats")
                
                positions_rows = await conn.fetch("""
                    SELECT id, strategy_type, asset, direction, entry_price, size_usdc, opened_at
                    FROM positions
                    WHERE status = 'OPEN'
                """)
                skips_rows = await conn.fetch("""
                    SELECT skip_reason, COUNT(*) as count 
                    FROM signals 
                    WHERE skip_reason IS NOT NULL AND fired_at >= CURRENT_DATE
                    GROUP BY skip_reason
                """)
                
                risk_events_rows = await conn.fetch("""
                    SELECT timestamp, event_type, asset, trigger_value, action_taken
                    FROM risk_events
                    ORDER BY timestamp DESC
                    LIMIT 20
                """)
                signals_rows = await conn.fetch("""
                    SELECT fired_at, strategy_type, asset, direction, yes_price, no_price, grade, outcome, skip_reason
                    FROM signals
                    ORDER BY fired_at DESC
                    LIMIT 20
                """)
                
                closed_rows = await conn.fetch("""
                    SELECT id, signal_id, strategy_type, market_id, asset, direction, entry_price, exit_price, size_usdc, entry_mode, dca_rounds_completed, pnl, close_reason, actual_lag_seconds, signal_to_fill_ms, opened_at, closed_at
                    FROM positions
                    WHERE status IN ('CLOSED', 'STOPPED')
                    ORDER BY closed_at DESC NULLS LAST
                    LIMIT 15
                """)

            config_rows, scalar_row = configs, scalars
            strategy_rows, oracle_rows = strategies_rows, oracles_rows
            pos_rows, skip_rows = positions_rows, skips_rows
            risk_rows, sig_rows = risk_events_rows, signals_rows

            # Process configs
            ks_val = next((r["value"] for r in config_rows if r["key"] == "kill_switch"), None)
            kill_switch = str(ks_val).lower() == "true" if ks_val is not None else False

            pm_val = next((r["value"] for r in config_rows if r["key"] == "paper_mode"), None)
            paper_mode = str(pm_val).lower() == "true" if pm_val is not None else True

            br_val = next((r["value"] for r in config_rows if r["key"] == "bankroll"), None)
            initial_bankroll = float(br_val) if br_val is not None else 1000.0

            pnl_sum = float(scalar_row["pnl_sum"]) if scalar_row["pnl_sum"] is not None else 0.0
            bankroll = initial_bankroll + pnl_sum

            # Today's PNL
            today_pnl = float(scalar_row["today_pnl"]) if scalar_row["today_pnl"] is not None else 0.0

            # Strategies — keys must match frontend fetchState() s.strategy_type / s.trades_total
            strategies = []
            registered_strats = {"ORBIT_A_240", "PHANTOM_MOMENTUM_V1", "LAST_SHADOW_TRADE_LITE_V4", "PHANTOM_ONE_V1"}
            for r in strategy_rows:
                strategies.append({
                    "strategy_type": r["strategy_type"],
                    "trades_total": r["trades_total"],
                    "win_rate": r["win_rate"],
                    "avg_pnl": r["avg_pnl_per_trade"],
                    "total_pnl": r["total_pnl"],
                    "weight": r["capital_weight"],
                    "status": r["status"]
                })
                registered_strats.discard(r["strategy_type"])

            for st in registered_strats:
                strategies.append({
                    "strategy_type": st,
                    "trades_total": 0,
                    "win_rate": 0.0,
                    "avg_pnl": 0.0,
                    "total_pnl": 0.0,
                    "weight": 0.25,
                    "status": "ACTIVE"
                })
            strategies.sort(key=lambda s: s["strategy_type"])

            # Oracles
            oracles = []
            registered_oracles = {"BTC", "ETH", "SOL"}
            for r in oracle_rows:
                oracles.append({
                    "asset": r["asset"],
                    "avg_lag": f"{r['avg_lag_seconds']:.2f}" if r["avg_lag_seconds"] is not None else "0.00",
                    "samples": r["sample_size"],
                    "status": r["status"]
                })
                registered_oracles.discard(r["asset"])

            for o in registered_oracles:
                oracles.append({
                    "asset": o,
                    "avg_lag": "0.00",
                    "samples": 0,
                    "status": "ACTIVE"
                })
            oracles.sort(key=lambda o: o["asset"])

            # Open Positions — keys must match frontend fetchState() p.strategy_type / p.direction / p.entry_price / p.size_usdc
            positions = []
            for r in pos_rows:
                opened_at_ts = r["opened_at"].timestamp() * 1000 if r["opened_at"] else 0
                duration_s = max(0, int(time.time() - opened_at_ts / 1000.0))
                mins = duration_s // 60
                secs = duration_s % 60
                time_open_str = f"{mins}m {secs}s"

                positions.append({
                    "id": str(r["id"])[:8],
                    "strategy_type": r["strategy_type"],
                    "asset": r["asset"],
                    "direction": r["direction"],
                    "entry_price": float(r["entry_price"]) if r["entry_price"] is not None else 0.0,
                    "size_usdc": float(r["size_usdc"]) if r["size_usdc"] is not None else 0.0,
                    "pnl": 0.0,
                    "time_open": time_open_str
                })

            # Risk Events
            risk_events = []
            for r in risk_rows:
                risk_events.append({
                    "time": r["timestamp"].strftime("%H:%M:%S"),
                    "event": r["event_type"],
                    "asset": r["asset"] or "ALL",
                    "trigger": r["trigger_value"],
                    "action": r["action_taken"]
                })

            # Signals — keys must match frontend fetchState() expectations exactly
            signals = []
            for r in sig_rows:
                reason_suffix = f": {r['skip_reason']}" if r['skip_reason'] else ""
                outcome_str = f"{r['outcome']}{reason_suffix}"
                signals.append({
                    "fired_at": r["fired_at"].strftime("%Y-%m-%d %H:%M:%S"),
                    "strategy": r["strategy_type"],
                    "asset": r["asset"],
                    "direction": r["direction"] or "N/A",
                    "binance_price": 0.0,
                    "poly_yes": float(r["yes_price"]) if r["yes_price"] is not None else 0.0,
                    "poly_no": float(r["no_price"]) if r["no_price"] is not None else 0.0,
                    "grade": r["grade"] or "N/A",
                    "status": outcome_str
                })

            # Trade History (Closed Positions)
            trade_history = []
            for r in closed_rows:
                opened_at_ts = r["opened_at"].timestamp() * 1000 if r["opened_at"] else 0
                closed_at_ts = r["closed_at"].timestamp() * 1000 if r["closed_at"] else 0
                duration_s = max(0, int((closed_at_ts - opened_at_ts) / 1000))
                mins = duration_s // 60
                secs = duration_s % 60
                duration_str = f"{mins}m {secs}s" if mins > 0 else f"{secs}s"

                trade_history.append({
                    "id": str(r["id"])[:8],
                    "full_id": str(r["id"]),
                    "signal_id": str(r["signal_id"]) if r["signal_id"] else "N/A",
                    "market_id": r["market_id"] or "N/A",
                    "strategy": r["strategy_type"],
                    "asset": r["asset"],
                    "dir": r["direction"],
                    "entry_mode": r["entry_mode"] or "SINGLE",
                    "dca_rounds_completed": r["dca_rounds_completed"] or 1,
                    "entry": f"{r['entry_price']:.2f}" if r['entry_price'] else "0.00",
                    "exit": f"{r['exit_price']:.2f}" if r['exit_price'] else "0.00",
                    "size": f"{r['size_usdc']:.2f}" if r['size_usdc'] else "0.00",
                    "pnl": float(r["pnl"]) if r["pnl"] is not None else 0.0,
                    "reason": r["close_reason"],
                    "duration": duration_str,
                    "actual_lag": f"{r['actual_lag_seconds']:.2f}s" if r['actual_lag_seconds'] is not None else "0.00s",
                    "fill_speed": f"{r['signal_to_fill_ms']}ms" if r['signal_to_fill_ms'] is not None else "0ms",
                    "opened_at": r["opened_at"].strftime("%Y-%m-%d %H:%M:%S") if r["opened_at"] else "N/A",
                    "closed_at": r["closed_at"].strftime("%Y-%m-%d %H:%M:%S") if r["closed_at"] else "N/A"
                })

            # System Stats & Funnel
            evaluated = scalar_row["evaluated"] or 0
            classified = scalar_row["classified"] or 0
            approved = scalar_row["approved"] or 0
            attempted = scalar_row["attempted"] or 0
            filled = scalar_row["filled"] or 0
            no_poly_state_count = scalar_row["no_poly_state_count"] or 0
            total_won = scalar_row["total_won"] or 0
            total_lost = scalar_row["total_lost"] or 0

            skips_by_reason = {r["skip_reason"]: r["count"] for r in skip_rows}

            system_stats = {
                "evaluated": evaluated,
                "triggered": attempted,
                "won": total_won or 0,
                "lost": total_lost or 0
            }

            funnel = {
                "evaluated": evaluated,
                "classified": classified,
                "approved": approved,
                "attempted": attempted,
                "filled": filled
            }

            return {
                "bankroll": bankroll,
                "today_pnl": today_pnl,
                "kill_switch": kill_switch,
                "paper_mode": paper_mode,
                "strategies": strategies,
                "oracles": oracles,
                "positions": positions,
                "risk_events": risk_events,
                "signals": signals,
                "trade_history": trade_history,
                "system_stats": system_stats,
                "funnel": funnel,
                "no_poly_state_count": no_poly_state_count,
                "skips_by_reason": skips_by_reason
            }
        except (asyncpg.InterfaceError, asyncpg.ConnectionDoesNotExistError, OSError):
            raise

    async def create_dca_journal(self, journal: dict) -> str:
        query = """
        INSERT INTO dca_execution_journal (
            id, signal_id, asset, direction, rounds_total, rounds_completed, 
            per_round_usdc, limit_price, interval_seconds, total_size_filled, 
            status, journal_version, created_at, last_updated
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 1, NOW(), NOW()
        )
        """
        journal_id = journal.get("id")
        if not journal_id:
            journal_id = str(uuid.uuid4())
            
        async with self.pool.acquire(timeout=5.0) as conn:
            await conn.execute(
                query,
                uuid.UUID(journal_id),
                uuid.UUID(journal["signal_id"]),
                journal["asset"],
                journal["direction"],
                journal["rounds_total"],
                journal.get("rounds_completed", 0),
                journal["per_round_usdc"],
                journal["limit_price"],
                journal["interval_seconds"],
                journal.get("total_size_filled", 0.0),
                journal.get("status", "ACTIVE")
            )
        return journal_id

    async def update_dca_journal(self, journal_id: str, rounds_completed: int, total_size_filled: float, status: str):
        query = """
        UPDATE dca_execution_journal SET
            rounds_completed = $2,
            total_size_filled = $3,
            status = $4,
            last_updated = NOW()
        WHERE id = $1
        """
        async with self.pool.acquire(timeout=5.0) as conn:
            await conn.execute(query, uuid.UUID(journal_id), rounds_completed, total_size_filled, status)

    async def load_active_dca_journals(self) -> list:
        query = """
        SELECT id, signal_id, asset, direction, rounds_total, rounds_completed, 
               per_round_usdc, limit_price, interval_seconds, total_size_filled, 
               status, journal_version, created_at, last_updated
        FROM dca_execution_journal
        WHERE status = 'ACTIVE'
        """
        async with self.pool.acquire(timeout=5.0) as conn:
            rows = await conn.fetch(query)
            journals = []
            for r in rows:
                journals.append({
                    "id": str(r["id"]),
                    "signal_id": str(r["signal_id"]),
                    "asset": r["asset"],
                    "direction": r["direction"],
                    "rounds_total": r["rounds_total"],
                    "rounds_completed": r["rounds_completed"],
                    "per_round_usdc": r["per_round_usdc"],
                    "limit_price": r["limit_price"],
                    "interval_seconds": r["interval_seconds"],
                    "total_size_filled": r["total_size_filled"],
                    "status": r["status"],
                    "journal_version": r["journal_version"]
                })
            return journals

    async def load_signal_by_id(self, signal_id: str) -> dict:
        query = """
        SELECT id, asset, strategy_type, direction, magnitude_pct, magnitude_usd, 
               duration_seconds, grade, poly_staleness_seconds, spread, yes_price, no_price, 
               market_id, time_to_resolution_seconds, liquidity_usdc, velocity_count, 
               outcome, skip_reason, fired_at
        FROM signals
        WHERE id = $1
        """
        async with self.pool.acquire(timeout=5.0) as conn:
            r = await conn.fetchrow(query, uuid.UUID(signal_id))
            if not r:
                return None
            return {
                "signal_id": str(r["id"]),
                "asset": r["asset"],
                "strategy_type": r["strategy_type"],
                "direction": r["direction"],
                "magnitude_pct": r["magnitude_pct"],
                "magnitude_usd": r["magnitude_usd"],
                "duration_seconds": r["duration_seconds"],
                "grade": r["grade"],
                "poly_staleness_seconds": r["poly_staleness_seconds"],
                "spread": r["spread"],
                "yes_price": r["yes_price"],
                "no_price": r["no_price"] if r["no_price"] is not None else round(1.0 - r["yes_price"], 4),
                "market_id": r["market_id"],
                "time_to_resolution_seconds": r["time_to_resolution_seconds"],
                "liquidity_usdc": r["liquidity_usdc"],
                "velocity_count": r["velocity_count"],
                "entry_mode": "DCA"
            }

    async def load_asset_lag_stats(self) -> dict:
        query = "SELECT asset, avg_lag_seconds, sample_size, status FROM asset_lag_stats"
        async with self.pool.acquire(timeout=5.0) as conn:
            rows = await conn.fetch(query)
            result = {}
            for r in rows:
                status = r["status"]
                # A DISABLED row with no samples is a crash artifact — reset to ACTIVE
                if status == "DISABLED" and (r["sample_size"] or 0) == 0:
                    status = "ACTIVE"
                result[r["asset"]] = {
                    "avg_lag_seconds": r["avg_lag_seconds"],
                    "sample_size": r["sample_size"] or 0,
                    "status": status,
                }
            return result

    async def get_funnel_stats(self, boot_time: datetime) -> dict:
        async with self.pool.acquire(timeout=5.0) as conn:
            # 1. Raw evaluations (ticks)
            evaluated = await conn.fetchval(
                "SELECT COUNT(*) FROM signals WHERE fired_at > $1", 
                boot_time
            ) or 0
            
            # 2. Classified signals (non-UNKNOWN/TICK_EVALUATION strategy types)
            classified = await conn.fetchval(
                "SELECT COUNT(*) FROM signals WHERE fired_at > $1 AND strategy_type NOT IN ('TICK_EVALUATION', 'UNKNOWN')",
                boot_time
            ) or 0
            
            # 3. Risk engine evaluations (signals not skipped or superseded at signal engine level)
            risk_evals = await conn.fetchval(
                "SELECT COUNT(*) FROM signals WHERE fired_at > $1 AND outcome NOT IN ('SKIPPED', 'SUPERSEDED')",
                boot_time
            ) or 0
            
            # 4. Executed trades count (from positions table)
            executed = await conn.fetchval(
                "SELECT COUNT(*) FROM positions WHERE opened_at > $1",
                boot_time
            ) or 0

            # 5. Rejection reasons breakdown
            rejection_rows = await conn.fetch("""
                SELECT rejection_reason, COUNT(*) as count
                FROM signals
                WHERE fired_at > $1 AND outcome = 'REJECTED' AND rejection_reason IS NOT NULL
                GROUP BY rejection_reason
            """, boot_time)
            
            rejection_breakdown = {
                "NO_LIVE_FEED": 0,
                "CHECK_3_TTR": 0,
                "CHECK_3_SPREAD": 0,
                "CHECK_3_LIQUIDITY": 0,
                "CHECK_5_COOLDOWN": 0,
                "CHECK_6_VELOCITY": 0,
                "CHECK_7_ORACLE": 0,
                "CHECK_8_EXCLUSIVITY": 0,
                "CHECK_8_POS_LIMIT": 0,
                "CHECK_9_DAILY_LIMIT": 0,
                "CHECK_11_STALENESS": 0
            }
            for r in rejection_rows:
                reason = r["rejection_reason"]
                if reason in rejection_breakdown:
                    rejection_breakdown[reason] = r["count"]

            return {
                "evaluated": evaluated,
                "classified": classified,
                "risk_evaluations": risk_evals,
                "executed": executed,
                "rejection_breakdown": rejection_breakdown
            }

    async def get_100_eval_report_stats(self, boot_time: datetime) -> dict:
        async with self.pool.acquire(timeout=5.0) as conn:
            # Counts
            total = await conn.fetchval("SELECT COUNT(*) FROM signals WHERE fired_at > $1", boot_time) or 0
            approved = await conn.fetchval("SELECT COUNT(*) FROM signals WHERE fired_at > $1 AND outcome IN ('APPROVED', 'EXECUTED')", boot_time) or 0
            rejected = await conn.fetchval("SELECT COUNT(*) FROM signals WHERE fired_at > $1 AND outcome = 'REJECTED'", boot_time) or 0
            skipped = await conn.fetchval("SELECT COUNT(*) FROM signals WHERE fired_at > $1 AND outcome = 'SKIPPED'", boot_time) or 0
            superseded = await conn.fetchval("SELECT COUNT(*) FROM signals WHERE fired_at > $1 AND outcome = 'SUPERSEDED'", boot_time) or 0
            
            # Rejection breakdown
            rej_rows = await conn.fetch("""
                SELECT rejection_reason, COUNT(*) as count
                FROM signals
                WHERE fired_at > $1 AND outcome = 'REJECTED' AND rejection_reason IS NOT NULL
                GROUP BY rejection_reason
            """, boot_time)
            rejections = {r["rejection_reason"]: r["count"] for r in rej_rows}
            
            # Positions/P&L stats since boot
            trades = await conn.fetchval("SELECT COUNT(*) FROM positions WHERE opened_at > $1", boot_time) or 0
            pnl = await conn.fetchval("SELECT SUM(pnl) FROM positions WHERE opened_at > $1 AND status IN ('CLOSED', 'STOPPED')", boot_time) or 0.0
            wins = await conn.fetchval("SELECT COUNT(*) FROM positions WHERE opened_at > $1 AND status IN ('CLOSED', 'STOPPED') AND pnl > 0", boot_time) or 0
            losses = await conn.fetchval("SELECT COUNT(*) FROM positions WHERE opened_at > $1 AND status IN ('CLOSED', 'STOPPED') AND pnl <= 0", boot_time) or 0
            
            return {
                "total_evals": total,
                "approved": approved,
                "rejected": rejected,
                "skipped": skipped,
                "superseded": superseded,
                "rejections": rejections,
                "trades_executed": trades,
                "net_pnl": float(pnl),
                "wins": wins,
                "losses": losses
            }

    async def save_trade_log(self, log_data: dict):
        query = """
        INSERT INTO trade_log (
            signal_id, asset, strategy_type, direction, gate_blocked_at, block_reason, confidence_score, logged_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, NOW()
        )
        """
        async with self.pool.acquire(timeout=5.0) as conn:
            await conn.execute(
                query,
                log_data.get("signal_id"),
                log_data.get("asset"),
                log_data.get("strategy_type"),
                log_data.get("direction"),
                log_data.get("gate_blocked_at"),
                log_data.get("block_reason"),
                log_data.get("confidence_score")
            )

    async def load_historical_positions_with_signals(self) -> list:
        query = """
        SELECT p.id as position_id, p.strategy_type, p.direction,
               s.time_to_resolution_seconds, s.fired_at, s.yes_price, s.no_price, s.spread, s.liquidity_usdc
        FROM positions p
        JOIN signals s ON p.signal_id = s.id
        """
        async with self.pool.acquire(timeout=5.0) as conn:
            rows = await conn.fetch(query)
            return [dict(r) for r in rows]

    async def update_position_safeguard(self, pos_id: str, blocked: bool, confidence: str):
        query = """
        UPDATE positions
        SET would_new_safeguard_have_blocked = $1, safeguard_replay_confidence = $2
        WHERE id = $3
        """
        async with self.pool.acquire(timeout=5.0) as conn:
            await conn.exec