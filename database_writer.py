import asyncio
import logging
import time
import math
from typing import Dict, Any, Callable, Optional
from collections import deque
import asyncpg

logger = logging.getLogger(__name__)

class DatabaseWriter:
    def __init__(
        self,
        db_manager,
        max_size: int = 1000,
        telegram_callback: Optional[Callable[[str], Any]] = None
    ):
        self.db_manager = db_manager
        self.max_size = max_size
        self.telegram_callback = telegram_callback
        
        self._queue = asyncio.Queue(maxsize=max_size)
        self._accepting_writes = True
        self._worker_task = None
        self._sampling_task = None
        
        # Alert thresholds
        self._warning_threshold = int(max_size * 0.50)
        self._critical_threshold = int(max_size * 0.80)
        self._emergency_threshold = int(max_size * 0.95)
        
        self._last_alert_level = 0  # 0: OK, 1: WARNING, 2: CRITICAL, 3: EMERGENCY
        
        # Metrics
        self.total_queued = 0
        self.total_written = 0
        self.total_failed = 0
        self.total_dropped = 0
        self.samples = deque(maxlen=100000)
        
    def start(self):
        """Starts the background worker loops."""
        logger.info("Starting DatabaseWriter worker loop...")
        self._worker_task = asyncio.create_task(self._worker_loop())
        self._sampling_task = asyncio.create_task(self._sampling_loop())
        
    async def write(self, table: str, data: Any):
        """
        Enqueues a database write task.
        Critical items block if queue is full.
        Non-critical items are dropped if queue is full.
        """
        if not self._accepting_writes:
            logger.warning(f"DatabaseWriter is shutting down, rejecting write for {table}")
            self.total_dropped += 1
            return
            
        # Determine if critical
        is_critical = self._is_critical(table, data)
        
        if is_critical:
            try:
                # Block until space is available
                await self._queue.put((table, data))
                self.total_queued += 1
                self._check_queue_health()
            except Exception as e:
                logger.error(f"Error enqueuing critical write for {table}: {e}")
                self.total_failed += 1
        else:
            try:
                # Attempt to enqueue without blocking
                self._queue.put_nowait((table, data))
                self.total_queued += 1
                self._check_queue_health()
            except asyncio.QueueFull:
                logger.warning(f"QUEUE_FULL: Dropping non-critical write for {table}")
                self.total_dropped += 1
                
    def _is_critical(self, table: str, data: Any) -> bool:
        if table in ("positions", "positions_update", "risk_events", "dca_journal_create", "dca_journal_update"):
            return True
        if table == "signals":
            # Critical if not skipped or if executed/approved
            outcome = data.get("outcome")
            grade = data.get("grade")
            if outcome in ("EXECUTED", "APPROVED") or (grade != "SKIP" and outcome != "SKIPPED"):
                return True
        return False
        
    async def _worker_loop(self):
        while True:
            try:
                item = await self._queue.get()
                table, data = item
                
                await self._process_write_with_retry(table, data)
                
                self._queue.task_done()
                self._check_queue_health()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unhandled error in DatabaseWriter worker: {e}", exc_info=True)
                
    async def _process_write_with_retry(self, table: str, data: Any):
        retries = 5
        delay = 1.0
        success = False
        
        for attempt in range(retries):
            try:
                await self._execute_db_write(table, data)
                success = True
                self.total_written += 1
                break
            except asyncpg.exceptions.UniqueViolationError as e:
                logger.warning(f"DB write duplicate key on table '{table}' — skipping: {e}")
                break
            except asyncpg.exceptions.ForeignKeyViolationError as e:
                logger.error(f"DB write FK violation on table '{table}' — skipping: {e}", exc_info=True)
                break
            except (asyncpg.InterfaceError, asyncpg.InternalClientError, ConnectionError, OSError) as e:
                logger.warning(
                    f"DB write connection failure on table '{table}' (attempt {attempt+1}/{retries}): {e}"
                )
                if attempt < retries - 1:
                    try:
                        await self.db_manager.ensure_pool()
                    except Exception as rebuild_err:
                        logger.error(f"Failed to rebuild DB pool: {rebuild_err}")
                    await asyncio.sleep(delay)
                    delay *= 2
            except RuntimeError as e:
                if "pool" in str(e) or "connection" in str(e).lower():
                    logger.warning(f"DB write pool error on table '{table}' (attempt {attempt+1}/{retries}): {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(delay)
                        delay *= 2
                else:
                    logger.error(f"DB write logic error on table '{table}': {e}", exc_info=True)
                    break
            except Exception as e:
                logger.error(f"DB write unexpected error on table '{table}': {e}", exc_info=True)
                break
                
        if not success:
            logger.error(f"DB WRITE FAILED after {retries} retries for table '{table}'")
            self.total_failed += 1
            
    async def _execute_db_write(self, table: str, data: Any):
        if not self.db_manager or not self.db_manager.pool:
            raise RuntimeError("DatabaseManager not initialized or pool is None")
            
        if table == "signals":
            await self.db_manager.save_signal(data)
        elif table == "positions":
            await self.db_manager.save_position(data)
        elif table == "positions_update":
            await self.db_manager.update_position(data)
        elif table == "risk_events":
            await self.db_manager.save_risk_event(data)
        elif table == "strategy_stats":
            await self.db_manager.upsert_strategy_stats(data)
        elif table == "dca_journal_create":
            await self.db_manager.create_dca_journal(data)
        elif table == "dca_journal_update":
            await self.db_manager.update_dca_journal(
                journal_id=data["id"],
                rounds_completed=data["rounds_completed"],
                total_size_filled=data["total_size_filled"],
                status=data["status"]
            )
        elif table == "trade_log":
            await self.db_manager.save_trade_log(data)
        else:
            logger.error(f"Unknown table destination in DatabaseWriter: {table}")
            
    def _check_queue_health(self):
        depth = self._queue.qsize()
        
        current_level = 0
        if depth >= self._emergency_threshold:
            current_level = 3
        elif depth >= self._critical_threshold:
            current_level = 2
        elif depth >= self._warning_threshold:
            current_level = 1
            
        if current_level != self._last_alert_level:
            if current_level > self._last_alert_level:
                # Level upgraded
                msg = f"⚠️ DATABASE WRITE QUEUE ALERT: Depth is {depth}/{self.max_size}. "
                if current_level == 1:
                    msg += "Level: WARNING (depth >= 50%)"
                elif current_level == 2:
                    msg += "Level: CRITICAL (depth >= 80%)"
                elif current_level == 3:
                    msg += "Level: EMERGENCY (depth >= 95%)"
                
                logger.warning(msg)
                if self.telegram_callback:
                    if asyncio.iscoroutinefunction(self.telegram_callback):
                        asyncio.create_task(self.telegram_callback(msg))
                    else:
                        self.telegram_callback(msg)
            else:
                # Level downgraded
                if current_level == 0:
                    msg = f"🟢 DATABASE WRITE QUEUE RECOVERED: Depth is {depth}/{self.max_size}. Status OK."
                else:
                    msg = f"ℹ️ DATABASE WRITE QUEUE DECREASED: Depth is {depth}/{self.max_size}. Status level: "
                    if current_level == 1:
                        msg += "WARNING"
                    elif current_level == 2:
                        msg += "CRITICAL"
                
                logger.info(msg)
                if self.telegram_callback:
                    if asyncio.iscoroutinefunction(self.telegram_callback):
                        asyncio.create_task(self.telegram_callback(msg))
                    else:
                        self.telegram_callback(msg)
                        
            self._last_alert_level = current_level
            
    async def _sampling_loop(self):
        while True:
            try:
                depth = self._queue.qsize()
                self.samples.append(depth)
            except Exception as e:
                logger.error(f"Error in sampling loop: {e}")
            await asyncio.sleep(1)
            
    def get_metrics(self) -> Dict[str, Any]:
        """Calculates queue depth statistics (average, peak, 95th percentile)."""
        depth = self._queue.qsize()
        if not self.samples:
            return {
                "queue_depth": depth,
                "total_queued": self.total_queued,
                "total_written": self.total_written,
                "total_failed": self.total_failed,
                "total_dropped": self.total_dropped,
                "avg_queue_depth": 0.0,
                "peak_queue_depth": 0,
                "p95_queue_depth": 0.0
            }
            
        avg_depth = sum(self.samples) / len(self.samples)
        peak_depth = max(self.samples)
        
        # Sort-based percentile
        sorted_samples = sorted(self.samples)
        idx = int(len(sorted_samples) * 0.95)
        idx = min(idx, len(sorted_samples) - 1)
        p95_depth = sorted_samples[idx] if sorted_samples else 0.0
        
        return {
            "queue_depth": depth,
            "total_queued": self.total_queued,
            "total_written": self.total_written,
            "total_failed": self.total_failed,
            "total_dropped": self.total_dropped,
            "avg_queue_depth": round(avg_depth, 2),
            "peak_queue_depth": peak_depth,
            "p95_queue_depth": p95_depth
        }
        
    async def shutdown(self):
        logger.info("Initiating DatabaseWriter shutdown...")
        self._accepting_writes = False
        
        # Wait for all currently queued tasks to be processed
        if not self._queue.empty():
            logger.info(f"Draining {self._queue.qsize()} remaining database writes...")
            try:
                # We can't block indefinitely in case the connection is completely dead,
                # so wait with a timeout of 10 seconds.
                await asyncio.wait_for(self._queue.join(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for queue to drain. Flushing remaining writes synchronously...")
                
        # Now cancel the worker task
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
                
        # Cancel metrics sampling task
        if self._sampling_task:
            self._sampling_task.cancel()
            try:
                await self._sampling_task
            except asyncio.CancelledError:
                pass
                
        # Flush any remaining items in queue (just in case)
        flush_count = 0
        while not self._queue.empty():
            try:
                table, data = self._queue.get_nowait()
                await self._execute_db_write(table, data)
                self.total_written += 1
                flush_count += 1
            except Exception as e:
                logger.error(f"Error flushing DB write during shutdown: {e}")
                self.total_failed += 1
                
        if flush_count > 0:
            logger.info(f"Flushed {flush_count} writes during shutdown.")
        logger.info("DatabaseWriter shutdown complete.")
