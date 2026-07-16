import asyncio
import logging
import time
import uuid
from typing import Dict, Any

logger = logging.getLogger(__name__)

class Executor:
    def __init__(self, risk_engine, db_callback=None, telegram_callback=None, state_provider=None):
        self.risk_engine = risk_engine
        self.db_callback = db_callback
        self.telegram_callback = telegram_callback
        self.state_provider = state_provider
        self.paper_mode = True 
        self.clob_client = None

    async def initialize(self):
        # Determine mode
        self.paper_mode = str(self.risk_engine.get_config("paper_mode", "true")).lower() == "true"
        if not self.paper_mode:
            logger.info("Initializing live Polymarket CLOB client...")
            # self.clob_client = ClobClient(...) -> pre-authenticated
        else:
            logger.info("Initializing Paper Executor...")

    async def process_approved_signal(self, payload: Dict[str, Any], db_task=None):
        """
        Receives payload from Risk Engine. Must have status 'APPROVED' and a 'signal'.
        """
        if payload.get("status") != "APPROVED" or "signal" not in payload:
            logger.warning("Executor ignored non-approved signal")
            return
            
        signal = payload["signal"]
        
        # Double check paper mode config right before execution
        self.paper_mode = str(self.risk_engine.get_config("paper_mode", "true")).lower() == "true"
        
        def _on_task_done(task, label):
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.error(f"[EXECUTOR] {label} task raised unhandled exception: {exc}", exc_info=True)

        if signal["entry_mode"] == "SINGLE":
            t = asyncio.create_task(self._execute_single(signal, db_task))
            t.add_done_callback(lambda task: _on_task_done(task, f"SINGLE {signal.get('asset')}"))
        elif signal["entry_mode"] == "DCA":
            t = asyncio.create_task(self._execute_dca(signal, db_task))
            t.add_done_callback(lambda task: _on_task_done(task, f"DCA {signal.get('asset')}"))

    async def _execute_single(self, signal: dict, db_task=None):
        asset = signal["asset"]
        strat = signal.get("strategy_type", "UNKNOWN")
        try:
            direction = signal["direction"]
            size = signal["approved_size_usdc"]
            
            # Respect trade direction for price
            exec_price = signal["yes_price"] if direction == "BUY_YES" else signal["no_price"]

            if exec_price is None or exec_price <= 0.0 or exec_price >= 1.0:
                logger.error("EXECUTOR_PRICE_GUARD_BLOCKED")
                return

            start_time = time.time()
            filled = False

            if self.paper_mode:
                await asyncio.sleep(0.05) # Simulated latency
                filled = True
            else:
                logger.info(f"LIVE: Placing limit order {direction} on {asset} at {exec_price}")
                # Pseudo-code for live:
                # order_id = self.clob_client.create_order(market_id, side, price, size)
                # while time.time() - start_time < 5.0:
                #     if self.clob_client.is_filled(order_id):
                #         filled = True
                #         break
                #     await asyncio.sleep(0.5)
                pass

            if not filled and not self.paper_mode:
                logger.warning(f"MISSED_FILL: Order not filled within 5s for {asset}")
                # self.clob_client.cancel_order(order_id)
                return
                
            signal_to_fill_ms = int((time.time() - start_time) * 1000)
            await self._log_fill(signal, exec_price, size, "SINGLE", 1, signal_to_fill_ms, db_task)
        except Exception as e:
            msg = f"🔴 EXECUTION FAILED — {strat} {asset} Single Order failed: {e}"
            logger.error(msg, exc_info=True)
            if self.telegram_callback:
                if asyncio.iscoroutinefunction(self.telegram_callback):
                    asyncio.create_task(self.telegram_callback(msg))
                else:
                    self.telegram_callback(msg)
            raise e
        finally:
            if self.state_provider:
                self.state_provider.pending_assets.discard(asset)

    async def resume_dca(self, signal: dict, journal: dict):
        asset = signal["asset"]
        strat = signal.get("strategy_type", "UNKNOWN")
        
        # Prevent duplicate executions during recovery
        if self.state_provider:
            has_open_pos = any(p.get("asset") == asset and p.get("status") == "OPEN" for p in self.state_provider.open_positions)
            is_pending = asset in self.state_provider.pending_assets
            if has_open_pos or is_pending:
                logger.warning(f"[RECOVERY] Skipped resume_dca for {asset}: position already active or pending.")
                return
                
            # Reserve asset in pending_assets before recovery begins
            self.state_provider.pending_assets.add(asset)
            
        # Reconstruct dca_config from journal
        signal["dca_config"] = {
            "rounds": journal["rounds_total"],
            "per_round_usdc": journal["per_round_usdc"],
            "limit_price": journal["limit_price"],
            "interval_seconds": journal["interval_seconds"]
        }
            
        logger.info(f"[RECOVERY] Resuming DCA loop for {asset} from journal {journal['id']} (completed {journal['rounds_completed']}/{journal['rounds_total']} rounds)")
        asyncio.create_task(self._execute_dca(signal, db_task=None, resume_journal=journal))

    async def _execute_dca(self, signal: dict, db_task=None, resume_journal=None):
        asset = signal["asset"]
        strat = signal.get("strategy_type", "UNKNOWN")
        
        journal_id = None
        rounds_completed = 0
        total_size_filled = 0.0
        
        try:
            config = signal["dca_config"]
            rounds = config["rounds"]
            per_round = config.get("per_round_usdc", config.get("amount_usdc", 10.0) / rounds)
            limit_price = config["limit_price"]
            interval = config["interval_seconds"]
            
            direction = signal["direction"]
            # BUG FIX: Track weighted average fill price across DCA rounds
            # instead of using a single stale price from signal time
            total_price_x_size = 0.0  # sum of (fill_price * round_size) for VWAP

            if resume_journal:
                journal_id = resume_journal["id"]
                rounds_completed = resume_journal["rounds_completed"]
                total_size_filled = resume_journal["total_size_filled"]
                # For resumed DCAs, approximate prior VWAP from signal price
                initial_price = signal["yes_price"] if direction == "BUY_YES" else signal["no_price"]
                total_price_x_size = initial_price * total_size_filled
                logger.info(f"[RECOVERY] Resuming DCA loop {journal_id} for {asset} from round {rounds_completed+1}")
            else:
                journal_id = str(uuid.uuid4())
                rounds_completed = 0
                total_size_filled = 0.0
                
                # Create persistent journal entry
                if self.db_callback:
                    journal_data = {
                        "id": journal_id,
                        "signal_id": signal["signal_id"],
                        "asset": asset,
                        "direction": signal["direction"],
                        "rounds_total": rounds,
                        "rounds_completed": 0,
                        "per_round_usdc": per_round,
                        "limit_price": limit_price,
                        "interval_seconds": interval,
                        "total_size_filled": 0.0,
                        "status": "ACTIVE"
                    }
                    if asyncio.iscoroutinefunction(self.db_callback):
                        await self.db_callback("dca_journal_create", journal_data)
                    else:
                        self.db_callback("dca_journal_create", journal_data)

            for r in range(rounds_completed, rounds):
                # Check 1: Regime stopped
                if self.risk_engine.regime_stopped:
                    logger.warning(f"DCA STOPPED ({rounds_completed}/{rounds}): Regime stopped")
                    break
                    
                # Check 2: Kill switch active
                if str(self.risk_engine.get_config("kill_switch", "false")).lower() == "true":
                    logger.warning(f"DCA STOPPED ({rounds_completed}/{rounds}): Kill switch active")
                    break
                    
                # Check 3: Adverse price > 10%
                direction = signal["direction"]
                current_price = signal["yes_price"] if direction == "BUY_YES" else signal["no_price"]
                if self.state_provider:
                    poly_state = self.state_provider.get_market_state(signal["asset"])
                    if poly_state:
                        current_price = poly_state["yes_price"] if direction == "BUY_YES" else poly_state["no_price"]
                
                # Stop DCA if price moved 10% AGAINST our position (adverse move)
                if current_price < limit_price * 0.90:
                    logger.warning(f"DCA_STOPPED_ADVERSE_PRICE ({rounds_completed}/{rounds}): Price dropped to {current_price} (entry limit {limit_price})")
                    break

                # BUG FIX: Use live price for this round's fill (paper mode)
                round_fill_price = current_price if self.paper_mode else limit_price
                
                # Price guard per round
                if round_fill_price is None or round_fill_price <= 0.0 or round_fill_price >= 1.0:
                    logger.warning(f"DCA round {r+1} price guard: {round_fill_price}, skipping round")
                    continue

                # Execute round
                if self.paper_mode:
                    total_size_filled += per_round
                    total_price_x_size += round_fill_price * per_round
                    rounds_completed += 1
                else:
                    logger.info(f"LIVE: Placing DCA limit order {r+1}/{rounds} for {signal['asset']}")
                    total_size_filled += per_round
                    total_price_x_size += round_fill_price * per_round
                    rounds_completed += 1

                # Update journal after each round completes
                if self.db_callback and journal_id:
                    update_data = {
                        "id": journal_id,
                        "rounds_completed": rounds_completed,
                        "total_size_filled": total_size_filled,
                        "status": "ACTIVE"
                    }
                    if asyncio.iscoroutinefunction(self.db_callback):
                        await self.db_callback("dca_journal_update", update_data)
                    else:
                        self.db_callback("dca_journal_update", update_data)

                if r < rounds - 1:
                    await asyncio.sleep(interval)
                    
            final_status = "COMPLETED" if rounds_completed == rounds else "STOPPED"
            if self.db_callback and journal_id:
                update_data = {
                    "id": journal_id,
                    "rounds_completed": rounds_completed,
                    "total_size_filled": total_size_filled,
                    "status": final_status
                }
                if asyncio.iscoroutinefunction(self.db_callback):
                    await self.db_callback("dca_journal_update", update_data)
                else:
                    self.db_callback("dca_journal_update", update_data)

            if rounds_completed > 0:
                # BUG FIX: Use volume-weighted average price instead of stale signal price
                vwap = total_price_x_size / total_size_filled if total_size_filled > 0 else limit_price
                await self._log_fill(signal, vwap, total_size_filled, "DCA", rounds_completed, 0, db_task)

        except Exception as e:
            msg = f"🔴 EXECUTION FAILED — {strat} {asset} DCA Order failed: {e}"
            logger.error(msg, exc_info=True)
            if self.db_callback and journal_id:
                update_data = {
                    "id": journal_id,
                    "rounds_completed": rounds_completed,
                    "total_size_filled": total_size_filled,
                    "status": "FAILED"
                }
                if asyncio.iscoroutinefunction(self.db_callback):
                    await self.db_callback("dca_journal_update", update_data)
                else:
                    self.db_callback("dca_journal_update", update_data)
            if self.telegram_callback:
                if asyncio.iscoroutinefunction(self.telegram_callback):
                    asyncio.create_task(self.telegram_callback(msg))
                else:
                    self.telegram_callback(msg)
            raise e
        finally:
            if self.state_provider:
                self.state_provider.pending_assets.discard(asset)

    async def _log_fill(self, signal: dict, fill_price: float, size_usdc: float, mode: str, rounds_completed: int, signal_to_fill_ms: int, db_task=None):
        # Await the signal db logging task first (to satisfy PostgreSQL foreign key constraint)
        if db_task:
            try:
                await db_task
            except Exception as e:
                logger.error(f"Error awaiting signal DB insert task: {e}")

        pos_id = str(uuid.uuid4())
        position = {
            "id": pos_id,
            "signal_id": signal["signal_id"],
            "strategy_type": signal["strategy_type"],
            "market_id": signal["market_id"],
            "asset": signal["asset"],
            "direction": signal["direction"],
            "entry_price": fill_price,
            "size_usdc": size_usdc,
            "entry_mode": mode,
            "dca_rounds_completed": rounds_completed,
            "status": "OPEN",
            "is_paper": self.paper_mode,
            "signal_to_fill_ms": signal_to_fill_ms,
            "opened_at": int(time.time() * 1000),
            "poly_staleness_seconds": signal.get("staleness_seconds", 0.0),
        }
        
        prefix = "[PAPER] " if self.paper_mode else ""
        dca_str = f"({rounds_completed} rounds)" if mode == "DCA" else ""
        msg = f"⚡ {prefix}TRADE ENTRY — {signal['strategy_type']} {signal['asset']} {signal['direction']} ${round(size_usdc,2)} @ {fill_price} {dca_str}"
        
        logger.info(msg)
        
        # Append to in-memory state BEFORE the DB await so no concurrent signal
        # can slip through the gap between pending_assets.discard and open_positions.append.
        if self.state_provider:
            self.state_provider.open_positions.append(position)

        if self.db_callback:
            if asyncio.iscoroutinefunction(self.db_callback):
                await self.db_callback("positions", position)
            else:
                self.db_callback("positions", position)
                
        if self.telegram_callback:
            if asyncio.iscoroutinefunction(self.telegram_callback):
                asyncio.create_task(self.telegram_callback(msg))
            else:
                self.telegram_callback(msg)
