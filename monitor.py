import asyncio
import logging
import time
from typing import Dict, Any, List
from datetime import timezone

logger = logging.getLogger(__name__)


async def _resolve(value):
    """Await value if it is a coroutine/future; return it directly otherwise.
    Eliminates the repeated iscoroutine/isawaitable dual-dispatch pattern."""
    if asyncio.iscoroutine(value) or asyncio.isfuture(value):
        return await value
    return value


class PositionMonitor:
    def __init__(self, state_provider, db_callback=None, telegram_callback=None):
        self.state_provider = state_provider
        self.db_callback = db_callback
        self.telegram_callback = telegram_callback
        self.running = False

    # ── STRATEGY CONFIGS ──────────────────────────────────────────────────────
    # Only active strategies. Archived strategies (BREAKOUT_SCALPER, FLASH_CRASH,
    # ORBIT_A, ORBIT_B, LAST_SHADOW_TRADE_LITE) removed to prevent confusion.
    _STRATEGY_CONFIGS: Dict[str, dict] = {
        "ORBIT_A_240": {
            "take_profit_pct": 0.07,   # target ~7% per winning trade
            # Percentage stop (12% of entry value) instead of a fixed 0.50 price.
            # The fixed stop didn't scale: catastrophic for high entries (0.82->0.37)
            # and it overshot to ~0.40. A %-stop caps each loss proportionally.
            "stop_loss": 0.12,
            "stop_loss_price": None,
            "max_hold_seconds": 90.0,
        },
        "ORBIT_A_260": {
            # MID MOMENTUM (per user spec): 6-8% take-profit, fixed 0.50 stop.
            "take_profit_pct": 0.07,   # target 6-8% per winning trade
            "stop_loss_price": 0.50,   # fixed stop at 0.50
            "stop_loss": None,
            "max_hold_seconds": 90.0,  # DEFAULT (not specified)
        },
        "LAST_SHADOW_TRADE_LITE_V4": {
            "take_profit": None,
            "stop_loss_price": 0.84,
            "max_hold_seconds": 15.0,
        },
    }
    _DEFAULT_CONFIG: dict = {"take_profit": None, "stop_loss": None, "max_hold_seconds": 240.0}

    # ── MAIN LOOP ─────────────────────────────────────────────────────────────

    async def run(self):
        """Standalone run loop (5 s interval for tight exit adherence)."""
        self.running = True
        logger.info("Position Monitor started.")
        try:
            await self.replay_historical_safeguards()
        except Exception as e:
            logger.error(f"Failed to replay historical safeguards on startup: {e}", exc_info=True)
        while self.running:
            try:
                await self.check_positions()
            except Exception as e:
                logger.error(f"Monitor error: {e}")
            await asyncio.sleep(5)  # tight interval so stops fire near 0.50, not overshoot



    async def check_positions(self):
        # Snapshot to avoid mutation during iteration
        open_positions = list(self.state_provider.get_open_positions())

        for pos in open_positions:
            try:
                await self._evaluate_position(pos)
            except Exception as e:
                logger.error(f"Error evaluating position {pos.get('id')}: {e}", exc_info=True)

        # Prune positions closed during this cycle from the in-memory list
        positions_list = self.state_provider.get_open_positions()
        closed = [p for p in positions_list if p.get("status") != "OPEN"]
        for cp in closed:
            try:
                positions_list.remove(cp)
                logger.debug(f"[MONITOR] Pruned CLOSED position for {cp.get('asset')}.")
            except ValueError:
                pass

        if getattr(self.state_provider, "phase_0_mode", False) and getattr(self.state_provider, "db_manager", None):
            await self._check_phase0_logs()

    # ── PHASE 0 ───────────────────────────────────────────────────────────────

    async def _check_phase0_logs(self):
        db = self.state_provider.db_manager
        if not db or not db.pool:
            return

        async with db.pool.acquire() as conn:
            logs = await conn.fetch("SELECT * FROM phase0_edge_log WHERE resolution_price IS NULL")

        for log in logs:
            market_id = log["market_id"]
            direction = log["direction"]

            is_resolved = await _resolve(self.state_provider.is_market_resolved(market_id))
            if not is_resolved:
                continue

            outcome_price = await _resolve(
                self.state_provider.get_resolution_price(market_id, direction)
            )
            raw_pnl = outcome_price - log["entry_price_would_have_been"]
            fee_pct = log.get("fee_pct_at_entry") or 0.0
            fee_adj_pnl = raw_pnl - fee_pct
            won = fee_adj_pnl > 0

            logger.info(
                f"[PHASE 0] Market {market_id} resolved! "
                f"Strategy: {log['strategy_type']} | Won: {won} | Fee-Adj EV: {fee_adj_pnl:.4f}"
            )
            await _resolve(
                db.update_phase0_resolution(log["id"], outcome_price, won, raw_pnl, fee_adj_pnl)
            )

    # ── POSITION EVALUATION ───────────────────────────────────────────────────

    async def _evaluate_position(self, pos: Dict[str, Any]):
        if pos.get("status") != "OPEN":
            return

        asset = pos["asset"]
        poly_state = self.state_provider.get_market_state(asset)

        # Resolution check
        is_resolved = await _resolve(self.state_provider.is_market_resolved(pos["market_id"]))
        if is_resolved:
            outcome_price = await _resolve(
                self.state_provider.get_resolution_price(pos["market_id"], pos["direction"])
            )
            await self._close_position(pos, "RESOLUTION", outcome_price)
            return

        now_ms = int(time.time() * 1000)
        time_held_sec = (now_ms - pos["opened_at"]) / 1000.0

        # LAST_SHADOW_TRADE_LITE_V4 self-resolves at market end via its own driver.
        # The monitor may close it on actual RESOLUTION (handled above) but must NOT
        # apply TIME_EXIT / STOP_LOSS / TAKE_PROFIT — doing so force-closes the trade
        # on a mismatched (already-rolled-over) market at a spurious loss. This guard
        # mainly protects positions orphaned into open_positions across a restart.
        if pos.get("strategy_type") == "LAST_SHADOW_TRADE_LITE_V4":
            # Safety net: if such an orphan can't be resolved for a long time, close it
            # flat as a timeout rather than leaking in the open-positions list forever.
            if time_held_sec >= 600:
                await self._close_position(pos, "RESOLUTION_TIMEOUT", pos["entry_price"])
            return

        if not poly_state:
            return

        strat_config = self._get_strategy_config(pos)

        # ── Time exit (must fire promptly — checked first) ────────────────────
        max_hold = strat_config.get("max_hold_seconds", 240.0)
        if time_held_sec >= max_hold:
            exit_price = self._get_exit_price(poly_state, pos["direction"])
            await self._close_position(pos, "TIME_EXIT", exit_price)
            return

        current_price = self._get_exit_price(poly_state, pos["direction"])
        if current_price is None:
            logger.warning(f"Could not get exit price for {asset}, skipping evaluation")
            return

        entry_price = pos["entry_price"]

        # ── Hard stop-loss price ──────────────────────────────────────────────
        stop_loss_price = strat_config.get("stop_loss_price")
        if stop_loss_price is not None and current_price < stop_loss_price:
            await self._close_position(pos, "STOP_LOSS", current_price)
            return

        # ── Percentage stop-loss ──────────────────────────────────────────────
        stop_loss_pct = strat_config.get("stop_loss")
        if stop_loss_pct is not None and entry_price > 0:
            current_value = (current_price / entry_price) * pos["size_usdc"]
            if current_value <= pos["size_usdc"] * (1 - stop_loss_pct):
                await self._close_position(pos, "STOP_LOSS", current_price)
                return

        # ── Absolute take-profit price ────────────────────────────────────────
        take_profit_price = strat_config.get("take_profit")
        if take_profit_price is not None and current_price >= take_profit_price:
            await self._close_position(pos, "TAKE_PROFIT", current_price)
            return

        # ── Percentage take-profit ────────────────────────────────────────────
        take_profit_pct = strat_config.get("take_profit_pct")
        if take_profit_pct is not None and entry_price > 0:
            current_value = (current_price / entry_price) * pos["size_usdc"]
            if current_value >= pos["size_usdc"] * (1 + take_profit_pct):
                await self._close_position(pos, "TAKE_PROFIT", current_price)
                return

    # ── HELPERS ───────────────────────────────────────────────────────────────

    def _get_exit_price(self, poly_state: dict, direction: str) -> float:
        """Return the price receivable on exit (bid side, not ask side)."""
        if direction == "BUY_YES":
            val = poly_state.get("best_bid") or poly_state.get("yes_price") or 0.50
            return float(val)
        else:
            best_ask = poly_state.get("best_ask") or poly_state.get("yes_price") or 0.50
            return round(1.0 - float(best_ask), 4)

    def _get_strategy_config(self, pos: Dict[str, Any]) -> dict:
        return self._STRATEGY_CONFIGS.get(pos["strategy_type"], self._DEFAULT_CONFIG)

    # ── CLOSE POSITION ────────────────────────────────────────────────────────

    async def _close_position(self, pos: dict, reason: str, exit_price: float):
        entry_price = pos["entry_price"]
        size_usdc = pos["size_usdc"]

        # PnL: shares model — shares = size / entry_price, exit_value = shares * exit_price
        shares = size_usdc / entry_price if entry_price > 0 else 0
        pnl = round(shares * exit_price - size_usdc, 4)

        # Outcome label defined unconditionally — never referenced before assignment
        win_or_loss = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "BREAKEVEN")

        # Snapshot for rollback on DB failure
        orig = {
            "status": pos.get("status"),
            "exit_price": pos.get("exit_price"),
            "pnl": pos.get("pnl"),
            "close_reason": pos.get("close_reason"),
            "closed_at": pos.get("closed_at"),
            "actual_lag_seconds": pos.get("actual_lag_seconds"),
        }

        actual_lag = pos.get("poly_staleness_seconds", 0.0)
        pos.update({
            "status": "CLOSED",
            "exit_price": exit_price,
            "pnl": pnl,
            "close_reason": reason,
            "closed_at": int(time.time() * 1000),
            "actual_lag_seconds": actual_lag,
        })

        db_success = False
        if self.state_provider and hasattr(self.state_provider, "db_manager") and self.state_provider.db_manager:
            try:
                await self.state_provider.db_manager.update_position_transaction(pos)
                db_success = True
            except Exception as db_err:
                logger.error(f"Database transaction failed for position {pos['id']} exit: {db_err}")
                pos.update(orig)  # rollback in-memory state
                raise db_err
        else:
            # Test / no-DB fallback
            if self.db_callback:
                if asyncio.iscoroutinefunction(self.db_callback):
                    await self.db_callback("positions_update", pos)
                else:
                    self.db_callback("positions_update", pos)
            db_success = True

        if db_success:
            self.state_provider.update_consecutive_loss_counter(win_or_loss)
            self.state_provider.update_asset_lag_stats(pos["asset"], actual_lag)
            self.state_provider.update_strategy_stats(pos)
            self.state_provider.record_pnl(pnl)

            prefix = "[PAPER] " if pos.get("is_paper") else ""
            icon = "✅" if pnl > 0 else "❌"
            sign = "+" if pnl > 0 else ""
            exec_speed = pos.get("signal_to_fill_ms", 0)
            msg = (
                f"{icon} {prefix}TRADE {win_or_loss} — "
                f"{pos['strategy_type']} {pos['asset']} {sign}${pnl} "
                f"{reason} ({exec_speed}ms execution)"
            )
            logger.info(msg)
            if self.telegram_callback:
                if asyncio.iscoroutinefunction(self.telegram_callback):
                    asyncio.create_task(self.telegram_callback(msg))
                else:
                    self.telegram_callback(msg)

    async def replay_historical_safeguards(self):
        if not self.state_provider or not getattr(self.state_provider, "db_manager", None):
            logger.info("No database manager available, skipping historical safeguard replay.")
            return

        db = self.state_provider.db_manager
        try:
            trades = await db.load_historical_positions_with_signals()
            logger.info(f"Replaying safeguard logic on {len(trades)} historical trades...")
            for trade in trades:
                pos_id = trade["position_id"]
                strategy = trade["strategy_type"]
                direction = trade["direction"]
                ttr = trade["time_to_resolution_seconds"] or 0
                fired_at = trade["fired_at"]
                
                # Run Gate 3 Replay
                # ORBIT_A_240: 90s - 280s elapsed -> 20 - 210 TTR remaining
                # ORBIT_A_260: 100s - 260s elapsed -> 40 - 200 TTR remaining
                valid_windows = {
                    "ORBIT_A_240": (20, 210),
                    "ORBIT_A_260": (40, 200)
                }
                min_ttr, max_ttr = valid_windows.get(strategy, (20, 210))
                
                gate_3_blocked = False
                if not (min_ttr <= ttr <= max_ttr):
                    gate_3_blocked = True
                elif ttr < 60:
                    gate_3_blocked = True
                else:
                    # Weekend check
                    fired_epoch = fired_at.timestamp()
                    window_start = float(int(fired_epoch / 300) * 300)
                    elapsed = fired_epoch - window_start
                    
                    is_weekend = fired_at.astimezone(timezone.utc).weekday() in [5, 6]
                    if is_weekend:
                        if elapsed < 150.0:
                            gate_3_blocked = True
                    else:
                        if elapsed < 90.0:
                            gate_3_blocked = True
                            
                blocked = gate_3_blocked
                confidence = "partial" if gate_3_blocked else "insufficient_data"
                
                await db.update_position_safeguard(pos_id, blocked, confidence)
                
            logger.info("Historical safeguard replay completed.")
        except Exception as e:
            logger.error(f"Error during historical safeguard replay: {e}", exc_info=True)
