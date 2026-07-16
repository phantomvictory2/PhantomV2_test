import logging
import time
import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, List

logger = logging.getLogger(__name__)

class SignalValidator:
    def __init__(self, state_provider=None):
        self.sp = state_provider
        self.btc_ticks: List[Tuple[float, float]] = []  # (timestamp, price)
        self.yes_ticks: Dict[str, List[Tuple[float, float]]] = {}  # asset -> (timestamp, price)
        self.first_tick_time: float = None
        self.warm_up_complete: bool = False
        self.window_start_time: float = 0.0
        self.log_file = "trade_log.json"
        self._reset_stats()
        self.last_checked_date = datetime.now(timezone.utc).date()

    def _reset_stats(self):
        self.stats = {
            "total_signals_received": 0,
            "blocked_gate_0": 0,
            "blocked_gate_1": 0,
            "blocked_gate_2": 0,
            "blocked_gate_3": 0,
            "blocked_gate_4": 0,
            "blocked_by_confidence_score": 0,
            "passed_to_risk_engine": 0,
            "passing_scores": [],
            "blocked_scores": []
        }

    def _write_daily_summary(self, date_obj):
        os.makedirs("logs", exist_ok=True)
        date_str = date_obj.isoformat()
        file_path = f"logs/validator_daily_{date_str}.json"
        
        passing_scores = self.stats.get("passing_scores", [])
        blocked_scores = self.stats.get("blocked_scores", [])
        
        avg_passing = round(sum(passing_scores) / len(passing_scores), 2) if passing_scores else 0.0
        avg_blocked = round(sum(blocked_scores) / len(blocked_scores), 2) if blocked_scores else 0.0
        
        summary = {
            "date": date_str,
            "total_signals_received": self.stats["total_signals_received"],
            "blocked_gate_0": self.stats["blocked_gate_0"],
            "blocked_gate_1": self.stats["blocked_gate_1"],
            "blocked_gate_2": self.stats["blocked_gate_2"],
            "blocked_gate_3": self.stats["blocked_gate_3"],
            "blocked_gate_4": self.stats["blocked_gate_4"],
            "blocked_by_confidence_score": self.stats["blocked_by_confidence_score"],
            "passed_to_risk_engine": self.stats["passed_to_risk_engine"],
            "avg_confidence_score_passing": avg_passing,
            "avg_confidence_score_blocked": avg_blocked
        }
        
        try:
            with open(file_path, "w") as f:
                json.dump(summary, f, indent=4)
            logger.info(f"Daily validator summary written to {file_path}")
        except Exception as e:
            logger.error(f"Failed to write validator daily summary to {file_path}: {e}")

    def start(self):
        """Starts the History Tracker task."""
        asyncio.create_task(self._tracker_loop())

    async def _tracker_loop(self):
        logger.info("SignalValidator History Tracker started.")
        while True:
            try:
                now = time.time()
                if self.first_tick_time is None:
                    self.first_tick_time = now
                
                # Check warm-up
                if not self.warm_up_complete and (now - self.first_tick_time >= 180):
                    self.warm_up_complete = True
                    logger.info("SignalValidator warm-up complete (180s history collected).")

                # Check for Midnight UTC transition to write summary
                utc_now = datetime.now(timezone.utc)
                current_date = utc_now.date()
                if self.last_checked_date is None:
                    self.last_checked_date = current_date
                elif current_date != self.last_checked_date:
                    try:
                        self._write_daily_summary(self.last_checked_date)
                    except Exception as e:
                        logger.error(f"Failed to write daily summary: {e}", exc_info=True)
                    self._reset_stats()
                    self.last_checked_date = current_date

                # Update window clock (resets every 300s aligned to 5m mark)
                self.window_start_time = float(int(now / 300) * 300)

                # Track BTC
                spot_feed = getattr(self.sp, "spot_feed", None)
                if spot_feed and spot_feed.price_history.get("BTC"):
                    btc_price = spot_feed.price_history["BTC"][-1][1]
                    self.btc_ticks.append((now, btc_price))

                # Track Polymarket YES prices
                poly_feed = getattr(self.sp, "poly_feed", None)
                if poly_feed:
                    for asset in ["BTC", "ETH", "SOL"]:
                        state = poly_feed.get_market_state(asset)
                        if state:
                            yes_price = state.get("yes_price")
                            if yes_price is not None:
                                if asset not in self.yes_ticks:
                                    self.yes_ticks[asset] = []
                                self.yes_ticks[asset].append((now, yes_price))

                # Prune history older than 600s
                cutoff = now - 600
                self.btc_ticks = [t for t in self.btc_ticks if t[0] >= cutoff]
                for asset in self.yes_ticks:
                    self.yes_ticks[asset] = [t for t in self.yes_ticks[asset] if t[0] >= cutoff]

            except Exception as e:
                logger.error(f"Error in SignalValidator tracker loop: {e}", exc_info=True)
            await asyncio.sleep(1.0)

    def _get_price_at(self, ticks: List[Tuple[float, float]], target_time: float) -> float:
        """Finds closest price to target_time in ticks."""
        if not ticks:
            return None
        closest_tick = min(ticks, key=lambda t: abs(t[0] - target_time))
        # Ensure it's reasonably close (within 10 seconds)
        if abs(closest_tick[0] - target_time) <= 10.0:
            return closest_tick[1]
        return None

    def _build_candles(self, ticks: List[Tuple[float, float]], duration=60) -> List[Dict[str, float]]:
        """Groups ticks into duration-second candles."""
        if not ticks:
            return []
        buckets = {}
        for ts, price in ticks:
            key = int(ts / duration) * duration
            if key not in buckets:
                buckets[key] = []
            buckets[key].append(price)

        candles = []
        for key in sorted(buckets.keys()):
            prices = buckets[key]
            candles.append({
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "start_time": key
            })
        return candles

    def validate_signal(self, signal: Dict[str, Any]) -> Tuple[bool, int, str]:
        """
        Validates signal against 4 gates and outputs score & validation result.
        Returns (passed, confidence_score, block_reason).
        """
        self.stats["total_signals_received"] += 1
        now = time.time()
        
        # Warm-up guard
        if not self.warm_up_complete:
            self.stats["blocked_gate_0"] += 1
            self.stats["blocked_scores"].append(0)
            self._log_rejection(signal, "Gate 0 - Warmup", "validator_warming_up", 0)
            return False, 0, "validator_warming_up"

        strategy = signal.get("strategy_type", "UNKNOWN")
        asset = signal.get("asset")
        direction = signal.get("direction")
        ttr = signal.get("time_to_resolution_seconds", 0)
        
        poly_feed = getattr(self.sp, "poly_feed", None)
        poly_state = poly_feed.get_market_state(asset) if poly_feed else None
        
        yes_price = signal.get("yes_price")
        if yes_price is None and poly_state:
            yes_price = poly_state.get("yes_price")
        
        if yes_price is None:
            self.stats["blocked_gate_0"] += 1
            self.stats["blocked_scores"].append(0)
            self._log_rejection(signal, "Gate 0 - Data", "missing_price_data", 0)
            return False, 0, "missing_price_data"

        # Derived Traded Price: YES price if BUY_YES, 1.0 - YES price if BUY_NO
        traded_price = yes_price if direction == "BUY_YES" else round(1.0 - yes_price, 4)

        # ----------------- GATE 1: MOMENTUM -----------------
        # elapsed in current 300s window
        window_start = float(int(now / 300) * 300)
        elapsed = now - window_start

        # BTC move check for early entry exception
        early_entry = False
        if elapsed <= 120.0:
            btc_start = self._get_price_at(self.btc_ticks, window_start)
            if self.btc_ticks and btc_start is not None:
                current_btc = self.btc_ticks[-1][1]
                if abs(current_btc - btc_start) > 100.0:
                    early_entry = True

        # Group ticks to 1-minute candles
        yes_ticks_list = self.yes_ticks.get(asset, [])
        candles = self._build_candles(yes_ticks_list, 60)
        
        # Exclude active incomplete candle
        current_minute_start = int(now / 60) * 60
        completed_candles = [c for c in candles if c["start_time"] < current_minute_start]

        passed_gate_1 = False
        gate_1_score = 0
        momentum_reason = ""

        if early_entry:
            passed_gate_1 = True
            gate_1_score = 25
        else:
            if len(completed_candles) >= 3:
                last_3 = completed_candles[-3:]
                # Direction of yes_price changes
                up_yes_count = sum(1 for c in last_3 if c["close"] > c["open"])
                down_yes_count = sum(1 for c in last_3 if c["close"] < c["open"])
                
                # Check consecutive direction
                if direction == "BUY_YES" and up_yes_count == 3:
                    passed_gate_1 = True
                elif direction == "BUY_NO" and down_yes_count == 3:
                    passed_gate_1 = True

                if passed_gate_1:
                    # Weakening check: body of last completed candle smaller than second-to-last completed
                    size_last = abs(last_3[-1]["close"] - last_3[-1]["open"])
                    size_prev = abs(last_3[-2]["close"] - last_3[-2]["open"])
                    if size_last < size_prev:
                        passed_gate_1 = False
                        momentum_reason = "momentum_weakening"
                    else:
                        # Determine score based on total consecutive same direction candles
                        total_consec = 3
                        # Scan further back
                        idx = len(completed_candles) - 4
                        while idx >= 0:
                            c = completed_candles[idx]
                            is_same = (direction == "BUY_YES" and c["close"] > c["open"]) or \
                                      (direction == "BUY_NO" and c["close"] < c["open"])
                            if is_same:
                                total_consec += 1
                                idx -= 1
                            else:
                                break
                        if total_consec >= 5:
                            gate_1_score = 25
                        elif total_consec == 4:
                            gate_1_score = 20
                        else:
                            gate_1_score = 15
                else:
                    momentum_reason = "no_momentum_in_direction"
            else:
                momentum_reason = "insufficient_history"

        if not passed_gate_1:
            self.stats["blocked_gate_1"] += 1
            self.stats["blocked_scores"].append(0)
            self._log_rejection(signal, "Gate 1 - Momentum", momentum_reason, 0)
            return False, 0, momentum_reason

        # ----------------- GATE 2: REVERSAL DETECTION -----------------
        passed_gate_2 = True
        gate_2_score = 25
        reversal_reason = ""

        # Price 30 seconds ago
        yes_30s_ago = self._get_price_at(yes_ticks_list, now - 30.0)
        if yes_30s_ago is not None:
            # Probability shift against trade direction
            if direction == "BUY_YES":
                prob_move_against = yes_30s_ago - yes_price
            else:
                prob_move_against = yes_price - yes_30s_ago

            if prob_move_against > 0.15:
                passed_gate_2 = False
                reversal_reason = "reversal_exceeded_15pct"
            else:
                # Score reversal risk
                if prob_move_against <= 0.01:
                    gate_2_score = 25
                elif prob_move_against <= 0.05:
                    gate_2_score = 20
                elif prob_move_against <= 0.10:
                    gate_2_score = 15
                elif prob_move_against <= 0.15:
                    gate_2_score = 10
                else:
                    gate_2_score = 0
        else:
            gate_2_score = 25  # default if no history

        # Exhaustion check (stalled after fast move)
        price_120s_ago = self._get_price_at(yes_ticks_list, now - 120.0)
        if price_120s_ago is not None and passed_gate_2:
            traded_120s_ago = price_120s_ago if direction == "BUY_YES" else round(1.0 - price_120s_ago, 4)
            fast_move = abs(traded_price - traded_120s_ago) >= 0.08
            
            # Stalled: range of YES price in last 20s is <= 0.01
            ticks_20s = [t[1] for t in yes_ticks_list if t[0] >= now - 20.0]
            if len(ticks_20s) >= 2:
                stalled = (max(ticks_20s) - min(ticks_20s)) <= 0.01
            else:
                stalled = False

            if fast_move and stalled:
                passed_gate_2 = False
                reversal_reason = "price_stalled_exhaustion"

        if not passed_gate_2:
            self.stats["blocked_gate_2"] += 1
            self.stats["blocked_scores"].append(0)
            self._log_rejection(signal, "Gate 2 - Reversal", reversal_reason, 0)
            return False, 0, reversal_reason

        # ----------------- GATE 3: ENTRY QUALITY -----------------
        passed_gate_3 = True
        gate_3_score = 25
        quality_reason = ""

        # Strategy valid TTR bounds
        valid_windows = {
            "ORBIT_A_240": (90, 240),
            "ORBIT_A_260": (90, 260),
            "PHANTOM_MOMENTUM_V1": (90, 260)
        }
        min_ttr, max_ttr = valid_windows.get(strategy, (90, 260))

        if ttr < 60:
            # Checked first: not enough time remaining to hit TP regardless of strategy window
            passed_gate_3 = False
            quality_reason = "insufficient_ttr_for_tp"
        elif not (min_ttr <= ttr <= max_ttr):
            passed_gate_3 = False
            quality_reason = "ttr_outside_window"
        else:
            # Score TTR quality
            if ttr >= 180 and ttr <= 240:
                gate_3_score = 25
            elif ttr >= 120 and ttr < 180:
                gate_3_score = 20
            elif ttr >= 90 and ttr < 120:
                gate_3_score = 15
            else:
                gate_3_score = 10

            # Weekend / Weekday elapsed time rule
            utc_now = datetime.now(timezone.utc)
            is_weekend = utc_now.weekday() in [5, 6]  # 5=Saturday, 6=Sunday
            
            if is_weekend:
                if elapsed < 150.0:
                    passed_gate_3 = False
                    quality_reason = "weekend_pre_150s_block"
            else:
                if elapsed < 90.0:
                    passed_gate_3 = False
                    quality_reason = "weekday_pre_90s_block"

        if not passed_gate_3:
            self.stats["blocked_gate_3"] += 1
            self.stats["blocked_scores"].append(0)
            self._log_rejection(signal, "Gate 3 - Quality", quality_reason, 0)
            return False, 0, quality_reason

        # ----------------- GATE 4: LIQUIDITY -----------------
        passed_gate_4 = True
        gate_4_score = 25
        liquidity_reason = ""

        bids = poly_state.get("bids", []) if poly_state else []
        asks = poly_state.get("asks", []) if poly_state else []

        # Depth on the side we need: asks for BUY_YES (we're buying), bids for BUY_NO (we sell YES)
        depth_side = asks if direction == "BUY_YES" else bids
        if depth_side:
            depth_yes = sum(float(b.get("price", 0)) * float(b.get("size", 0)) for b in depth_side)
        else:
            depth_yes = signal.get("liquidity_usdc", 1000.0)

        # Slippage check
        slippage_pct = 0.0
        if direction == "BUY_YES" and asks:
            remaining = 100.0
            cost = 0.0
            shares = 0.0
            for ask in asks:
                ask_p = float(ask.get("price", 0))
                ask_s = float(ask.get("size", 0))
                avail = ask_p * ask_s
                if remaining <= avail:
                    shares += remaining / ask_p
                    cost += remaining
                    remaining = 0.0
                    break
                else:
                    shares += ask_s
                    cost += avail
                    remaining -= avail
            if remaining > 0.0:
                slippage_pct = 1.0
            else:
                avg_price = cost / shares
                slippage_pct = (avg_price - traded_price) / traded_price if traded_price > 0 else 1.0
        elif direction == "BUY_NO" and bids:
            remaining = 100.0
            cost = 0.0
            shares = 0.0
            for bid in bids:
                bid_p = float(bid.get("price", 0))
                bid_s = float(bid.get("size", 0))
                avail = bid_p * bid_s
                if remaining <= avail:
                    shares += remaining / bid_p
                    cost += remaining
                    remaining = 0.0
                    break
                else:
                    shares += bid_s
                    cost += avail
                    remaining -= avail
            if remaining > 0.0:
                slippage_pct = 1.0
            else:
                avg_price = cost / shares
                avg_no_price = 1.0 - avg_price
                slippage_pct = (avg_no_price - traded_price) / traded_price if traded_price > 0 else 1.0
        else:
            spread = signal.get("spread", 0.01)
            slippage_pct = spread / traded_price if traded_price > 0 else 0.01

        if depth_yes < 500.0:
            passed_gate_4 = False
            liquidity_reason = "insufficient_depth"
        elif slippage_pct > 0.10:
            passed_gate_4 = False
            liquidity_reason = "excessive_slippage"
        else:
            if depth_yes >= 2000.0:
                gate_4_score = 25
            elif depth_yes >= 1000.0:
                gate_4_score = 20
            elif depth_yes >= 500.0:
                gate_4_score = 15
            else:
                gate_4_score = 0

        if not passed_gate_4:
            self.stats["blocked_gate_4"] += 1
            self.stats["blocked_scores"].append(0)
            self._log_rejection(signal, "Gate 4 - Liquidity", liquidity_reason, 0)
            return False, 0, liquidity_reason

        # ----------------- CONFIDENCE SCORE -----------------
        total_score = gate_1_score + gate_2_score + gate_3_score + gate_4_score

        utc_now = datetime.now(timezone.utc)
        time_window_flag = (8 <= utc_now.hour < 10) or (22 <= utc_now.hour < 24)
        
        if time_window_flag:
            total_score += 5
            
        total_score = min(100, total_score)
        signal["time_window_flag"] = time_window_flag

        if total_score < 70:
            self.stats["blocked_by_confidence_score"] += 1
            self.stats["blocked_scores"].append(total_score)
            self._log_rejection(signal, "Gate 5 - Score", "low_confidence_score", total_score)
            return False, total_score, "low_confidence_score"

        self.stats["passed_to_risk_engine"] += 1
        self.stats["passing_scores"].append(total_score)
        return True, total_score, None

    def _log_rejection(self, signal: Dict[str, Any], gate: str, reason: str, score: int):
        rejection_data = {
            "signal_id": signal.get("signal_id", str(time.time())),
            "asset": signal.get("asset"),
            "strategy_type": signal.get("strategy_type"),
            "direction": signal.get("direction"),
            "gate_blocked_at": gate,
            "block_reason": reason,
            "confidence_score": score,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            logs = []
            if os.path.exists(self.log_file):
                with open(self.log_file, "r") as f:
                    try:
                        logs = json.load(f)
                    except json.JSONDecodeError:
                        logs = []
            logs.append(rejection_data)
            with open(self.log_file, "w") as f:
                json.dump(logs, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to log rejection to file: {e}")

        db_writer = getattr(self.sp, "db_writer", None)
        if db_writer:
            asyncio.create_task(