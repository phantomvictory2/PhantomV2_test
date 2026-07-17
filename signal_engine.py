import logging
import asyncio
import time
from typing import Dict, Any

from strategies import (
    classify_last_shadow_trade_lite_v4,
    classify_orbit_a_240,
    classify_phantom_momentum_v1,
)


logger = logging.getLogger(__name__)

class SignalEngine:
    def __init__(self, risk_engine_callback=None, db_callback=None, poly_feed=None):
        self.risk_engine_callback = risk_engine_callback
        self.db_callback = db_callback
        self.poly_feed = poly_feed
        # Last Shadow (self-contained) + signal-driven strategies:
        # ORBIT_A_240 restored; PHANTOM_MOMENTUM_V1 replaces ORBIT_A_260.
        self.strategy_enabled = {
            "LAST_SHADOW_TRADE_LITE_V4": True,
            "ORBIT_A_240": False,   # disabled: -$499 over 825 paper trades (55.6% WR vs 63% breakeven)
            "PHANTOM_MOMENTUM_V1": True,
        }
        self._last_scan_log: Dict[str, float] = {}  # asset → last log time
        self._last_signal_time: Dict[str, float] = {}  # "asset:strategy" → last Grade-A fire time
        self._signal_cooldown = 10.0  # seconds — minimum gap between Grade A signals per asset/strategy

    async def process_raw_signal(self, binance_signal: Dict[str, Any]):
        # Currently inactive as per architecture directive, Orbit operates on Polymarket feeds directly.
        pass

    async def process_poly_signal(self, asset: str, poly_state: Dict[str, Any]):
        signals = []
        if self.strategy_enabled.get("LAST_SHADOW_TRADE_LITE_V4", True):
            signals.append(classify_last_shadow_trade_lite_v4(poly_state))
        if self.strategy_enabled.get("ORBIT_A_240", True):
            signals.append(classify_orbit_a_240(poly_state))
        if self.strategy_enabled.get("PHANTOM_MOMENTUM_V1", True):
            signals.append(classify_phantom_momentum_v1(poly_state))
            
        # Throttled scan diagnostic — logs once per 30s per asset so the system is visibly alive
        now = time.time()
        if now - self._last_scan_log.get(asset, 0) >= 30:
            self._last_scan_log[asset] = now
            ttr = poly_state.get("time_to_resolution_seconds", 0)
            yes = poly_state.get("yes_price", 0)
            no = poly_state.get("no_price", 0)
            prev_yes = poly_state.get("prev_yes_price", yes)
            momentum = round(yes - prev_yes, 4)
            skip_reasons = [s.get("skip_reason", "—") for s in signals if s.get("grade") == "SKIP"]
            logger.info(
                f"[SCAN] {asset} | TTR={ttr:.0f}s | YES={yes:.3f} (Δ{momentum:+.3f}) | NO={no:.3f} | skips={skip_reasons}"
            )

        passing_signals = []
        for sig in signals:
            if sig["grade"] in ["SKIP", "NEAR_MISS_TIME_GATED"]:
                if sig["grade"] != "SKIP" and self.db_callback:
                    if asyncio.iscoroutinefunction(self.db_callback):
                        await self.db_callback("signals", sig)
                    else:
                        self.db_callback("signals", sig)
            else:
                # Cooldown gate — suppress duplicate Grade-A signals for the same asset+strategy
                # within the cooldown window to prevent signal flood from rapid WS ticks.
                cooldown_key = f"{sig['asset']}:{sig['strategy_type']}"
                last_fired = self._last_signal_time.get(cooldown_key, 0)
                if now - last_fired < self._signal_cooldown:
                    continue  # silently drop — not a new trade opportunity
                self._last_signal_time[cooldown_key] = now
                passing_signals.append(sig)
                
        if passing_signals:
            # First one wins
            winning_sig = passing_signals[0]
            logger.info(f"✅ POLYMARKET SIGNAL CLASSIFIED: {winning_sig['strategy_type']} Grade: {winning_sig['grade']}")
            
            # Discard/Supersede the rest if any
            for superseded_sig in passing_signals[1:]:
                superseded_sig["grade"] = "SKIP"
                superseded_sig["outcome"] = "SUPERSEDED"
                superseded_sig["skip_reason"] = f"Superseded by {winning_sig['strategy_type']}"
                if self.db_callback:
                    if asyncio.iscoroutinefunction(self.db_callback):
                        await self.db_callback("signals", superseded_sig)
                    else:
                        self.db_callback("signals", superseded_sig)
                        
            # Pass winning signal to Risk Engine or Phase 0
            if getattr(self, 'phase_0_mode', False):
                if hasattr(self, 'phase_0_callback') and self.phase_0_callback:
                    if asyncio.iscoroutinefunction(self.phase_0_callback):
                        asyncio.create_task(self.phase_0_callback(winning_sig))
                    else:
                        self.phase_0_callback(winning_sig)
            else:
                if self.risk_engine_callback:
                    if asyncio.iscoroutinefunction(self.risk_engine_callback):
                        task = asyncio.create_task(self.risk_engine_callback(winning_sig))
                        def _log_risk_exc(t):
                            try:
                                t.result()
                            except Exception as e:
                                logger.error(f"Exception in risk_engine_callback for {winning_sig.get('asset')}: {e}", exc_info=True)
                        task.add_done_callback(_log_risk_exc)
                    else:
                        self.risk_engine_callback(winning_sig)
