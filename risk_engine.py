import logging
import time
import asyncio
from typing import Dict, Any

logger = logging.getLogger(__name__)


class RiskEngine:
    # TTR windows exposed as a class constant so startup validation can assert
    # that strategy classifier files are in sync with these values.
    VALID_TTR_WINDOWS: Dict[str, tuple] = {
        "LATENCY_ARB":              (5,  295),
        "ORBIT_A_240":              (20, 210),  # TTR remaining (90s - 280s elapsed)
        "ORBIT_A_260":              (40, 200),  # retired — replaced by PHANTOM_MOMENTUM_V1
        "PHANTOM_MOMENTUM_V1":      (40, 200),  # TTR remaining (100s - 260s elapsed)
        "LAST_SHADOW_TRADE_LITE":   (5,  295),
        "LAST_SHADOW_TRADE_LITE_V4":(5,   15),
    }

    def __init__(self, db_callback=None, state_provider=None):
        self.db_callback = db_callback
        self.state_provider = state_provider
        self.regime_stopped = False
        self.paused_until = 0

    def get_config(self, key, default):
        if self.state_provider:
            return self.state_provider.get_config(key, default)
        return default

    def _log_risk_event(self, event_type: str, signal: dict, trigger: str, action: str):
        logger.warning(f"⛔ RISK EVENT: {event_type} | {signal.get('asset', 'ALL')} | {trigger} -> {action}")
        event = {
            "event_type": event_type,
            "signal_id": signal.get("signal_id"),
            "asset": signal.get("asset"),
            "strategy_type": signal.get("strategy_type"),
            "trigger_value": trigger,
            "action_taken": action,
            "timestamp": int(time.time() * 1000),
        }
        if self.db_callback:
            if asyncio.iscoroutinefunction(self.db_callback):
                asyncio.create_task(self.db_callback("risk_events", event))
            else:
                self.db_callback("risk_events", event)

    async def process_signal(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Runs 7 risk checks in sequence. Returns {'status': 'APPROVED'|'REJECTED', ...}.
        approved_size_usdc is always set on the signal (0.0 for rejections).
        """
        sp = self.state_provider

        # Pre-populate all audit fields so analytics never hits a KeyError on rejections
        signal["rejection_reason"] = None
        signal["ttr_at_rejection"] = None
        signal["oracle_lag_at_rejection"] = None
        signal["signal_velocity_at_rejection"] = None
        signal["spread_at_rejection"] = None
        signal["approved_size_usdc"] = 0.0  # overwritten on APPROVED path

        # ── CHECK 1: System state (kill switch / pause / regime) ──────────────
        if self.get_config("kill_switch", False) is True:
            self._log_risk_event("KILL_SWITCH", signal, "Kill switch active", "REJECT")
            signal["rejection_reason"] = "KILL_SWITCH"
            return {"status": "REJECTED", "reason": "Kill switch active"}

        if time.time() < self.paused_until:
            signal["rejection_reason"] = "ENGINE_PAUSED"
            return {"status": "REJECTED", "reason": f"Signal engine paused for {int(self.paused_until - time.time())}s"}

        if self.regime_stopped:
            signal["rejection_reason"] = "REGIME_STOPPED"
            return {"status": "REJECTED", "reason": "Market regime invalid — trading stopped today"}

        # ── CHECK 2: Live feed validation ─────────────────────────────────────
        poly_feed = getattr(sp, "poly_feed", None) if sp else None
        poly_state = poly_feed.get_market_state(signal["asset"]) if poly_feed else None
        if poly_state is None or not poly_state.get("has_live_data", False):
            signal["rejection_reason"] = "NO_LIVE_FEED"
            return {"status": "REJECTED", "reason": "NO_LIVE_FEED"}

        # ── CHECK 3: Market validity (TTR + price bounds + spread + liquidity) ─
        strat = signal.get("strategy_type")
        ttr = signal["time_to_resolution_seconds"]

        if ttr < 2:
            signal["rejection_reason"] = "CHECK_3_TTR"
            signal["ttr_at_rejection"] = ttr
            return {"status": "REJECTED", "reason": "TTR < 2s (execution latency floor breached)"}

        # Global price protection — LAST_SHADOW_V4 is exempt (trades 0.94-0.99)
        is_last_shadow_v4 = (strat == "LAST_SHADOW_TRADE_LITE_V4")

        if signal["direction"] == "BUY_YES" and signal["yes_price"] > 0.85 and not is_last_shadow_v4:
            signal["rejection_reason"] = "CHECK_3_PRICE"
            return {"status": "REJECTED", "reason": f"YES price {signal['yes_price']} > MAX_YES_ENTRY (0.85)"}

        if signal["direction"] == "BUY_NO" and signal["no_price"] < 0.15 and not is_last_shadow_v4:
            signal["rejection_reason"] = "CHECK_3_PRICE"
            return {"status": "REJECTED", "reason": f"NO price {signal['no_price']} < MIN_NO_ENTRY (0.15)"}

        if is_last_shadow_v4:
            entry_price = signal["yes_price"] if signal["direction"] == "BUY_YES" else signal["no_price"]
            if not (0.94 <= entry_price <= 1.0):
                signal["rejection_reason"] = "CHECK_3_PRICE"
                return {"status": "REJECTED", "reason": f"LAST_SHADOW_V4 entry {entry_price} outside (0.94-1.0)"}

        if strat in self.VALID_TTR_WINDOWS:
            min_s, max_s = self.VALID_TTR_WINDOWS[strat]
            if not (min_s <= ttr <= max_s):
                signal["rejection_reason"] = "CHECK_3_TTR"
                signal["ttr_at_rejection"] = ttr
                return {"status": "REJECTED", "reason": f"TTR {ttr}s outside strategy window {min_s}-{max_s}s"}

        if signal["spread"] > 0.05:
            signal["rejection_reason"] = "CHECK_3_SPREAD"
            signal["spread_at_rejection"] = signal["spread"]
            return {"status": "REJECTED", "reason": "Spread too wide (>5%)"}

        if signal["liquidity_usdc"] < 500:
            signal["rejection_reason"] = "CHECK_3_LIQUIDITY"
            return {"status": "REJECTED", "reason": "Insufficient liquidity (<$500)"}

        # ── CHECK 4: Oracle lag (ORBIT strategies only) ───────────────────────
        if strat in ["LATENCY_ARB", "ORBIT_A_240", "ORBIT_A_260", "PHANTOM_MOMENTUM_V1"]:
            lag_stats = (
                sp.get_asset_lag_stats(signal["asset"])
                if sp
                else {"sample_size": 0, "avg_lag_seconds": 0.0, "status": "ACTIVE"}
            )
            avg_lag_val = lag_stats["avg_lag_seconds"] or 0.0
            if lag_stats["status"] == "DISABLED" or (
                lag_stats["sample_size"] >= 10 and avg_lag_val > 10.0
            ):
                if sp:
                    try:
                        sp.disable_asset(signal["asset"])
                    except Exception as e:
                        logger.error(f"Error disabling asset {signal['asset']}: {e}", exc_info=True)
                        tg_cb = getattr(sp, "telegram_callback", None)
                        if tg_cb:
                            if asyncio.iscoroutinefunction(tg_cb):
                                asyncio.create_task(tg_cb(f"⚠️ Error disabling asset {signal['asset']}: {e}"))
                            else:
                                tg_cb(f"⚠️ Error disabling asset {signal['asset']}: {e}")
                avg_lag = lag_stats["avg_lag_seconds"] or 0.0
                self._log_risk_event(
                    "ORACLE_DISABLED", signal,
                    f"avg lag {avg_lag:.1f}s", "Asset disabled"
                )
                signal["rejection_reason"] = "CHECK_4_ORACLE"
                signal["oracle_lag_at_rejection"] = avg_lag
                return {
                    "status": "REJECTED",
                    "reason": f"{signal['asset']} oracle lag {avg_lag:.1f}s > 10s — feed degraded",
                }

        # ── CHECK 5: Position limits & exclusivity ────────────────────────────
        # pending_assets is NOT re-checked here — main.py screens for it before
        # adding to pending_assets, so by the time we reach the risk engine the
        # asset slot is already reserved. Re-checking would always self-reject.
        if sp:
            has_open_pos = any(
                p.get("asset") == signal["asset"] and p.get("status") == "OPEN"
                for p in sp.open_positions
            )
            if has_open_pos:
                signal["rejection_reason"] = "CHECK_5_EXCLUSIVITY"
                return {"status": "REJECTED", "reason": f"Asset {signal['asset']} already has an open position"}

        pos_stats = sp.get_position_stats(signal["asset"]) if sp else {"total": 0}
        open_markets = sp.get_open_markets() if sp and hasattr(sp, "get_open_markets") else set()

        if signal["market_id"] in open_markets:
            signal["rejection_reason"] = "CHECK_5_EXCLUSIVITY"
            return {"status": "REJECTED", "reason": f"Already an open position for market {signal['market_id']}"}

        if pos_stats["total"] >= 2:
            signal["rejection_reason"] = "CHECK_5_POS_LIMIT"
            return {"status": "REJECTED", "reason": "Max concurrent positions reached (2)"}

        # ── CHECK 6: Oracle staleness cap ─────────────────────────────────────
        staleness = signal.get("poly_staleness_seconds")
        if staleness is not None and staleness > 90.0:
            self._log_risk_event(
                "ORACLE_STALENESS_EXCEEDED", signal,
                f"Staleness {staleness}s > 90s", "REJECT"
            )
            signal["rejection_reason"] = "CHECK_6_STALENESS"
            return {"status": "REJECTED", "reason": "Oracle staleness > 90s — feed likely dead"}

        # ── CHECK 7: Position sizing & approval ───────────────────────────────
        if strat in ("ORBIT_A_240", "ORBIT_A_260", "PHANTOM_MOMENTUM_V1"):
            size_usdc = 30.0
            signal["entry_mode"] = "SINGLE"
        else:
            # Non-ORBIT strategies: size relative to live bankroll (10-30% of equity)
            bankroll = getattr(sp, "bankroll", 1000.0) if sp else 1000.0
            momentum = signal.get("momentum", 0.01)

            if momentum <= 0.01:
                size_pct = 0.10
            elif momentum >= 0.03:
                size_pct = 0.30
            else:
                size_pct = 0.10 + (momentum - 0.01) * 10.0

            size_usdc = bankroll * size_pct
            size_usdc = max(33.0, min(bankroll * 0.30, size_usdc))

        if signal.get("entry_mode") == "DCA" and signal.get("dca_config"):
            rounds = signal["dca_config"]["rounds"]
            signal["dca_config"]["per_round_usdc"] = round