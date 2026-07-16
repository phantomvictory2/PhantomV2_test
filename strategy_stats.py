import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any

logger = logging.getLogger(__name__)

class StrategyStatsEngine:
    def __init__(self, telegram_callback=None, db_callback=None):
        self.telegram_callback = telegram_callback
        self.db_callback = db_callback
        
        self.strategies = {}
        # Seed active strategies so they show up on the dashboard by default
        strategy_names = ["ORBIT_A_240", "ORBIT_A_260", "LAST_SHADOW_TRADE_LITE_V4"]
        
        for st in strategy_names:
            self._init_strategy_stats(st)

    def _init_strategy_stats(self, st: str):
        self.strategies[st] = {
            "strategy_type": st,
            "trades_total": 0,
            "buy_yes_signals": 0,
            "buy_no_signals": 0,
            "buy_yes_wins": 0,
            "buy_no_wins": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "avg_pnl_per_trade": 0.0,
            "total_pnl": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "profit_factor": 0.0,
            "max_drawdown": 0.0,
            "peak_pnl": 0.0,
            "total_execution_ms": 0,
            "avg_execution_ms": 0.0,
            "best_asset": "None",
            "best_time_window": "None",
            "status": "ACTIVE",
            "capital_weight": 0.25,  # initial equal weight
            "last_updated_ms": 0,
            
            # Internal tracking
            "_asset_pnl": {"BTC": 0.0, "ETH": 0.0, "SOL": 0.0},
            "_window_pnl": {}
        }

    async def process_closed_trade(self, position: Dict[str, Any]):
        stype = position.get("strategy_type")
        if not stype:
            return

        # LAST_SHADOW_TRADE_LITE_V4 owns its own strategy_stats row (recomputed from
        # the positions table by the strategy itself). This engine must not write it,
        # or its in-memory copy — which never tracks last-shadow closures — would
        # overwrite the real counts with 0/1.
        if stype == "LAST_SHADOW_TRADE_LITE_V4":
            return

        # Dynamically initialize unrecognized strategies (e.g. for legacy tests)
        if stype not in self.strategies:
            self._init_strategy_stats(stype)
            
        stats = self.strategies[stype]
        
        if stats["status"] == "PAUSED":
            return
            
        pnl = position.get("pnl", 0.0)
        asset = position.get("asset", "BTC")
        exec_ms = position.get("signal_to_fill_ms", 0)
        opened_at = position.get("opened_at", 0)
        
        # Calculate time window (e.g., "14:00-15:00")
        hour = datetime.fromtimestamp(opened_at / 1000.0, tz=timezone.utc).strftime("%H:00")
        
        pnl = position.get("pnl", 0.0)
        direction = position.get("direction", "BUY_YES")
        
        stats["trades_total"] += 1
        
        if direction == "BUY_YES":
            stats["buy_yes_signals"] += 1
        elif direction == "BUY_NO":
            stats["buy_no_signals"] += 1
            
        if pnl > 0:
            stats["wins"] += 1
            if direction == "BUY_YES":
                stats["buy_yes_wins"] += 1
            elif direction == "BUY_NO":
                stats["buy_no_wins"] += 1
                
            stats["gross_profit"] += pnl
        elif pnl < 0:
            stats["losses"] += 1
            stats["gross_loss"] += abs(pnl)
            
        # Update metrics
        stats["total_pnl"] += pnl
        if stats["total_pnl"] > stats["peak_pnl"]:
            stats["peak_pnl"] = stats["total_pnl"]
        
        current_drawdown = stats["peak_pnl"] - stats["total_pnl"]
        if current_drawdown > stats["max_drawdown"]:
            stats["max_drawdown"] = current_drawdown
            
        stats["win_rate"] = stats["wins"] / stats["trades_total"] if stats["trades_total"] > 0 else 0.0
        stats["avg_pnl_per_trade"] = stats["total_pnl"] / stats["trades_total"]
        stats["profit_factor"] = (stats["gross_profit"] / stats["gross_loss"]) if stats["gross_loss"] > 0 else float('inf')
        
        stats["total_execution_ms"] += exec_ms
        stats["avg_execution_ms"] = stats["total_execution_ms"] / stats["trades_total"]
        
        # Best Asset
        stats["_asset_pnl"][asset] = stats["_asset_pnl"].get(asset, 0) + pnl
        stats["best_asset"] = max(stats["_asset_pnl"].items(), key=lambda x: x[1])[0]
        
        # Best Time Window
        stats["_window_pnl"][hour] = stats["_window_pnl"].get(hour, 0) + pnl
        stats["best_time_window"] = max(stats["_window_pnl"].items(), key=lambda x: x[1])[0]
        
        stats["last_updated_ms"] = int(time.time() * 1000)

        # Check Auto-Pause
        if stats["win_rate"] < 0.45 and stats["trades_total"] >= 30 and stats["status"] == "ACTIVE":
            stats["status"] = "UNDER_REVIEW"
            msg = f"⚠️ {stype} auto-paused — win rate {round(stats['win_rate']*100, 1)}%"
            logger.warning(msg)
            if self.telegram_callback:
                if asyncio.iscoroutinefunction(self.telegram_callback):
                    asyncio.create_task(self.telegram_callback(msg))
                else:
                    self.telegram_callback(msg)

        # Check Auto-Ranking (every 10 trades across all strategies or per strategy?)
        # "After every 10 completed trades per strategy"
        if stats["trades_total"] % 10 == 0:
            await self._recalculate_capital_weights()
            if self.db_callback:
                for s in self.strategies.values():
                    # LAST_SHADOW_TRADE_LITE_V4 is self-contained: it closes its own
                    # positions and writes its own strategy_stats row directly. Writing
                    # this engine's in-memory copy (which stays at 0 because it never
                    # receives last-shadow closures) would overwrite the real counts.
                    if (s["status"] == "ACTIVE" and s["strategy_type"] != stype
                            and s["strategy_type"] != "LAST_SHADOW_TRADE_LITE_V4"):
                        if asyncio.iscoroutinefunction(self.db_callback):
                            await self.db_callback("strategy_stats", s)
                        else:
                            self.db_callback("strategy_stats", s)

        if self.db_callback:
            if asyncio.iscoroutinefunction(self.db_callback):
                await self.db_callback("strategy_stats", stats)
            else:
                self.db_callback("strategy_stats", stats)

    async def _recalculate_capital_weights(self):
        active_strats = [s for s in self.strategies.values() if s["status"] == "ACTIVE"]
        if not active_strats:
            return
            
        total_win_rate = sum(s["win_rate"] for s in active_strats)
        
        for s in active_strats:
            if total_win_rate > 0:
                s["capital_weight"] = round(s["win_rate"] / total_win_rate, 4)
            else:
                s["capital_weight"] = round(1.0 / len(active_strats), 4)
                
        logger.info("Capital weights recalculated based on win rates.")

    def get_strategy_stats(self, strategy_type: str) -> dict:
        return self.strategies.get(strategy_type)
