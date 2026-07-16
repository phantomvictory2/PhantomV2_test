import asyncio
import logging
import time

from spot_feed import SpotFeed
from poly_feed import PolyFeed
from signal_engine import SignalEngine
from risk_engine import RiskEngine
from executor import Executor
from monitor import PositionMonitor
from strategy_stats import StrategyStatsEngine
from telegram import TelegramBot

logging.basicConfig(level=logging.INFO, format="%(message)s")

class E2EStateProvider:
    def __init__(self):
        self.config = {"kill_switch": False, "paper_mode": "true", "daily_trade_limit": 100}
        self.open_positions = []
        self.lag_stats = {"BTC": {"avg_lag_seconds": 15.0, "sample_size": 20, "status": "ACTIVE"}}
        self.daily_stats = {"BTC": {"loss_pct": 0.0, "win_rate": 0.50}}
        self.pos_stats = {"total": 0, "BTC": 0}
        self.trade_counts = {"total": 0, "BTC": 0}
        self.cons_loss = 0
        self.poly_feed = None
        self.spot_feed = None
        self.binance_feed = None
        self.strategy_engine = None
        self.bankroll = 1000.0
        self.pending_assets = set()

    def get_config(self, key, default): return self.config.get(key, default)
    def set_config(self, key, val): self.config[key] = val
    def get_asset_lag_stats(self, asset): return self.lag_stats.get(asset, {"status": "ACTIVE"})
    def get_daily_asset_stats(self, asset): return self.daily_stats.get(asset, {"loss_pct": 0, "win_rate": 0.5})
    def get_daily_stats(self): return {"daily_loss_pct": 0.0, "win_rate_today": 0.5, "loss_rate_today": 0.0, "trades_today": 0}
    def get_position_stats(self, asset): return {"total": 0, "asset_in_candle": 0}
    def get_open_markets(self): return set()
    def get_open_position_stats(self): return self.pos_stats
    def get_live_balance(self): return self.bankroll
    def get_daily_trade_count(self, asset): return self.trade_counts.get(asset, 0), self.trade_counts.get("total", 0)
    def get_consecutive_losses(self): return {"count": 0, "seconds_since_last": 9999}
    def get_bankroll(self): return self.bankroll
    def set_cooldown(self, asset, duration): pass
    def is_on_cooldown(self, asset): return False
    
    def get_market_state(self, asset):
        if self.poly_feed:
            return self.poly_feed.get_market_state(asset)
        return {"yes_price": 0.5, "no_price": 0.5}

    def get_open_positions(self): return self.open_positions
    def is_market_resolved(self, market_id): return True # Force resolution for test
    def get_resolution_price(self, market_id, dir): return 1.0 # Win

    def update_consecutive_loss_counter(self, result): pass
    def update_asset_lag_stats(self, asset, lag): pass
    
    def update_strategy_stats(self, stype, result, pnl):
        if self.strategy_engine:
            pass 
    
    def record_pnl(self, pnl): self.bankroll += pnl

    def get_dashboard_state(self):
        return {"bankroll": self.bankroll, "today_pnl": 0.0, "kill_switch": self.config["kill_switch"]}

def mock_db_callback(arg1, arg2=None):
    pass

async def test_end_to_end():
    print("--- PHANTOM V2 E2E TEST START ---")
    
    sp = E2EStateProvider()
    tg = TelegramBot()

    stats_engine = StrategyStatsEngine(telegram_callback=tg.send_alert, db_callback=mock_db_callback)
    sp.strategy_engine = stats_engine

    poly_feed = PolyFeed()
    sp.poly_feed = poly_feed
    poly_feed.set_active_market("BTC", "m_btc_1", "t_btc_1", int(time.time()*1000) + 200*1000, 10000.0, int(time.time()*1000))
    poly_feed.market_state["BTC"]["yes_price"] = 0.50
    poly_feed.market_state["BTC"]["no_price"] = 0.50
    poly_feed.market_state["BTC"]["last_updated_ms"] = int(time.time()*1000) - 15000

    signal_engine = SignalEngine(poly_feed=poly_feed)
    risk_engine = RiskEngine(state_provider=sp)
    
    executor = Executor(risk_engine, db_callback=mock_db_callback, telegram_callback=tg.send_alert, state_provider=sp)
    await executor.initialize()
    
    async def handle_signal(signal):
        print(f"\n[E2E] Signal Engine fired: {signal['strategy_type']} on {signal['asset']}")
        payload = await risk_engine.process_signal(signal)
        print(f"[E2E] Risk Engine returned: {payload['status']}")
        
        if payload["status"] == "APPROVED":
            await executor.process_approved_signal(payload)
            # Give executor a moment to log
            await asyncio.sleep(0.1)
            pos = {
                "id": "e2e_1", "signal_id": signal["signal_id"], "strategy_type": signal["strategy_type"],
                "market_id": signal["market_id"], "asset": signal["asset"], "direction": signal["direction"],
                "entry_price": 0.50, "size_usdc": payload["signal"]["approved_size_usdc"],
                "is_paper": True, "opened_at": int(time.time()*1000), "status": "OPEN", "signal_to_fill_ms": 15
            }
            sp.open_positions.append(pos)
            
    signal_engine.risk_engine_callback = handle_signal
    monitor = PositionMonitor(sp, db_callback=mock_db_callback, telegram_callback=tg.send_alert)

    print("\n[E2E] Simulating Coinbase BTC price surge (+0.5%)...")
    spot_feed = SpotFeed()
    sp.spot_feed = spot_feed
    
    # 1. Base price
    spot_feed.process_tick("BTC", 50000.0, int(time.time()*1000))
    await asyncio.sleep(0.1)
    
    # 2. Surge
    spot_feed.process_tick("BTC", 50250.0, int(time.time()*1000) + 1000)
    await asyncio.sleep(0.5) 
    
    print("\n[E2E] Triggering Monitor Resolution...")
    await monitor.check_positions()
    await asyncio.sleep(0.1)

    print("\n[E2E] End-to-End Flow Complete.")

if __name__ == "__main__":
    asyncio.run(test_end_to_end())
