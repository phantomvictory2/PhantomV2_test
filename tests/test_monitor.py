import asyncio
import logging
import time
from monitor import PositionMonitor

logging.basicConfig(level=logging.INFO, format="%(message)s")

class MockStateProvider:
    def __init__(self):
        self.open_positions = []
        self.market_state = {"yes_price": 0.50, "no_price": 0.50}
        self.resolved_markets = {}
        self.resolution_prices = {}
        
        self.cons_loss = 0
        self.lag_history = []
        self.strategy_stats = {}
        self.total_pnl = 0.0

    def get_open_positions(self): return self.open_positions
    def get_market_state(self, asset): return self.market_state
    def is_market_resolved(self, m_id): return self.resolved_markets.get(m_id, False)
    def get_resolution_price(self, m_id, dir): return self.resolution_prices.get(m_id, 0.0)
    
    def update_consecutive_loss_counter(self, result):
        if result == "LOSS": self.cons_loss += 1
        else: self.cons_loss = 0
        
    def update_asset_lag_stats(self, asset, lag):
        self.lag_history.append(lag)
        
    def update_strategy_stats(self, pos: dict):
        stype = pos.get("strategy_type")
        pnl = pos.get("pnl", 0.0)
        result = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "BREAKEVEN")
        if stype not in self.strategy_stats:
            self.strategy_stats[stype] = {"wins": 0, "losses": 0, "pnl": 0}
        if result == "WIN": self.strategy_stats[stype]["wins"] += 1
        elif result == "LOSS": self.strategy_stats[stype]["losses"] += 1
        self.strategy_stats[stype]["pnl"] += pnl
        
    def record_pnl(self, pnl): self.total_pnl += pnl

async def test_monitor():
    sp = MockStateProvider()
    monitor = PositionMonitor(state_provider=sp)
    now_ms = int(time.time() * 1000)

    # Base Position Template
    base_pos = {
        "id": "1", "signal_id": "s1", "strategy_type": "LATENCY_ARB",
        "market_id": "m1", "asset": "BTC", "direction": "BUY_YES",
        "entry_price": 0.50, "size_usdc": 100.0, "is_paper": True,
        "signal_to_fill_ms": 67, "opened_at": now_ms, "status": "OPEN"
    }

    # 1. TEST RESOLUTION
    print("\n--- TEST: RESOLUTION (WIN) ---")
    pos_res = dict(base_pos, id="res_1", market_id="m_res")
    sp.open_positions = [pos_res]
    sp.resolved_markets["m_res"] = True
    sp.resolution_prices["m_res"] = 1.0 # 1.0 pays out 1.0 per share. Entry 0.5 -> shares 200 -> value 200 -> PNL +100
    await monitor.check_positions()
    print(f"Outcome PNL: {pos_res.get('pnl')}, Reason: {pos_res.get('close_reason')}")
    print(f"Asset lag updated? History count: {len(sp.lag_history)}")

    # 2. TEST TAKE PROFIT
    print("\n--- TEST: TAKE PROFIT (BREAKOUT SCALPER) ---")
    pos_tp = dict(base_pos, id="tp_1", strategy_type="BREAKOUT_SCALPER")
    sp.open_positions = [pos_tp]
    sp.market_state["yes_price"] = 0.80 # Target is 0.75, so 0.80 triggers TP
    await monitor.check_positions()
    print(f"Outcome PNL: {pos_tp.get('pnl')}, Reason: {pos_tp.get('close_reason')}")

    # 3. TEST STOP LOSS
    print("\n--- TEST: STOP LOSS (BREAKOUT SCALPER) ---")
    pos_sl = dict(base_pos, id="sl_1", strategy_type="BREAKOUT_SCALPER")
    sp.open_positions = [pos_sl]
    sp.market_state["yes_price"] = 0.30 # Entry 0.50. 0.30/0.50 = 60% value left (40% drop). SL is 25%.
    await monitor.check_positions()
    print(f"Outcome PNL: {pos_sl.get('pnl')}, Reason: {pos_sl.get('close_reason')}")

    # 4. TEST SAFETY EXIT (TIMEOUT)
    print("\n--- TEST: SAFETY EXIT TIMEOUT ---")
    pos_time = dict(base_pos, id="time_1", opened_at=now_ms - (25 * 60 * 1000)) # 25 mins ago
    sp.open_positions = [pos_time]
    sp.market_state["yes_price"] = 0.45
    await monitor.check_positions()
    print(f"Outcome PNL: {pos_time.get('pnl')}, Reason: {pos_time.get('close_reason')}")

if __name__ == "__main__":
    asyncio.run(test_monitor())
