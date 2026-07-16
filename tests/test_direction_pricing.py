import asyncio
import logging
import sys
from executor import Executor

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

class MockRiskEngine:
    def __init__(self):
        self.config = {"paper_mode": "true", "kill_switch": "false"}
        self.regime_stopped = False
    def get_config(self, key, default):
        return self.config.get(key, default)

class MockStateProvider:
    def __init__(self):
        self.market_state = {"yes_price": 0.54, "no_price": 0.46}
        self.open_positions = []
        self.pending_assets = set()
    def get_market_state(self, asset):
        return self.market_state

captured_positions = []

async def mock_db_callback(table, data):
    if table == "positions":
        captured_positions.append(data)

async def mock_tg_callback(msg):
    pass

async def test_direction_pricing():
    re = MockRiskEngine()
    sp = MockStateProvider()
    
    # 1. TEST PAPER SINGLE BUY_YES
    captured_positions.clear()
    executor = Executor(risk_engine=re, db_callback=mock_db_callback, telegram_callback=mock_tg_callback, state_provider=sp)
    await executor.initialize()
    
    yes_signal = {
        "signal_id": "yes_1",
        "strategy_type": "MOMENTUM_RIDE_5M",
        "market_id": "0x123",
        "asset": "BTC",
        "direction": "BUY_YES",
        "yes_price": 0.70,
        "no_price": 0.30,
        "approved_size_usdc": 10.0,
        "entry_mode": "SINGLE"
    }
    
    print("\nRunning BUY_YES SINGLE paper fill test...")
    await executor.process_approved_signal({"status": "APPROVED", "signal": yes_signal})
    await asyncio.sleep(0.1)
    
    assert len(captured_positions) == 1, "Position not logged"
    fill_price = captured_positions[0]["entry_price"]
    print(f"BUY_YES filled at: {fill_price} (expected: 0.70)")
    assert fill_price == 0.70, f"Expected 0.70, got {fill_price}"
    
    # 2. TEST PAPER SINGLE BUY_NO
    captured_positions.clear()
    no_signal = {
        "signal_id": "no_1",
        "strategy_type": "MOMENTUM_RIDE_5M",
        "market_id": "0x123",
        "asset": "BTC",
        "direction": "BUY_NO",
        "yes_price": 0.70,
        "no_price": 0.30,
        "approved_size_usdc": 10.0,
        "entry_mode": "SINGLE"
    }
    
    print("\nRunning BUY_NO SINGLE paper fill test...")
    await executor.process_approved_signal({"status": "APPROVED", "signal": no_signal})
    await asyncio.sleep(0.1)
    
    assert len(captured_positions) == 1, "Position not logged"
    fill_price = captured_positions[0]["entry_price"]
    print(f"BUY_NO filled at: {fill_price} (expected: 0.30)")
    assert fill_price == 0.30, f"Expected 0.30, got {fill_price}"

    # 3. TEST PAPER DCA BUY_NO
    captured_positions.clear()
    sp.market_state = {"yes_price": 0.80, "no_price": 0.20}
    dca_no_signal = {
        "signal_id": "no_dca_1",
        "strategy_type": "BREAKOUT_SCALPER",
        "market_id": "0x123",
        "asset": "BTC",
        "direction": "BUY_NO",
        "yes_price": 0.80,
        "no_price": 0.20,
        "approved_size_usdc": 10.0,
        "entry_mode": "DCA",
        "dca_config": {
            "rounds": 2, "per_round_usdc": 5.0, "limit_price": 0.22, "interval_seconds": 0.1
        }
    }
    
    print("\nRunning BUY_NO DCA paper fill test...")
    await executor.process_approved_signal({"status": "APPROVED", "signal": dca_no_signal})
    await asyncio.sleep(0.3)
    
    assert len(captured_positions) == 1, "Position not logged"
    fill_price = captured_positions[0]["entry_price"]
    print(f"BUY_NO DCA filled at: {fill_price} (expected: 0.20)")
    assert fill_price == 0.20, f"Expected 0.20, got {fill_price}"
    
    print("\nAll direction-aware pricing checks passed successfully!")

if __name__ == "__main__":
    asyncio.run(test_direction_pricing())
