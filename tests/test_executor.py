import asyncio
import logging
from executor import Executor

logging.basicConfig(level=logging.INFO, format="%(message)s")

class MockRiskEngine:
    def __init__(self):
        self.config = {"paper_mode": "true", "kill_switch": "false"}
        self.regime_stopped = False
    def get_config(self, key, default):
        return self.config.get(key, default)

class MockStateProvider:
    def __init__(self):
        self.market_state = {"yes_price": 0.50}
        self.open_positions = []
        self.pending_assets = set()
    def get_market_state(self, asset):
        return self.market_state

async def mock_db_callback(table, data):
    print(f"DB [{table}]: {data}")

async def mock_tg_callback(msg):
    try:
        print(f"TELEGRAM: {msg}")
    except UnicodeEncodeError:
        print(f"TELEGRAM (safe): {msg.encode('ascii', 'ignore').decode('ascii')}")

async def test_executor():
    re = MockRiskEngine()
    sp = MockStateProvider()
    executor = Executor(risk_engine=re, db_callback=mock_db_callback, telegram_callback=mock_tg_callback, state_provider=sp)
    await executor.initialize()
    
    base_signal = {
        "signal_id": "test_1",
        "strategy_type": "LATENCY_ARB",
        "market_id": "0x123",
        "asset": "BTC",
        "direction": "BUY_YES",
        "yes_price": 0.54,
        "no_price": 0.52,
        "approved_size_usdc": 12.40,
        "entry_mode": "SINGLE"
    }

    print("\n--- TEST: SINGLE ENTRY (PAPER MODE) ---")
    await executor.process_approved_signal({"status": "APPROVED", "signal": dict(base_signal)})
    await asyncio.sleep(0.1)

    print("\n--- TEST: DCA ENTRY (NORMAL COMPLETION) ---")
    dca_signal = dict(base_signal, entry_mode="DCA", dca_config={
        "rounds": 5, "per_round_usdc": 2.0, "limit_price": 0.55, "interval_seconds": 0.1
    })
    await executor.process_approved_signal({"status": "APPROVED", "signal": dict(dca_signal)})
    await asyncio.sleep(0.6) # Wait for 5 rounds of 0.1s

    print("\n--- TEST: DCA ENTRY (ADVERSE PRICE STOP) ---")
    sp.market_state["yes_price"] = 0.54
    # Launch DCA
    await executor.process_approved_signal({"status": "APPROVED", "signal": dict(dca_signal)})
    # Wait 2 rounds, then change price drastically
    await asyncio.sleep(0.25)
    sp.market_state["yes_price"] = 0.65 # 0.55 * 1.10 = 0.605, so 0.65 is adverse
    print("Market price spiked to 0.65...")
    await asyncio.sleep(0.3)

    print("\n--- TEST: DCA ENTRY (KILL SWITCH STOP) ---")
    # Reset price
    sp.market_state["yes_price"] = 0.54
    await executor.process_approved_signal({"status": "APPROVED", "signal": dict(dca_signal)})
    await asyncio.sleep(0.25)
    print("Activating kill switch...")
    re.config["kill_switch"] = "true"
    await asyncio.sleep(0.3)

if __name__ == "__main__":
    asyncio.run(test_executor())
