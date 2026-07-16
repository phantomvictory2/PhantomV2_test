import asyncio
import logging
import unittest

from risk_engine import RiskEngine
from executor import Executor
from dashboard import GlobalStateProvider

logging.basicConfig(level=logging.INFO, format="%(message)s")

class TestPendingReservation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sp = GlobalStateProvider()
        self.sp.config["kill_switch"] = False
        self.sp.config["paper_mode"] = "true"
        self.sp.bankroll = 1000.0
        self.sp.lag_stats["BTC"] = {"avg_lag_seconds": 2.0, "sample_size": 5, "status": "ACTIVE"}
        
        self.risk_engine = RiskEngine(state_provider=self.sp)
        from test_risk_engine import MockPolyFeed
        self.sp.poly_feed = MockPolyFeed()
        self.executor = Executor(self.risk_engine, state_provider=self.sp)
        await self.executor.initialize()

        self.signal = {
            "signal_id": "test-sig-123",
            "asset": "BTC",
            "strategy_type": "LATENCY_ARB",
            "grade": "A_PLUS",
            "time_to_resolution_seconds": 200,
            "poly_staleness_seconds": 12.0,
            "market_id": "m_btc_1",
            "spread": 0.01,
            "liquidity_usdc": 1000.0,
            "entry_mode": "SINGLE",
            "direction": "BUY_YES",
            "yes_price": 0.55,
            "no_price": 0.46
        }

    async def test_duplicate_signal_rejection(self):
        print("\n--- TEST: Duplicate Signal Rejection (open-position exclusivity) ---")

        # Dedup design: main.py screens pending_assets BEFORE the risk engine, so the
        # risk engine no longer re-checks pending_assets (that would always self-reject).
        # The risk engine's CHECK 5 enforces exclusivity against OPEN positions instead.

        # 1. First signal is approved when the asset has no open position.
        payload1 = await self.risk_engine.process_signal(self.signal)
        self.assertEqual(payload1["status"], "APPROVED")

        # 2. Simulate the first trade now being live as an OPEN position.
        self.sp.open_positions.append({"asset": self.signal["asset"], "status": "OPEN"})
        print(f"Open position registered for {self.signal['asset']}")

        # 3. A second signal for the same asset must be rejected by CHECK 5.
        duplicate_signal = dict(self.signal, signal_id="test-sig-456")
        payload2 = await self.risk_engine.process_signal(duplicate_signal)

        print(f"Second signal status: {payload2['status']}")
        self.assertEqual(payload2["status"], "REJECTED")
        self.assertIn("already has an open position", payload2["reason"])
        self.assertEqual(duplicate_signal["rejection_reason"], "CHECK_5_EXCLUSIVITY")

        # Clean up
        self.sp.open_positions.clear()

    async def test_successful_execution_cleanup(self):
        print("\n--- TEST: Successful Execution Cleanup ---")
        
        # 1. Process signal first (it should be approved since asset is not pending yet)
        payload = await self.risk_engine.process_signal(self.signal)
        self.assertEqual(payload["status"], "APPROVED")
        
        # 2. Reserve asset immediately after signal approval
        self.sp.pending_assets.add(self.signal["asset"])
        self.assertIn(self.signal["asset"], self.sp.pending_assets)
        
        # Execute single trade (spawns background task inside Executor)
        # We run it directly and await the execution to simulate completion
        await self.executor._execute_single(payload["signal"])
        
        # Assert reservation was discarded
        print(f"Pending assets after successful trade: {self.sp.pending_assets}")
        self.assertNotIn(self.signal["asset"], self.sp.pending_assets)

    async def test_failed_execution_cleanup(self):
        print("\n--- TEST: Failed Execution Cleanup ---")
        
        # 1. Process signal first (it should be approved since asset is not pending yet)
        payload = await self.risk_engine.process_signal(self.signal)
        self.assertEqual(payload["status"], "APPROVED")
        
        # 2. Reserve asset immediately after signal approval
        self.sp.pending_assets.add(self.signal["asset"])
        self.assertIn(self.signal["asset"], self.sp.pending_assets)
        
        # We force an exception by passing invalid arguments to _execute_single
        # or by mocking the log function to fail
        original_log = self.executor._log_fill
        
        async def mock_fail_log(*args, **kwargs):
            raise RuntimeError("Database connection timed out during execution log")
            
        self.executor._log_fill = mock_fail_log
        
        try:
            with self.assertRaises(RuntimeError):
                await self.executor._execute_single(payload["signal"])
        finally:
            self.executor._log_fill = original_log
            
        # Assert reservation was still cleaned up by the finally block
        print(f"Pending assets after failed trade: {self.sp.pending_assets}")
        self.assertNotIn(self.signal["asset"], self.sp.pending_assets)

if __name__ == "__main__":
    unittest.main()
