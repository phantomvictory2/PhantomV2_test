import asyncio
from risk_engine import RiskEngine

class MockPolyFeed:
    def __init__(self):
        self.has_live_data = True

    def get_market_state(self, asset):
        return {
            "has_live_data": self.has_live_data,
            "staleness_seconds": 0.0,
            "time_to_resolution_seconds": 200,
            "spread": 0.02,
            "yes_price": 0.55,
            "no_price": 0.45,
            "best_bid": 0.53,
            "best_ask": 0.55
        }

class MockStateProvider:
    def __init__(self):
        self.config = {"kill_switch": False, "paper_mode": True, "max_trade_usdc": 100.0}
        self.daily_stats = {"daily_loss_pct": 0.0, "win_rate_today": 0.5, "loss_rate_today": 0.5, "trades_today": 0}
        self.cons_losses = {"count": 0, "seconds_since_last": 9999}
        self.lag_stats = {"sample_size": 15, "avg_lag_seconds": 12.0, "status": "ACTIVE"}
        self.pos_stats = {"total": 0, "asset_in_candle": 0}
        self.bankroll = 1000.0
        self.open_positions = []
        self.pending_assets = set()
        self.poly_feed = MockPolyFeed()

    def get_config(self, key, default): return self.config.get(key, default)
    def get_daily_stats(self): return self.daily_stats
    def get_consecutive_losses(self): return self.cons_losses
    def get_asset_lag_stats(self, asset): return self.lag_stats
    def disable_asset(self, asset): self.lag_stats["status"] = "DISABLED"
    def get_position_stats(self, asset): return self.pos_stats
    def get_live_balance(self): return self.bankroll

async def test_risk_checks():
    sp = MockStateProvider()
    engine = RiskEngine(state_provider=sp)
    
    base_signal = {
        "signal_id": "mock_123",
        "asset": "BTC",
        "strategy_type": "LATENCY_ARB",
        "grade": "A_PLUS",
        "direction": "BUY_YES",
        "yes_price": 0.55,
        "no_price": 0.45,
        "time_to_resolution_seconds": 200,
        "poly_staleness_seconds": 12.0,
        "market_id": "m_123",
        "spread": 0.02,
        "liquidity_usdc": 20000,
        "velocity_count": 1,
        "entry_mode": "SINGLE"
    }

    print("\n--- TEST: APPROVED ---")
    res = await engine.process_signal(dict(base_signal))
    print(f"Status: {res['status']}")
    if res['status'] == 'APPROVED':
        print(f"Size: {res['signal']['approved_size_usdc']} USDC (5% of 1000)")

    print("\n--- TEST: KILL SWITCH ---")
    sp.config["kill_switch"] = True
    res = await engine.process_signal(dict(base_signal))
    print(f"Status: {res['status']} | Reason: {res['reason']}")
    sp.config["kill_switch"] = False

    print("\n--- TEST: MARKET VALIDITY (Spread > 5%) ---")
    bad_market = dict(base_signal, spread=0.06)
    res = await engine.process_signal(bad_market)
    print(f"Status: {res['status']} | Reason: {res['reason']}")

    print("\n--- TEST: DAILY REGIME PROTECTION (IGNORED) ---")
    sp.daily_stats["daily_loss_pct"] = 0.04
    sp.daily_stats["win_rate_today"] = 0.35
    res = await engine.process_signal(dict(base_signal))
    print(f"Status: {res['status']} (Expected: APPROVED)")
    # Reset engine regime
    engine.regime_stopped = False
    sp.daily_stats["daily_loss_pct"] = 0.0

    print("\n--- TEST: CONSECUTIVE LOSS COOLDOWN (IGNORED) ---")
    sp.cons_losses["count"] = 3
    sp.cons_losses["seconds_since_last"] = 500
    res = await engine.process_signal(dict(base_signal))
    print(f"Status: {res['status']} (Expected: APPROVED)")
    sp.cons_losses["count"] = 0

    print("\n--- TEST: SIGNAL VELOCITY (IGNORED) ---")
    fast_signal = dict(base_signal, velocity_count=6)
    res = await engine.process_signal(fast_signal)
    print(f"Status: {res['status']} (Expected: APPROVED)")
    # Reset pause
    engine.paused_until = 0

    print("\n--- TEST: ASSET ORACLE PERFORMANCE ---")
    sp.lag_stats["avg_lag_seconds"] = 4.0
    res = await engine.process_signal(dict(base_signal))
    print(f"Status: {res['status']} | Reason: {res['reason']}")
    sp.lag_stats["avg_lag_seconds"] = 15.0
    sp.lag_stats["status"] = "ACTIVE"

    print("\n--- TEST: OPEN POSITION LIMITS ---")
    sp.pos_stats["total"] = 3
    res = await engine.process_signal(dict(base_signal))
    print(f"Status: {res['status']} | Reason: {res['reason']}")
    sp.pos_stats["total"] = 0

    print("\n--- TEST: DYNAMIC TRADE LIMITS (IGNORED) ---")
    sp.daily_stats["win_rate_today"] = 0.70
    sp.daily_stats["trades_today"] = 100
    res = await engine.process_signal(dict(base_signal))
    print(f"Status: {res['status']} (Expected: APPROVED)")

    print("\n--- TEST: POSITION SIZING (DCA and No Dynamic Reduction) ---")
    sp.daily_stats["trades_today"] = 5
    sp.daily_stats["loss_rate_today"] = 0.35
    dca_signal = dict(base_signal, grade="A", entry_mode="DCA", dca_config={"rounds": 10})
    res = await engine.process_signal(dca_signal)
    print(f"Status: {res['status']}")
    if res['status'] == 'APPROVED':
        size = res['signal']['approved_size_usdc']
        per_round = res['signal']['dca_config']['per_round_usdc']
        print(f"Size: {size} USDC (Grade A 2% of 1000 = 20, Expected: 20)")
        print(f"Per round: {per_round} USDC")

if __name__ == "__main__":
    asyncio.run(test_risk_checks())
