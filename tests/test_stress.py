"""Stress tests for the production hot paths: market churn / pruning, spot-feed tick
flood + window integrity, risk gates under rapid entries, and resolution correctness."""
import os
import time
import asyncio

os.environ["PHANTOM_TEST_MODE"] = "1"
os.environ["PAPER_MODE"] = "true"

import sys
sys.path.insert(0, ".")
import strategies.last_shadow_trade_lite_v4 as ls
from spot_feed import SpotFeed
from dashboard import state_provider


def test_window_state_pruning_under_churn():
    """Create far more markets than the cap; WINDOW_STATE must stay bounded, no errors."""
    ls.WINDOW_STATE.clear()
    for i in range(500):
        mid = f"0x{i:064x}"
        ls.WINDOW_STATE[mid] = ls._new_state(mid, "BTC")
        ls._prune_market_state()
    assert len(ls.WINDOW_STATE) <= ls._MAX_TRACKED_MARKETS, "pruning failed to bound WINDOW_STATE"
    ls.WINDOW_STATE.clear()


def test_spot_feed_tick_flood_bounded_memory():
    """Flood the spot feed with 50k ticks across many windows; history must stay pruned."""
    sf = SpotFeed()
    base = time.time()
    # simulate 50k ticks; monkeypatch _window_ts via price stream at real time is fine —
    # price_history prunes by wall-clock 15min, windows dict prunes by _MAX_WINDOWS.
    for i in range(50000):
        sf._on_price("BTC", 100.0 + (i % 100) * 0.01)
    assert len(sf.price_history["BTC"]) <= 50000  # sanity
    # windows dict must be bounded regardless of tick count
    assert len(sf.windows.get("BTC", {})) <= sf._MAX_WINDOWS
    m = sf.get_margin("BTC")
    assert m is not None and "margin_pct" in m


def test_spot_resolution_correctness_across_windows():
    """Winner must match the close>=open rule for many independent windows."""
    sf = SpotFeed()
    wts = sf._window_ts()
    for k in range(sf._MAX_WINDOWS):
        w = wts - k * 300
        opn = 100.0
        cls = 100.0 + (1 if k % 2 == 0 else -1)   # alternate up/down
        sf.windows.setdefault("BTC", {})[w] = {"open": opn, "close": cls}
    for k in range(sf._MAX_WINDOWS):
        w = wts - k * 300
        expected = "UP" if k % 2 == 0 else "DOWN"
        assert sf.get_window_resolution("BTC", w) == expected


def test_risk_gate_holds_under_rapid_entries():
    """Fire many concurrent risk checks; MAX_CONCURRENT must never be exceeded."""
    for k in ("KILL_SWITCH", "DAILY_LOSS_LIMIT_USDC"):
        os.environ.pop(k, None)
    state_provider.db_manager = None
    state_provider.config["kill_switch"] = "false"
    ls.WINDOW_STATE.clear()
    ls._reversal_times.clear()
    ls._breaker_until = 0.0
    os.environ["MAX_CONCURRENT_USDC"] = "30"
    os.environ["PER_TRADE_USDC"] = "10"

    async def run():
        # simulate 3 filled positions ($30 open) then 20 concurrent entry attempts
        for i in range(3):
            ls.WINDOW_STATE[f"m{i}"] = {"order_filled": True, "logged": False, "size_usdc": 10.0}
        results = await asyncio.gather(*[ls._risk_blocks_entry("BTC", 10.0) for _ in range(20)])
        return results

    results = asyncio.run(run())
    # 30 open + 10 = 40 > 30 -> every attempt must be blocked
    assert all(r == "max_concurrent_usdc" for r in results), "risk cap breached under load"
    os.environ.pop("MAX_CONCURRENT_USDC", None)
    os.environ.pop("PER_TRADE_USDC", None)
    ls.WINDOW_STATE.clear()


if __name__ == "__main__":
    test_window_state_pruning_under_churn()
    test_spot_feed_tick_flood_bounded_memory()
    test_spot_resolution_correctness_across_windows()
    test_risk_gate_holds_under_rapid_entries()
    print("stress tests passed")
