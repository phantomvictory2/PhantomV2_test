import logging
import os
import time
import asyncio

# Gate Last Shadow test fallback before importing the strategy
os.environ["PHANTOM_TEST_MODE"] = "1"

from strategies.last_shadow_trade_lite_v4 import classify as classify_last_shadow_trade_lite_v4
from dashboard import state_provider

logging.basicConfig(level=logging.INFO, format="%(message)s")


async def test_all_strategies():
    asset = "BTC"

    poly_base = {
        "market_id": "0x123",
        "asset": asset,
        "yes_price": 0.63,
        "no_price": 0.37,
        "prev_yes_price": 0.60,
        "spread": 0.04,
        "staleness_seconds": 1.0,
        "time_to_resolution_seconds": 120,
        "liquidity_usdc": 20000.0,
    }

    print("\n" + "=" * 50)
    print("TEST: LAST_SHADOW_TRADE_LITE_V4 (production v1 strategy)")

    # Pass: extreme consensus (0.94-0.99) in the final seconds (TTR 5-15s).
    poly_shadow = dict(poly_base, yes_price=0.95, prev_yes_price=0.93, time_to_resolution_seconds=10)
    sig_shadow = classify_last_shadow_trade_lite_v4(poly_shadow)
    print("[Pass] Expected: A | Actual:", sig_shadow["grade"])
    assert sig_shadow["grade"] == "A"
    assert sig_shadow["direction"] == "BUY_YES"

    # Skip: TTR too high (120s — outside the 5-15s test window).
    sig_high_ttr = classify_last_shadow_trade_lite_v4(poly_base)
    print("[High TTR Skip] Expected: SKIP | Actual:", sig_high_ttr["grade"])
    assert sig_high_ttr["grade"] == "SKIP"

    # Skip: in-window but price below the 0.94 threshold.
    poly_low = dict(poly_base, yes_price=0.80, no_price=0.20, time_to_resolution_seconds=10)
    assert classify_last_shadow_trade_lite_v4(poly_low)["grade"] == "SKIP"


if __name__ == "__main__":
    asyncio.run(test_all_strategies())
