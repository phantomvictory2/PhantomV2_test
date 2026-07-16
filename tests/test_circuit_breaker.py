import os
import time
import asyncio

os.environ["PHANTOM_TEST_MODE"] = "1"
os.environ["PAPER_MODE"] = "true"

import sys
sys.path.insert(0, ".")
import strategies.last_shadow_trade_lite_v4 as ls
from dashboard import state_provider


def _reset():
    ls._reversal_times.clear()
    ls._breaker_until = 0.0
    ls.WINDOW_STATE.clear()
    state_provider.db_manager = None
    state_provider.config["kill_switch"] = "false"
    for k in ("KILL_SWITCH", "MAX_CONCURRENT_USDC", "DAILY_LOSS_LIMIT_USDC",
              "CIRCUIT_BREAKER_REVERSALS", "CIRCUIT_BREAKER_WINDOW_SEC",
              "CIRCUIT_BREAKER_COOLDOWN_SEC"):
        os.environ.pop(k, None)


def test_breaker_off_by_default():
    _reset()
    assert ls._circuit_breaker_active() is False


def test_breaker_trips_at_threshold():
    _reset()
    os.environ["CIRCUIT_BREAKER_REVERSALS"] = "2"
    ls._record_reversal()
    assert ls._circuit_breaker_active() is False   # 1 reversal, below threshold
    ls._record_reversal()
    assert ls._circuit_breaker_active() is True     # 2 reversals -> tripped


def test_breaker_blocks_entry():
    _reset()
    os.environ["CIRCUIT_BREAKER_REVERSALS"] = "2"
    ls._record_reversal(); ls._record_reversal()
    assert asyncio.run(ls._risk_blocks_entry("BTC", 10.0)) == "circuit_breaker"


def test_old_reversals_age_out_of_window():
    _reset()
    os.environ["CIRCUIT_BREAKER_REVERSALS"] = "2"
    os.environ["CIRCUIT_BREAKER_WINDOW_SEC"] = "1800"
    os.environ["CIRCUIT_BREAKER_COOLDOWN_SEC"] = "0"   # no cooldown, test window only
    # two reversals from 40 min ago -> outside the 30-min window
    old = time.time() - 40 * 60
    ls._reversal_times.extend([old, old + 1])
    assert ls._circuit_breaker_active() is False


def test_cooldown_holds_after_trip():
    _reset()
    os.environ["CIRCUIT_BREAKER_REVERSALS"] = "2"
    os.environ["CIRCUIT_BREAKER_COOLDOWN_SEC"] = "3600"
    ls._record_reversal(); ls._record_reversal()
    assert ls._circuit_breaker_active() is True
    # even if reversals age out, cooldown keeps it active
    ls._reversal_times.clear()
    assert ls._circuit_breaker_active() is True   # still within cooldown


if __name__ == "__main__":
    test_breaker_off_by_default()
    test_breaker_trips_at_threshold()
    test_breaker_blocks_entry()
    test_old_reversals_age_out_of_window()
    test_cooldown_holds_after_trip()
    print("circuit breaker tests passed")
