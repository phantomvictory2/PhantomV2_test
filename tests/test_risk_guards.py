import os
import asyncio

os.environ["PHANTOM_TEST_MODE"] = "1"
os.environ["PAPER_MODE"] = "true"

import sys
sys.path.insert(0, ".")
import strategies.last_shadow_trade_lite_v4 as ls
from dashboard import state_provider


def _reset_env():
    for k in ("PER_TRADE_USDC", "KILL_SWITCH", "MAX_CONCURRENT_USDC", "DAILY_LOSS_LIMIT_USDC"):
        os.environ.pop(k, None)
    ls.WINDOW_STATE.clear()
    state_provider.db_manager = None            # daily-pnl query returns cached 0
    state_provider.config["kill_switch"] = "false"   # reset shared-singleton config
    ls._daily_pnl_cache.update(day=None, pnl=0.0, ts=0.0)
    ls._reversal_times.clear()                  # reset circuit breaker
    ls._breaker_until = 0.0
    for k in ("CIRCUIT_BREAKER_REVERSALS", "CIRCUIT_BREAKER_WINDOW_SEC", "CIRCUIT_BREAKER_COOLDOWN_SEC"):
        os.environ.pop(k, None)


def test_position_size_env_override():
    _reset_env()
    assert ls._position_size() == ls.CONFIG["position_size_usdc"]  # default (50)
    os.environ["PER_TRADE_USDC"] = "5"
    assert ls._position_size() == 5.0
    _reset_env()


def test_kill_switch_env():
    _reset_env()
    assert ls._kill_switch_active() is False
    os.environ["KILL_SWITCH"] = "true"
    assert ls._kill_switch_active() is True
    _reset_env()


def test_open_exposure_from_window_state():
    _reset_env()
    ls.WINDOW_STATE["m1"] = {"order_filled": True, "logged": False, "size_usdc": 20.0}
    ls.WINDOW_STATE["m2"] = {"order_filled": True, "logged": True, "size_usdc": 20.0}   # resolved
    ls.WINDOW_STATE["m3"] = {"order_filled": False, "logged": False, "size_usdc": 20.0} # not filled
    assert ls._open_exposure_usdc() == 20.0  # only m1 counts
    _reset_env()


def test_open_exposure_ages_out_zombies():
    """A filled-but-never-resolved position must stop counting after EXPOSURE_MAX_AGE_SEC,
    so a stuck 'zombie' can't pin MAX_CONCURRENT_USDC and block all new entries forever."""
    import time
    _reset_env()
    os.environ["EXPOSURE_MAX_AGE_SEC"] = "360"
    now = time.time()
    # fresh fill (10s ago) counts; zombie (1 hour ago) does not
    ls.WINDOW_STATE["fresh"]  = {"order_filled": True, "logged": False, "size_usdc": 50.0,
                                 "filled_at": now - 10}
    ls.WINDOW_STATE["zombie"] = {"order_filled": True, "logged": False, "size_usdc": 50.0,
                                 "filled_at": now - 3600}
    assert ls._open_exposure_usdc() == 50.0   # only the fresh one counts
    os.environ.pop("EXPOSURE_MAX_AGE_SEC", None)
    _reset_env()


def test_risk_blocks_kill_switch():
    _reset_env()
    os.environ["KILL_SWITCH"] = "true"
    assert asyncio.run(ls._risk_blocks_entry("BTC", 5.0)) == "kill_switch"
    _reset_env()


def test_risk_blocks_max_concurrent():
    _reset_env()
    os.environ["MAX_CONCURRENT_USDC"] = "30"
    ls.WINDOW_STATE["m1"] = {"order_filled": True, "logged": False, "size_usdc": 20.0}
    # 20 open + 20 new = 40 > 30 -> blocked
    assert asyncio.run(ls._risk_blocks_entry("BTC", 20.0)) == "max_concurrent_usdc"
    # 20 open + 5 new = 25 <= 30 -> allowed
    assert asyncio.run(ls._risk_blocks_entry("BTC", 5.0)) is None
    _reset_env()


def test_risk_allows_when_unset():
    _reset_env()
    assert asyncio.run(ls._risk_blocks_entry("BTC", 50.0)) is None
    _reset_env()


if __name__ == "__main__":
    test_position_size_env_override()
    test_kill_switch_env()
    test_open_exposure_from_window_state()
    test_open_exposure_ages_out_zombies()
    test_risk_blocks_kill_switch()
    test_risk_blocks_max_concurrent()
    test_risk_allows_when_unset()
    print("risk guard tests passed")
