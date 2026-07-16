import asyncio
import time
import pytest
from datetime import datetime, timezone
from signal_validator import SignalValidator

class MockPolyFeed:
    def __init__(self):
        self.market_state = {
            "BTC": {
                "yes_price": 0.65,
                "no_price": 0.35,
                "best_bid": 0.63,
                "best_ask": 0.65,
                "spread": 0.02,
                "last_updated_ms": int(time.time() * 1000),
                "resolution_time_ms": int((time.time() + 200) * 1000),
                "time_to_resolution_seconds": 200,
                "liquidity_usdc": 1000.0,
                "bids": [{"price": "0.63", "size": "1000"}],
                "asks": [{"price": "0.65", "size": "1000"}]
            }
        }
    def get_market_state(self, asset):
        return self.market_state.get(asset)

class MockStateProvider:
    def __init__(self):
        self.poly_feed = MockPolyFeed()
        self.spot_feed = type("SF", (), {"price_history": {"BTC": []}})()
        self.binance_feed = None
        self.db_writer = None

@pytest.mark.asyncio
async def test_signal_validation_cases():
    print("\n==================================================")
    print("PHANTOM V2 — SIGNAL VALIDATION LAYER — TEST CASES")
    print("==================================================")

    sp = MockStateProvider()
    validator = SignalValidator(state_provider=sp)
    
    # 1. Warm-up Guard Check
    print("\n--- Warm-up Guard Check ---")
    signal = {
        "signal_id": "test_sig_1",
        "asset": "BTC",
        "strategy_type": "ORBIT_A_240",
        "direction": "BUY_YES",
        "time_to_resolution_seconds": 200,
        "yes_price": 0.65,
        "spread": 0.02,
        "liquidity_usdc": 1000.0
    }
    
    passed, score, block_reason = validator.validate_signal(signal)
    print(f"Passed: {passed} | Score: {score} | Reason: {block_reason}")
    assert not passed
    assert block_reason == "validator_warming_up"

    # Simulate completed warm-up and mock history
    validator.warm_up_complete = True
    
    # Mock current time deterministically to be 200s into the 5m window
    now = 6200.0
    validator.window_start_time = 6000.0

    import time
    time_func_orig = time.time
    time.time = lambda: now

    # Mock 3 consecutive rising candles for YES token (Close > Open)
    # Candle 1 (starts at 6000)
    # Candle 2 (starts at 6060)
    # Candle 3 (starts at 6120)
    # Active candle (starts at 6180)
    validator.yes_ticks["BTC"] = [
        (6010, 0.50),  # Open
        (6050, 0.54),  # Close
        # Candle 2
        (6070, 0.54),  # Open
        (6110, 0.58),  # Close
        # Candle 3
        (6130, 0.58),  # Open
        (6170, 0.64),  # Close (size 0.06)
        # Active candle
        (6185, 0.64),
        (6200, 0.65)
    ]
    
    # Mock BTC price ticks
    validator.btc_ticks = [
        (6010, 60000.0),
        (6200, 60050.0)
    ]

    # Adjust current time to weekday to pass weekend rules
    # We will temporarily mock datetime.now in validator check
    class MockDatetime:
        @classmethod
        def now(cls, tz=None):
            # A Wednesday (weekday)
            return datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
    
    import signal_validator
    dt_orig = signal_validator.datetime
    # Monkeypatch datetime
    signal_validator.datetime = MockDatetime

    # Case 2: PASSING SIGNAL
    print("\n--- Test Case 1: PASSING SIGNAL ---")
    # TTR = 200s, YES price = 0.65
    passed, score, block_reason = validator.validate_signal(signal)
    print(f"Passed: {passed} | Score: {score} | Block Reason: {block_reason}")
    assert passed
    assert score >= 70

    # Case 3: REJECTED SIGNAL - Weakening Momentum (Gate 1)
    print("\n--- Test Case 2: REJECTED SIGNAL (Weakening Momentum) ---")
    # Make the last completed candle (Candle 3) smaller than the second-to-last (Candle 2)
    # Candle 2 size: 0.58 - 0.54 = 0.04
    # Make Candle 3 size: 0.60 - 0.58 = 0.02 (which is smaller than 0.04)
    validator.yes_ticks["BTC"] = [
        (6010, 0.50),
        (6050, 0.54),
        # Candle 2 (size 0.04)
        (6070, 0.54),
        (6110, 0.58),
        # Candle 3 (size 0.02)
        (6130, 0.58),
        (6170, 0.60),
        # Active candle
        (6185, 0.60),
        (6200, 0.61)
    ]
    passed, score, block_reason = validator.validate_signal(signal)
    print(f"Passed: {passed} | Score: {score} | Block Reason: {block_reason}")
    assert not passed
    assert block_reason == "momentum_weakening"

    # Restore patches
    signal_validator.datetime = dt_orig
    time.time = time_func_orig


@pytest.mark.asyncio
async def test_signal_validator_daily_summary():
    import json
    import os
    from datetime import datetime, timezone, date

    sp = MockStateProvider()
    validator = SignalValidator(state_provider=sp)
    validator.warm_up_complete = True
    
    # Process 1 passing signal
    signal = {
        "signal_id": "test_sig_passed",
        "asset": "BTC",
        "strategy_type": "ORBIT_A_240",
        "direction": "BUY_YES",
        "time_to_resolution_seconds": 200,
        "yes_price": 0.65,
        "spread": 0.02,
        "liquidity_usdc": 1000.0
    }
    
    now = 6200.0
    validator.window_start_time = 6000.0
    validator.yes_ticks["BTC"] = [
        (6010, 0.50), (6050, 0.54),
        (6070, 0.54), (6110, 0.58),
        (6130, 0.58), (6170, 0.64),
        (6185, 0.64), (6200, 0.65)
    ]
    validator.btc_ticks = [(6010, 60000.0), (6200, 60050.0)]
    
    import signal_validator
    dt_orig = signal_validator.datetime
    class MockDatetime:
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
    signal_validator.datetime = MockDatetime

    import time
    time_func_orig = time.time
    time.time = lambda: now

    passed, score, reason = validator.validate_signal(signal)
    assert passed
    assert validator.stats["total_signals_received"] == 1
    assert validator.stats["passed_to_risk_engine"] == 1
    
    # Trigger midnight transition by changing the last checked date
    validator.last_checked_date = date(2026, 6, 30)
    
    summary_file = "logs/validator_daily_2026-06-30.json"
    if os.path.exists(summary_file):
        os.remove(summary_file)
        
    utc_now = datetime(2026, 7, 1, 0, 1, 0, tzinfo=timezone.utc)
    class MockDatetimeMidnight:
        @classmethod
        def now(cls, tz=None):
            return utc_now
    signal_validator.datetime = MockDatetimeMidnight
    
    current_date = utc_now.date()
    if validator.last_checked_date != current_date:
        validator._write_daily_summary(validator.last_checked_date)
        validator._reset_stats()
        validator.last_checked_date = current_date
        
    assert os.path.exists(summary_file)
    with open(summary_file, "r") as f:
        summary = json.load(f)
        assert summary["date"] == "2026-06-30"
        assert summary["total_signals_received"] == 1
        assert summary["passed_to_risk_engine"] == 1
        assert summary["avg_confidence_score_passing"] > 0
        
    os.remove(summary_file)
    signal_validator.datetime = dt_orig
    time.time = time_func_orig


