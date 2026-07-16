import pytest
from analytics_engine import AnalyticsEngine

@pytest.fixture
def engine():
    e = AnalyticsEngine()
    # Mock some trades
    e.trades = [
        {
            "trade_id": "1",
            "strategy": "ORBIT_A_240",
            "side": "BUY_YES",
            "entry_price": 0.65,
            "exit_price": 0.75,
            "position_size": 100,
            "profit_loss": 15.38,
            "result": "WIN",
            "hold_seconds": 30,
            "ttr_seconds": 120,
            "timeout_triggered": False,
            "sl_triggered": False,
            "spread_at_entry": 0.01
        },
        {
            "trade_id": "2",
            "strategy": "ORBIT_A_240",
            "side": "BUY_YES",
            "entry_price": 0.70,
            "exit_price": 0.50,
            "position_size": 100,
            "profit_loss": -28.57,
            "result": "LOSS",
            "hold_seconds": 10,
            "ttr_seconds": 120,
            "timeout_triggered": False,
            "sl_triggered": True,
            "spread_at_entry": 0.01
        },
        {
            "trade_id": "3",
            "strategy": "ORBIT_A_260",
            "side": "BUY_NO",
            "entry_price": 0.80,
            "exit_price": 0.90,
            "position_size": 100,
            "profit_loss": 12.5,
            "result": "WIN",
            "hold_seconds": 45,
            "ttr_seconds": 120,
            "timeout_triggered": False,
            "sl_triggered": False,
            "spread_at_entry": 0.01
        }
    ]
    return e

def test_calculate_strategy_stats(engine):
    stats = engine.calculate_strategy_stats()
    assert "ORBIT_A_240" in stats
    assert "ORBIT_A_260" in stats
    
    s240 = stats["ORBIT_A_240"]
    assert s240["trades"] == 2
    assert s240["wins"] == 1
    assert s240["losses"] == 1
    assert s240["win_rate"] == 50.0
    
def test_root_cause_analysis(engine):
    rca = engine.perform_root_cause_analysis()
    # Only losing trades should be in RCA
    assert "ORBIT_A_240" in rca
    assert len(rca["ORBIT_A_240"]) == 1
    
    # Trade 2 held for 10 seconds and sl_triggered = True -> FAKE_BREAKOUT
    assert rca["ORBIT_A_240"][0]["reason"] == "FAKE_BREAKOUT"
    
def test_win_patterns(engine):
    patterns = engine.analyze_win_patterns()
    assert "ORBIT_A_240" in patterns
    assert patterns["ORBIT_A_240"]["avg_entry"] == 0.65
    assert patterns["ORBIT_A_240"]["avg_hold"] == 30.0

def test_rank_strategies(engine):
    stats = engine.calculate_strategy_stats()
    ranking = engine.rank_strategies(stats)
    
    assert len(ranking) == 2
    # ORBIT_A_260 has 100% win rate, should be ranked 1st
    assert ranking[0]["strategy"] == "ORBIT_A_260"
    assert ranking[1]["strategy"] == "ORBIT_A_240"
