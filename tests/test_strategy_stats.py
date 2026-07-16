import asyncio
import logging
from strategy_stats import StrategyStatsEngine

logging.basicConfig(level=logging.INFO, format="%(message)s")

async def test_stats_engine():
    engine = StrategyStatsEngine()
    
    def create_mock_pos(stype, pnl, asset="BTC"):
        return {
            "strategy_type": stype,
            "asset": asset,
            "pnl": pnl,
            "signal_to_fill_ms": 50,
            "opened_at": 1781757161988 # static time for test
        }

    print("\n--- TEST: AUTO-RANKING & CAPITAL WEIGHTS ---")
    # Give LATENCY_ARB 6 wins, 4 losses (60% win rate)
    for _ in range(6): await engine.process_closed_trade(create_mock_pos("LATENCY_ARB", 10.0))
    for _ in range(4): await engine.process_closed_trade(create_mock_pos("LATENCY_ARB", -5.0))
    
    # Give LATE_STAGE_CONFIRM 8 wins, 2 losses (80% win rate)
    for _ in range(8): await engine.process_closed_trade(create_mock_pos("LATE_STAGE_CONFIRM", 10.0))
    for _ in range(2): await engine.process_closed_trade(create_mock_pos("LATE_STAGE_CONFIRM", -5.0))
    
    # The others have 0 trades, so 0% win rate.
    # Total active win rate = 0.60 + 0.80 = 1.40
    # Expected Weight for LATENCY: 0.60 / 1.40 = 0.4286 (43%)
    # Expected Weight for LATE_STAGE_CONFIRM: 0.80 / 1.40 = 0.5714 (57%)
    
    lat = engine.get_strategy_stats("LATENCY_ARB")
    late = engine.get_strategy_stats("LATE_STAGE_CONFIRM")
    print(f"LATENCY_ARB Win Rate: {lat['win_rate']}, Weight: {lat['capital_weight']}")
    print(f"LATE_STAGE_CONFIRM Win Rate: {late['win_rate']}, Weight: {late['capital_weight']}")


    print("\n--- TEST: AUTO-PAUSE TRIGGER ---")
    # Give FLASH_CRASH 10 wins, 20 losses (30 trades total, 33% win rate)
    for _ in range(10): await engine.process_closed_trade(create_mock_pos("FLASH_CRASH", 5.0))
    for _ in range(20): await engine.process_closed_trade(create_mock_pos("FLASH_CRASH", -10.0))
    
    fc = engine.get_strategy_stats("FLASH_CRASH")
    print(f"FLASH_CRASH Trades: {fc['trades_total']}, Win Rate: {round(fc['win_rate'], 4)}, Status: {fc['status']}")


    print("\n--- TEST: MAX DRAWDOWN & PROFIT FACTOR ---")
    # Breakout Scalper: Win +100, Loss -50, Loss -50, Win +200
    await engine.process_closed_trade(create_mock_pos("BREAKOUT_SCALPER", 100.0, "SOL"))
    await engine.process_closed_trade(create_mock_pos("BREAKOUT_SCALPER", -50.0, "ETH"))
    await engine.process_closed_trade(create_mock_pos("BREAKOUT_SCALPER", -50.0, "BTC"))
    await engine.process_closed_trade(create_mock_pos("BREAKOUT_SCALPER", 200.0, "SOL"))
    
    bs = engine.get_strategy_stats("BREAKOUT_SCALPER")
    print(f"BREAKOUT Total PNL: {bs['total_pnl']}")
    print(f"BREAKOUT Peak PNL: {bs['peak_pnl']}")
    print(f"BREAKOUT Max Drawdown: {bs['max_drawdown']}")
    print(f"BREAKOUT Profit Factor: {bs['profit_factor']}")
    print(f"BREAKOUT Best Asset: {bs['best_asset']}")

if __name__ == "__main__":
    asyncio.run(test_stats_engine())
