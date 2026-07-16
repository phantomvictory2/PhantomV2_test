import asyncio
import os
from telegram import (
    TelegramBot,
    format_startup,
    format_trade_entry,
    format_trade_win,
    format_trade_loss,
    format_regime_stop,
    format_velocity_pause,
    format_oracle_disabled,
    format_daily_summary,
    format_strategy_review,
    format_kill_switch
)

async def test_telegram_alerts():
    # Force disable sending to real API for the test unless env vars are provided
    # The class will mock print if token is missing
    bot = TelegramBot()

    print("\n--- TEST: FORMAT & FIRE ALL ALERTS ---")
    
    alerts = [
        format_startup(paper_mode=True),
        format_trade_entry(True, "LATENCY_ARB", "BTC", "YES", 12.40, 0.52),
        format_trade_win(True, "LATENCY_ARB", "BTC", 4.20, 67),
        format_trade_loss(True, "FLASH_CRASH", "SOL", -2.10, "STOP_LOSS"),
        format_regime_stop(0.032, 0.38),
        format_velocity_pause("BTC", 7, 15, 10),
        format_oracle_disabled("SOL", 4.2),
        format_daily_summary(8, 6, 2, 0.75, 18.40, 518.40),
        format_strategy_review("FLASH_CRASH", 0.33, 30),
        format_kill_switch()
    ]

    for alert in alerts:
        await bot.send_alert(alert)
        
    print("\n[OK] All 10 alert types generated successfully.")

if __name__ == "__main__":
    asyncio.run(test_telegram_alerts())
