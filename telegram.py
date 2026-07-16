import os
import logging
import asyncio
import urllib.request
import urllib.parse
import json

logger = logging.getLogger(__name__)

class TelegramBot:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.enabled = bool(self.token and self.chat_id)
        if not self.enabled:
            logger.warning("Telegram alerts DISABLED (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID).")

    async def send_alert(self, text: str):
        if not self.enabled:
            # If disabled in config, just log what would have been sent
            logger.info(f"[TELEGRAM_MOCK] {text}")
            return
            
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": self.chat_id,
            "text": text
        }).encode("utf-8")
        
        req = urllib.request.Request(url, data=data, method="POST")
        
        try:
            # Run blocking IO in a thread
            await asyncio.to_thread(self._send_request, req)
            logger.debug("Telegram alert sent successfully.")
        except Exception as e:
            logger.error(f"Failed to send Telegram alert: {e}")
            
    def _send_request(self, req):
        with urllib.request.urlopen(req, timeout=5.0) as response:
            return response.read()

# Helper templates to generate the exact required formats

def format_startup(paper_mode: bool) -> str:
    mode = "PAPER MODE" if paper_mode else "LIVE MODE"
    return f"🟢 Phantom V2 started — {mode}"

def format_trade_entry(paper: bool, strategy: str, asset: str, direction: str, size: float, price: float) -> str:
    prefix = "[PAPER] " if paper else ""
    return f"⚡ {prefix}TRADE ENTRY — {strategy} {asset} {direction} ${size:.2f} @ {price}"

def format_trade_win(paper: bool, strategy: str, asset: str, pnl: float, exec_ms: int) -> str:
    prefix = "[PAPER] " if paper else ""
    return f"✅ {prefix}TRADE WIN — {strategy} {asset} +${pnl:.2f} ({exec_ms}ms execution)"

def format_trade_loss(paper: bool, strategy: str, asset: str, pnl: float, reason: str) -> str:
    prefix = "[PAPER] " if paper else ""
    return f"❌ {prefix}TRADE LOSS — {strategy} {asset} -${abs(pnl):.2f} {reason}"

def format_regime_stop(loss_pct: float, win_rate: float) -> str:
    return f"⛔ REGIME STOP — loss {loss_pct*100:.1f}% + win rate {win_rate*100:.0f}% — all trading paused today"

def format_velocity_pause(asset: str, signals: int, window_min: int, pause_min: int) -> str:
    return f"⚠️ VELOCITY PAUSE — {asset} {signals} signals/{window_min}min — signal engine paused {pause_min}min"

def format_oracle_disabled(asset: str, lag: float) -> str:
    return f"⚠️ ORACLE DISABLED — {asset} avg lag {lag:.1f}s — removed from scanning"

def format_daily_summary(trades: int, wins: int, losses: int, win_rate: float, pnl: float, bankroll: float) -> str:
    sign = "+" if pnl >= 0 else "-"
    return f"🌙 DAILY SUMMARY — {trades} trades | {wins}W {losses}L | {win_rate*100:.0f}% | P&L {sign}${abs(pnl):.2f} | Bankroll ${bankroll:.2f}"

def format_strategy_review(strategy: str, win_rate: float, trades: int) -> str:
    return f"⚠️ STRATEGY REVIEW — {strategy} win rate {win_rate*100:.0f}% after {trades} trades — auto-paused"

def format_kill_switch() -> str:
    return "🔴 KILL SWITCH ACTIVATED"
