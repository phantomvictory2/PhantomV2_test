"""
spot_feed.py — Coinbase spot price feed (BTC/ETH/SOL-USD) for Tier 2 instrumentation.

Polymarket's 5-min markets resolve on Chainlink Data Streams: "Up" if the asset's
price at the window CLOSE >= the price at the window OPEN. Coinbase spot tracks the
Chainlink median within basis points, so it's a good real-time proxy for "is the asset
decisively above/below its window-open price."

Phase 2a is INSTRUMENTATION ONLY — this feed records the margin at each Last Shadow
fill so we can later calibrate a conviction filter. It does NOT gate any trades yet,
and it degrades safely: if Coinbase is unreachable, margin is simply unavailable.

Window alignment: Polymarket 5-min windows align to wall-clock 5-min boundaries (ET
offsets are whole hours, so also 5-min boundaries in UTC). The current window's open
is therefore floor(now/300)*300, and we capture the first tick of each new window as
its open price.
"""

import time
import json
import asyncio
import logging
from collections import deque

import websockets

logger = logging.getLogger("SpotFeed")

_PRODUCTS = {"BTC-USD": "BTC", "ETH-USD": "ETH", "SOL-USD": "SOL"}
_URL = "wss://ws-feed.exchange.coinbase.com"


class SpotFeed:
    _MAX_WINDOWS = 20   # ~100 min of windows — covers the 60-min settlement-sweep lookback

    def __init__(self):
        self.latest: dict = {}        # asset -> latest price
        # asset -> {window_ts: {"open": float, "close": float}}. `close` is the last
        # price seen while that window was current ≈ the price at window close, which
        # is what Chainlink resolves on (Up if close >= open).
        self.windows: dict = {}
        self.connected = False
        self.last_msg_ts = 0.0
        self.symbols = ["BTC", "ETH", "SOL"]
        self.price_history = {sym: deque() for sym in self.symbols}
        self.reconnect_count = 0

    @staticmethod
    def _window_ts(now: float = None) -> int:
        now = now if now is not None else time.time()
        return int(now - (now % 300))

    def _on_price(self, asset: str, price: float):
        self.latest[asset] = price
        now_ms = int(time.time() * 1000)
        self.price_history[asset].append((now_ms, price))
        
        # Prune old price history (> 15 minutes)
        cutoff_ms = now_ms - 15 * 60 * 1000
        while self.price_history[asset] and self.price_history[asset][0][0] < cutoff_ms:
            self.price_history[asset].popleft()

        wts = self._window_ts()
        w = self.windows.setdefault(asset, {})
        if wts not in w:
            w[wts] = {"open": price, "close": price}   # first tick of the window ≈ open
            if len(w) > self._MAX_WINDOWS:
                for k in sorted(w)[:-self._MAX_WINDOWS]:
                    w.pop(k, None)
        else:
            w[wts]["close"] = price                    # keep close = latest in-window price

    def process_tick(self, asset: str, price: float, timestamp_ms: int):
        """Inject a tick manually (for paper mode testing & dashboard)."""
        self.latest[asset] = price
        self.price_history[asset].append((timestamp_ms, price))
        
        # Prune old price history (> 15 minutes)
        cutoff_ms = timestamp_ms - 15 * 60 * 1000
        while self.price_history[asset] and self.price_history[asset][0][0] < cutoff_ms:
            self.price_history[asset].popleft()

        # Update windows
        wts = self._window_ts(timestamp_ms / 1000.0)
        w = self.windows.setdefault(asset, {})
        if wts not in w:
            w[wts] = {"open": price, "close": price}
            if len(w) > self._MAX_WINDOWS:
                for k in sorted(w)[:-self._MAX_WINDOWS]:
                    w.pop(k, None)
        else:
            w[wts]["close"] = price

    def get_margin(self, asset: str):
        """Return margin for the CURRENT window, or None if unavailable.
        margin_pct > 0 => price above open (favors 'Up'/BUY_YES)."""
        price = self.latest.get(asset)
        wts = self._window_ts()
        w = (self.windows.get(asset) or {}).get(wts)
        if price is None or not w or not w.get("open"):
            return None
        open_price = w["open"]
        margin = (price - open_price) / open_price
        return {
            "window_ts": wts,
            "open": round(open_price, 4),
            "now": round(price, 4),
            "margin_pct": round(margin * 100, 5),
        }

    def get_window_resolution(self, asset: str, window_ts: int):
        """Chainlink-proxy resolution for a finished window: 'UP' if the spot price at
        close >= the price at open, else 'DOWN'. None if we lack data for that window."""
        w = (self.windows.get(asset) or {}).get(window_ts)
        if not w or not w.get("open") or not w.get("close"):
            return None
        return "UP" if w["close"] >= w["open"] else "DOWN"

    async def run(self):
        while True:
            try:
                async with websockets.connect(_URL, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "product_ids": list(_PRODUCTS),
                        "channels": ["ticker"],
                    }))
                    self.connected = True
                    logger.info("SpotFeed connected to Coinbase (BTC/ETH/SOL-USD)")
                    async for raw in ws:
                        try:
                            m = json.loads(raw)
                            if m.get("type") == "ticker":
                                asset = _PRODUCTS.get(m.get("product_id"))
                                price = m.get("price")
                                if asset and price:
                                    self._on_price(asset, float(price))
                                    self.last_msg_ts = time.time()
                        except Exception:
                            continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.connected = False
                self.reconnect_count += 1
                logger.error(f"SpotFeed error: {e}; reconnecting in 5s")
                await asyncio.sleep(5)

