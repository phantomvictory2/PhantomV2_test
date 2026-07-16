import asyncio
import json
import logging
import os
import time
import websockets
from typing import Dict, Any

logger = logging.getLogger(__name__)

class PolyFeed:
    def __init__(self, signal_callback=None):
        # State stored by asset: 'BTC', 'ETH', 'SOL'
        self.market_state: Dict[str, Dict[str, Any]] = {}
        # Mapping from token_id to asset to route WS messages
        self.token_to_asset: Dict[str, str] = {}
        self.signal_callback = signal_callback
        self.running = False
        self.connected = False
        self.ws = None
        self.reconnect_count = 0
        self.client_reconnects = 0
        self.server_disconnects = 0
        # Feed instrumentation
        self._msg_count = 0            # total WS messages processed (price updates)
        self._msg_count_window = 0     # messages in current 60-second window
        self._window_start_ts = time.time()
        self._subscription_count = 0   # total subscription messages sent
        self._last_msg_ts = 0.0        # epoch seconds of last price update received
        self.last_message_timestamp = 0.0
        
    async def update_subscriptions(self):
        try:
            if self.connected and self.ws:
                logger.info(f"Forcing WS reconnect to update Polymarket subscriptions: {list(self.token_to_asset.keys())}")
                await self.ws.close(code=1000, reason="Resubscribe")
        except Exception as e:
            logger.error(f"Failed to force disconnect for subscriptions: {e}")

    def set_active_market(self, asset: str, market_id: str, token_id: str,
                          resolution_time_ms: int, liquidity_usdc: float, market_open_time: int,
                          no_token_id: str = None, condition_id: str = None):
        """
        Register the active 5-minute market. `token_id` is the subscribed (YES/Up) outcome
        whose price we track; `no_token_id` is the opposite (NO/Down) outcome, needed to
        place a live BUY_NO order. (Convention: clobTokenIds[0]=YES/Up, [1]=NO/Down — verify
        on first live order.)
        """
        # Check if we already track this exact token for this asset
        is_new_token = token_id not in self.token_to_asset
        
        # If it's a new token but we were tracking another token for this asset, clean up the old token mapping
        if is_new_token:
            old_tokens = [t for t, a in self.token_to_asset.items() if a == asset]
            for t in old_tokens:
                self.token_to_asset.pop(t, None)
                
        self.market_state[asset] = {
            "market_id": market_id,
            "asset": asset,
            "yes_token_id": token_id,
            "no_token_id": no_token_id,
            "yes_price": self.market_state.get(asset, {}).get("yes_price", 0.50) if not is_new_token else 0.50,
            "no_price": self.market_state.get(asset, {}).get("no_price", 0.50) if not is_new_token else 0.50,
            "prev_yes_price": self.market_state.get(asset, {}).get("prev_yes_price", 0.50) if not is_new_token else 0.50,
            "best_bid": self.market_state.get(asset, {}).get("best_bid", 0.50) if not is_new_token else 0.50,
            "best_ask": self.market_state.get(asset, {}).get("best_ask", 0.50) if not is_new_token else 0.50,
            "spread": self.market_state.get(asset, {}).get("spread", 0.0) if not is_new_token else 0.0,
            "last_updated_ms": self.market_state.get(asset, {}).get("last_updated_ms", int(time.time() * 1000)) if not is_new_token else int(time.time() * 1000),
            "staleness_seconds": self.market_state.get(asset, {}).get("staleness_seconds", 0.0) if not is_new_token else 0.0,
            "resolution_time_ms": resolution_time_ms,
            "time_to_resolution_seconds": 0,
            "liquidity_usdc": liquidity_usdc,
            "market_open_time": market_open_time,
            "is_stale": self.market_state.get(asset, {}).get("is_stale", False) if not is_new_token else False,
            "has_live_data": self.market_state.get(asset, {}).get("has_live_data", False) if not is_new_token else False,
            "had_first_tick": self.market_state.get(asset, {}).get("had_first_tick", False) if not is_new_token else False,
            "condition_id": condition_id or market_id,  # conditionId for redemption; fall back to market_id
        }
        self.token_to_asset[token_id] = asset
        
        if is_new_token:
            logger.info(f"PolyFeed tracking NEW {asset} market: {market_id} (Token: {token_id})")
            if self.connected and self.ws:
                asyncio.create_task(self.update_subscriptions())
        else:
            logger.debug(f"PolyFeed updated metadata for existing {asset} market: {market_id}")

    def cleanup_markets(self, active_tokens: list):
        """
        Removes any tokens and market states not in the provided active_tokens list
        and sends a new subscription message to the WebSocket.
        """
        tokens_to_remove = [t for t in self.token_to_asset.keys() if t not in active_tokens]
        if not tokens_to_remove:
            return
            
        for token_id in tokens_to_remove:
            asset = self.token_to_asset.pop(token_id, None)
            if asset and asset in self.market_state:
                # check if this asset has another token being tracked
                if not any(a == asset for a in self.token_to_asset.values()):
                    self.market_state.pop(asset, None)
            logger.info(f"PolyFeed removed tracking for inactive token: {token_id}")
            
        if self.connected and self.ws:
            asyncio.create_task(self.update_subscriptions())

    def get_market_state(self, asset: str) -> Dict[str, Any]:
        """
        Called by Signal Engine synchronously in the hot path.
        Recalculates staleness and time_to_resolution on every call.
        """
        state = self.market_state.get(asset)
        if not state:
            return None
            
        now_ms = int(time.time() * 1000)
        staleness = (now_ms - state["last_updated_ms"]) / 1000.0
        
        time_to_res = max(0, int((state["resolution_time_ms"] - now_ms) / 1000))
        
        state["staleness_seconds"] = round(staleness, 3)
        state["time_to_resolution_seconds"] = time_to_res
        state["is_stale"] = staleness > 10.0
        
        # Return a copy to prevent accidental mutation downstream, 
        # though in a true hot path we might return the reference.
        return state.copy()

    def process_ws_message(self, data: dict):
        """
        Processes incoming WS updates. We expect orderbook updates to calculate prices and spread.
        """
        token_id = data.get("asset_id") or data.get("token_id")
        if not token_id or token_id not in self.token_to_asset:
            return
            
        asset = self.token_to_asset[token_id]
        state = self.market_state[asset]
        old_yes_price = state.get("yes_price", 0.50)
        old_no_price = state.get("no_price", 0.50)
        
        # 1. Real Polymarket CLOB best_bid_ask top-of-book payload structure
        if "best_bid" in data or "best_ask" in data:
            try:
                best_bid = float(data["best_bid"]) if data.get("best_bid") not in (None, "") else None
            except (ValueError, TypeError):
                best_bid = None
            try:
                best_ask = float(data["best_ask"]) if data.get("best_ask") not in (None, "") else None
            except (ValueError, TypeError):
                best_ask = None
            
            if best_bid is not None and best_ask is not None:
                state["has_live_data"] = True
                self.last_message_timestamp = time.time()
                state["yes_price"] = best_ask
                state["no_price"] = round(1.0 - best_bid, 4)
                state["best_bid"] = best_bid
                state["best_ask"] = best_ask
                state["spread"] = round(best_ask - best_bid, 4)
                # Do NOT fabricate fake depth — leave bids/asks empty so Gate 4
                # falls through to the real liquidity_usdc value from market metadata.
                state["bids"] = []
                state["asks"] = []
                state["last_updated_ms"] = int(time.time() * 1000)
                # Instrumentation counters
                self._msg_count += 1
                self._msg_count_window += 1
                self._last_msg_ts = time.time()
                logger.debug(f"PolyFeed best_bid_ask update {asset}: YES={state['yes_price']}, NO={state['no_price']}, spread={state['spread']}")
 
        # 2. Real Polymarket CLOB payload structure (bids/asks lists)
        elif "bids" in data or "asks" in data:
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            
            # Parse bids/asks safely, finding the maximum bid and minimum ask price levels
            parsed_bids = []
            for b in bids:
                try:
                    p = b.get("price")
                    if p not in (None, ""):
                        parsed_bids.append(float(p))
                except (ValueError, TypeError):
                    pass
            best_bid = max(parsed_bids) if parsed_bids else None
 
            parsed_asks = []
            for a in asks:
                try:
                    p = a.get("price")
                    if p not in (None, ""):
                        parsed_asks.append(float(p))
                except (ValueError, TypeError):
                    pass
            best_ask = min(parsed_asks) if parsed_asks else None
            
            if best_bid is not None and best_ask is not None:
                state["has_live_data"] = True
                state["yes_price"] = best_ask
                state["no_price"] = round(1.0 - best_bid, 4)
                state["best_bid"] = best_bid
                state["best_ask"] = best_ask
                state["spread"] = round(best_ask - best_bid, 4)
                state["bids"] = bids
                state["asks"] = asks
                state["last_updated_ms"] = int(time.time() * 1000)
                # Instrumentation counters
                self._msg_count += 1
                self._msg_count_window += 1
                self._last_msg_ts = time.time()
                logger.debug(f"PolyFeed update {asset}: YES={state['yes_price']}, NO={state['no_price']}, spread={state['spread']}")
 
        # 3. Simulated/legacy payload structure
        elif "yes_bid" in data and "yes_ask" in data:
            yes_bid = float(data["yes_bid"])
            yes_ask = float(data["yes_ask"])
            
            # Buying YES means paying the ask
            state["yes_price"] = yes_ask
            # Buying NO usually means paying (1 - bid)
            state["no_price"] = round(1.0 - yes_bid, 4)
            state["best_bid"] = yes_bid
            state["best_ask"] = yes_ask
            state["spread"] = round(yes_ask - yes_bid, 4)
            state["bids"] = []
            state["asks"] = []
            state["last_updated_ms"] = int(time.time() * 1000)

        # Trigger callback if yes_price or no_price changed
        new_yes_price = state.get("yes_price", 0.50)
        new_no_price = state.get("no_price", 0.50)
        
        if new_yes_price != old_yes_price or new_no_price != old_no_price:
            if not state.get("had_first_tick", False):
                state["had_first_tick"] = True
                state["prev_yes_price"] = new_yes_price
                state["prev_no_price"] = new_no_price
                return

            state["prev_yes_price"] = old_yes_price
            state["prev_no_price"] = old_no_price
            if self.signal_callback:
                updated_state = self.get_market_state(asset)
                if updated_state:
                    if asyncio.iscoroutinefunction(self.signal_callback):
                        task = asyncio.create_task(self.signal_callback(asset, updated_state))
                        def handle_exception(t):
                            try:
                                t.result()
                            except Exception as e:
                                logger.error(f"Exception inside poly_feed signal_callback for {asset}: {e}", exc_info=True)
                        task.add_done_callback(handle_exception)
                    else:
                        try:
                            self.signal_callback(asset, updated_state)
                        except Exception as e:
                            logger.error(f"Exception inside poly_feed sync signal_callback for {asset}: {e}", exc_info=True)

    async def run(self):
        self.running = True
        url = os.getenv("POLY_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/")
        
        # Append 'market' sub-path if it points to the base /ws/ path to prevent HTTP 404
        if url.endswith("/ws/"):
            url = url + "market"
        elif url.endswith("/ws"):
            url = url + "/market"
            
        backoff = 1
        
        while self.running:
            try:
                # We only connect if we have tokens to subscribe to
                if not self.token_to_asset:
                    await asyncio.sleep(2)
                    continue
                    
                logger.info(f"Connecting to Poly WS: {url}")
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    self.ws = ws
                    backoff = 1
                    self.connected = True
                    logger.info("Connected to Polymarket stream.")
                    
                    # Send standard subscription message
                    sub_msg = {
                        "type": "market",
                        "assets_ids": list(self.token_to_asset.keys())
                    }
                    await ws.send(json.dumps(sub_msg))
                    self._subscription_count += 1
                    logger.info(f"Polymarket subscription sent for tokens: {list(self.token_to_asset.keys())}")
                    
                    try:
                        async for msg in ws:
                            if not self.running:
                                break
                            if msg == "PONG" or msg == '{"pong":1}' or msg == 'pong':
                                logger.debug("Received Poly WS PONG response")
                                continue
                            try:
                                data = json.loads(msg)
                                if isinstance(data, dict) and "pong" in data:
                                    logger.debug(f"Received Poly WS JSON pong: {data}")
                                    continue
                                
                                if isinstance(data, list):
                                    for item in data:
                                        if isinstance(item, dict):
                                            self.process_ws_message(item)
                                elif isinstance(data, dict):
                                    self.process_ws_message(data)
                            except json.JSONDecodeError:
                                pass
                    finally:
                        if self.connected:
                            self.reconnect_count += 1
                            self.client_reconnects += 1
                        self.connected = False
                        self.ws = None
                            
            except websockets.exceptions.ConnectionClosed as cc:
                if self.connected:
                    self.reconnect_count += 1
                    if cc.code in (1000, 1001, 1005):
                        self.client_reconnects += 1
                        sleep_time = 0.1
                        backoff = 1
                    else:
                        self.server_disconnects += 1
                        sleep_time = backoff
                        backoff = min(backoff * 2, 60)
                else:
                    sleep_time = backoff
                    backoff = min(backoff * 2, 60)
                self.connected = False
                self.ws = None
                logger.error(f"Poly WS connection closed: code={cc.code}, reason='{cc.reason}'")
                logger.info(f"Reconnecting in {sleep_time} seconds...")
                await asyncio.sleep(sleep_time)
            except Exception as e:
                if self.connected:
                    self.reconnect_count += 1
                    self.server_disconnects += 1
                self.connected = False
                self.ws = None
                logger.error(f"Poly WS error: {e}")
                logger.info(f"Reconnecting in {backoff} seconds...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def get_feed_metrics(self) -> dict:
        """Return a comprehensive snapshot of feed health metrics for /api/metrics."""
        now = time.time()

        # Compute messages-per-minute over the rolling 60s window
        elapsed = now - self._window_start_ts
        if elapsed >= 60.0:
            msgs_per_min = round(self._msg_count_window / (elapsed / 60.0), 2)
            # Reset window
            self._msg_count_window = 0
            self._window_start_ts = now
        else:
            msgs_per_min = round(self._msg_count_window / max(elapsed, 1) * 60, 2)

        last_msg_age = round(now - self._last_msg_ts, 1) if self._last_msg_ts > 0 else None

        # Per-asset snapshot
        assets = {}
        for asset in ["BTC", "ETH", "SOL"]:
            state = self.get_market_state(asset)
            if state:
                assets[asset] = {
                    "staleness_seconds": state["staleness_seconds"],
                    "is_stale": state["is_stale"],
                    "feed_status": "OK" if self.connected else "ERR",
                    "yes_price": state["yes_price"],
                    "spread": state["spread"],
                    "time_to_resolution_seconds": state["time_to_resolution_seconds"],
                }
            else:
                assets[asset] = {
                    "staleness_seconds": None,
                    "is_stale": True,
                    "feed_status": "NO_MARKET",
                    "yes_price": None,
                    "spread": None,
                    "time_to_resolution_seconds": None,
                }

        return {
            "connected": self.connected,
            "reconnect_count": self.reconnect_count,
            "client_reconnects": self.client_reconnects,
            "server_disconnects": self.server_disconnects,
            "total_messages": self._msg_count,
            "messages_per_minute": msgs_per_min,
            "subscription_count": self._subscription_count,
            "last_message_age_seconds": last_msg_age,
            "tracked_tokens": len(self.token_to_asset),
            "stale_threshold_seconds": 10.0,
            "assets": assets,
        }

