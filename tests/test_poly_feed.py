import asyncio
import logging
import time
from poly_feed import PolyFeed

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

async def test_poly_feed():
    feed = PolyFeed()
    
    now_ms = int(time.time() * 1000)
    
    # 1. Register a BTC 15-min market
    # Let's say it resolves in 15 minutes (900 seconds)
    resolution_time_ms = now_ms + (900 * 1000)
    
    feed.set_active_market(
        asset="BTC",
        market_id="0xmarket123_btc_up_down",
        token_id="0xtoken_btc_yes",
        resolution_time_ms=resolution_time_ms,
        liquidity_usdc=15000.0,
        market_open_time=now_ms
    )
    
    # 2. Simulate WS message updating the orderbook (Legacy / Simulated format)
    logger.info("\n--- SIMULATING WS MESSAGE (LEGACY) ---")
    mock_ws_data = {
        "asset_id": "0xtoken_btc_yes",
        "yes_bid": "0.52",
        "yes_ask": "0.55"
    }
    feed.process_ws_message(mock_ws_data)
    
    # 3. Fetch state immediately
    state_immediate = feed.get_market_state("BTC")
    logger.info("MARKET STATE (IMMEDIATE):")
    for k, v in state_immediate.items():
        if k != "resolution_time_ms":
            logger.info(f"  {k}: {v}")
            
    assert state_immediate["best_bid"] == 0.52
    assert state_immediate["best_ask"] == 0.55
    assert state_immediate["spread"] == 0.03

    # 4. Simulate WS message with the real CLOB book format (bids ascending, asks descending)
    logger.info("\n--- SIMULATING WS MESSAGE (REAL CLOB BOOK) ---")
    mock_clob_data = {
        "asset_id": "0xtoken_btc_yes",
        "event_type": "book",
        "bids": [
            {"price": "0.01", "size": "1000"},
            {"price": "0.02", "size": "500"},
            {"price": "0.24", "size": "100"}
        ],
        "asks": [
            {"price": "0.99", "size": "2000"},
            {"price": "0.98", "size": "800"},
            {"price": "0.25", "size": "150"}
        ]
    }
    feed.process_ws_message(mock_clob_data)
    
    state_clob = feed.get_market_state("BTC")
    logger.info("MARKET STATE (CLOB BOOK):")
    for k, v in state_clob.items():
        if k != "resolution_time_ms":
            logger.info(f"  {k}: {v}")
            
    assert state_clob["best_bid"] == 0.24
    assert state_clob["best_ask"] == 0.25
    assert state_clob["yes_price"] == 0.25
    assert state_clob["no_price"] == 0.76  # 1.0 - 0.24
    assert state_clob["spread"] == 0.01

    # 5. Wait 12 seconds to test staleness calculation
    logger.info("\n--- WAITING 12 SECONDS TO TEST STALENESS ---")
    await asyncio.sleep(12)
    
    state_delayed = feed.get_market_state("BTC")
    logger.info("MARKET STATE (AFTER 12s):")
    for k, v in state_delayed.items():
        if k != "resolution_time_ms":
            logger.info(f"  {k}: {v}")
            
    assert state_delayed["is_stale"] is True
    assert state_delayed["staleness_seconds"] >= 11.5

if __name__ == "__main__":
    asyncio.run(test_poly_feed())
