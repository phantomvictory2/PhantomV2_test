import uuid
import time

def create_base_signal_direct(strategy_type: str, poly_state: dict, direction: str) -> dict:
    if direction == "BUY_NO":
        prev = poly_state.get("prev_no_price", 0.50)
        curr = poly_state.get("no_price", 0.50)
    else:
        prev = poly_state.get("prev_yes_price", 0.50)
        curr = poly_state.get("yes_price", 0.50)
        
    mag_pct = round((curr - prev) / prev * 100, 4) if prev > 0 else 0.0
    mag_usd = round(curr - prev, 4)
    
    return {
        "signal_id": str(uuid.uuid4()),
        "strategy_type": strategy_type,
        "asset": poly_state["asset"],
        "direction": direction,
        "side": direction,
        "magnitude_pct": abs(mag_pct),
        "magnitude_usd": mag_usd,
        "duration_seconds": 0,
        "poly_staleness_seconds": poly_state.get("staleness_seconds"),
        "spread": poly_state.get("spread"),
        "yes_price": poly_state.get("yes_price"),
        "no_price": poly_state.get("no_price"),
        "market_id": poly_state.get("market_id"),
        "time_to_resolution_seconds": poly_state.get("time_to_resolution_seconds"),
        "liquidity_usdc": poly_state.get("liquidity_usdc"),
        "velocity_count": 1,
        "entry_mode": "SINGLE",
        "dca_config": None,
        "classified_at_ms": int(time.time() * 1000)
    }
