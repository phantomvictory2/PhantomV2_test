"""
orbit_a_240.py — ORBIT A 240: tick-momentum breakout (signal-driven, restored).

The original ORBIT_A_240 classifier was removed in the Production-v1 cleanup;
this is a faithful reconstruction from the scaffolding that survived it:

  • risk_engine.VALID_TTR_WINDOWS["ORBIT_A_240"] = (20, 210) TTR
  • signal_validator valid_windows["ORBIT_A_240"] = (90, 240) TTR,
    weekday elapsed >= 90s (weekend >= 150s)
  • monitor._STRATEGY_CONFIGS: take-profit 7%, stop-loss 12% of entry,
    max hold 90s
  • daily_report.md tuning notes: "optimal entry target closer to 0.66",
    "max hold time closer to 63.0s"
  • risk sizing: fixed $30 per trade (CHECK 7)

Entry logic: a momentum burst on the Polymarket book mid-window. When one
side's price jumps >= MOMENTUM_MIN between ticks while trading inside the
ENTRY_BAND, buy that side. Exits are handled entirely by the position
monitor (TP/SL/max-hold) — classify() only produces the entry signal.

Flow: poly_feed tick -> signal_engine.classify -> (validator bypassed by
design for ORBIT) -> risk_engine (TTR 20-210, spread, liquidity, $30) ->
executor paper/live fill -> monitor exits.
"""

import logging
from .utils import create_base_signal_direct

logger = logging.getLogger(__name__)

STRATEGY = "ORBIT_A_240"

CONFIG = {
    "enabled": True,
    "momentum_min": 0.02,     # tick-to-tick jump that defines a burst
    "entry_band_low": 0.55,   # only chase momentum from a confirmed leader...
    "entry_band_high": 0.78,  # ...but never into overpriced territory
    "ttr_min": 90,            # intersection of validator (90-240) and risk (20-210)
    "ttr_max": 210,
}


def _skip(poly_state: dict, reason: str) -> dict:
    sig = create_base_signal_direct(STRATEGY, poly_state, "NONE")
    sig["grade"] = "SKIP"
    sig["skip_reason"] = reason
    return sig


def classify(poly_state: dict) -> dict:
    if not CONFIG["enabled"]:
        return _skip(poly_state, "strategy_disabled")

    ttr = poly_state.get("time_to_resolution_seconds", 0)
    if not (CONFIG["ttr_min"] <= ttr <= CONFIG["ttr_max"]):
        return _skip(poly_state, f"TTR {ttr:.0f}s outside {CONFIG['ttr_min']}-{CONFIG['ttr_max']}s")

    yes = poly_state.get("yes_price")
    no = poly_state.get("no_price")
    prev_yes = poly_state.get("prev_yes_price")
    if yes is None or no is None or prev_yes is None:
        return _skip(poly_state, "missing_price_data")

    dy = round(yes - prev_yes, 4)
    lo, hi = CONFIG["entry_band_low"], CONFIG["entry_band_high"]

    # Upward YES burst: YES gaining and trading in the entry band.
    if dy >= CONFIG["momentum_min"] and lo <= yes <= hi:
        sig = create_base_signal_direct(STRATEGY, poly_state, "BUY_YES")
        sig["grade"] = "A"
        sig["momentum"] = abs(dy)
        return sig

    # Downward YES burst == upward NO burst: NO gaining and in the band.
    if dy <= -CONFIG["momentum_min"] and lo <= no <= hi:
        sig = create_base_signal_direct(STRATEGY, poly_state, "BUY_NO")
        sig["grade"] = "A"
        sig["momentum"] = abs(dy)
        return sig

    return _skip(poly_state, "no_momentum_burst")
