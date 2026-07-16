"""
phantom_momentum_v1.py — PHANTOM MOMENTUM V1 (replaces ORBIT_A_260).

Signal-driven mid-window momentum, upgraded with the entry evidence from the
July 2026 seven-wallet study. It keeps ORBIT_A_260's exit mechanics (monitor:
take-profit 7%, fixed 0.50 stop, 90s max hold) but replaces its entry with
the bands that actually beat their implied probability in the wallet data:

  • Leader must trade 0.55–0.70 — the only price band profitable for 5 of 7
    top wallets (win rates 0.61–0.90 vs ~0.62 implied).
  • Momentum confirmation: the leading side gained >= MOMENTUM_MIN on the
    latest tick (buying the side that is being bid, not fading it).
  • Spot agreement: Coinbase spot has moved >= MIN_DISPLACEMENT_BP from the
    5-min window open in the direction of the leader (>10bp windows carried
    63–87% of the profitable wallets' PnL; displacement is the ex-ante proxy).
  • TTR 90–200s (mid-window: risk_engine window is (40, 200), validator
    requires >= 90 elapsed weekday / >= 60s TTR minimum for TP room).

Unlike PHANTOM_ONE_V1 (self-contained, holds to resolution), this strategy
goes through the full signal path — validator, risk engine, executor — and is
exited early by the monitor via TP/SL/max-hold. The two are intentionally
complementary: same evidence base, different exit style, so paper results
will show whether early profit-taking beats hold-to-resolution.
"""

import logging
from .utils import create_base_signal_direct

logger = logging.getLogger(__name__)

STRATEGY = "PHANTOM_MOMENTUM_V1"

CONFIG = {
    "enabled": True,
    "band_low": 0.55,             # leader entry band (wallet-study sweet spot)
    "band_high": 0.70,
    "momentum_min": 0.01,         # leader must be gaining on the latest tick
    "min_displacement_bp": 3.0,   # spot moved >= 3bp from window open, agreeing
    "ttr_min": 90,
    "ttr_max": 200,
}


def _skip(poly_state: dict, reason: str) -> dict:
    sig = create_base_signal_direct(STRATEGY, poly_state, "NONE")
    sig["grade"] = "SKIP"
    sig["skip_reason"] = reason
    return sig


def _spot_margin_pct(asset: str):
    """Displacement of spot from the current 5-min window open, in percent
    (SpotFeed.get_margin). None if the feed isn't ready. Imported lazily so
    unit tests can exercise classify() without the dashboard stack."""
    try:
        from dashboard import state_provider
        sf = getattr(state_provider, "spot_feed", None)
        if sf is None:
            return None
        m = sf.get_margin(asset)
        return m["margin_pct"] if m else None
    except Exception:
        return None


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

    leader_is_yes = yes >= no
    leader_price = yes if leader_is_yes else no
    lo, hi = CONFIG["band_low"], CONFIG["band_high"]
    if leader_price < lo:
        return _skip(poly_state, "no_clear_leader")
    if leader_price > hi:
        return _skip(poly_state, "leader_above_band")

    # Momentum confirmation: the leader gained on the latest tick.
    dy = round(yes - prev_yes, 4)
    leader_gain = dy if leader_is_yes else -dy
    if leader_gain < CONFIG["momentum_min"]:
        return _skip(poly_state, "no_leader_momentum")

    # Volatility/direction gate: spot displacement agrees with the leader.
    margin_pct = _spot_margin_pct(poly_state.get("asset", "BTC"))
    if margin_pct is None:
        return _skip(poly_state, "no_spot_margin")
    if abs(margin_pct) < CONFIG["min_displacement_bp"] / 100.0:
        return _skip(poly_state, "displacement_below_gate")
    if (margin_pct > 0) != leader_is_yes:
        return _skip(poly_state, "spot_disagrees_with_leader")

    direction = "BUY_YES" if leader_is_yes else "BUY_NO"
    sig = create_base_signal_direct(STRATEGY, poly_state, direction)
    sig["grade"] = "A"
    sig["momentum"] = abs(leader_gain)
    return sig
