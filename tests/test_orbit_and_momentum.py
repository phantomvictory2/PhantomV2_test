"""
Unit tests for the two signal-driven strategies added/restored alongside
PHANTOM_ONE_V1:

  • ORBIT_A_240 (restored)      — tick-momentum breakout, TTR 90-210
  • PHANTOM_MOMENTUM_V1 (new)   — replaces ORBIT_A_260; wallet-study entry
                                  band 0.55-0.70 + momentum + spot agreement
"""

from unittest.mock import patch

from strategies.orbit_a_240 import classify as classify_orbit
from strategies.phantom_momentum_v1 import classify as classify_momo


BASE = {
    "market_id": "0xabc",
    "asset": "BTC",
    "spread": 0.02,
    "staleness_seconds": 1.0,
    "liquidity_usdc": 20000.0,
}


def _state(**kw):
    s = dict(BASE)
    s.update(kw)
    return s


# ── ORBIT_A_240 ───────────────────────────────────────────────────────────────

def test_orbit_fires_on_yes_burst_in_band():
    sig = classify_orbit(_state(yes_price=0.66, no_price=0.34,
                                prev_yes_price=0.62, time_to_resolution_seconds=150))
    assert sig["grade"] == "A"
    assert sig["direction"] == "BUY_YES"
    assert sig["momentum"] == 0.04


def test_orbit_fires_on_no_burst_in_band():
    # YES dropping 0.44 -> 0.40 == NO burst; NO at 0.60 is in the band.
    sig = classify_orbit(_state(yes_price=0.40, no_price=0.60,
                                prev_yes_price=0.44, time_to_resolution_seconds=150))
    assert sig["grade"] == "A"
    assert sig["direction"] == "BUY_NO"


def test_orbit_skips_small_move():
    sig = classify_orbit(_state(yes_price=0.63, no_price=0.37,
                                prev_yes_price=0.62, time_to_resolution_seconds=150))
    assert sig["grade"] == "SKIP"
    assert sig["skip_reason"] == "no_momentum_burst"


def test_orbit_skips_outside_ttr():
    for ttr in (60, 250):
        sig = classify_orbit(_state(yes_price=0.66, no_price=0.34,
                                    prev_yes_price=0.62, time_to_resolution_seconds=ttr))
        assert sig["grade"] == "SKIP"


def test_orbit_skips_burst_outside_band():
    # Burst but leader already at 0.85 — above the 0.78 band ceiling.
    sig = classify_orbit(_state(yes_price=0.85, no_price=0.15,
                                prev_yes_price=0.80, time_to_resolution_seconds=150))
    assert sig["grade"] == "SKIP"


# ── PHANTOM_MOMENTUM_V1 ───────────────────────────────────────────────────────

def _momo(margin_pct, **kw):
    with patch("strategies.phantom_momentum_v1._spot_margin_pct", return_value=margin_pct):
        return classify_momo(_state(**kw))


def test_momo_fires_yes_leader_with_agreement():
    sig = _momo(0.06, yes_price=0.62, no_price=0.38,
                prev_yes_price=0.60, time_to_resolution_seconds=150)
    assert sig["grade"] == "A"
    assert sig["direction"] == "BUY_YES"


def test_momo_fires_no_leader_with_agreement():
    sig = _momo(-0.06, yes_price=0.38, no_price=0.62,
                prev_yes_price=0.40, time_to_resolution_seconds=150)
    assert sig["grade"] == "A"
    assert sig["direction"] == "BUY_NO"


def test_momo_skips_leader_above_band():
    sig = _momo(0.06, yes_price=0.75, no_price=0.25,
                prev_yes_price=0.72, time_to_resolution_seconds=150)
    assert sig["grade"] == "SKIP"
    assert sig["skip_reason"] == "leader_above_band"


def test_momo_skips_no_clear_leader():
    sig = _momo(0.06, yes_price=0.52, no_price=0.48,
                prev_yes_price=0.50, time_to_resolution_seconds=150)
    assert sig["grade"] == "SKIP"
    assert sig["skip_reason"] == "no_clear_leader"


def test_momo_skips_without_leader_momentum():
    # Leader in band but flat/fading tick.
    sig = _momo(0.06, yes_price=0.62, no_price=0.38,
                prev_yes_price=0.62, time_to_resolution_seconds=150)
    assert sig["grade"] == "SKIP"
    assert sig["skip_reason"] == "no_leader_momentum"


def test_momo_skips_spot_disagreement():
    sig = _momo(-0.06, yes_price=0.62, no_price=0.38,
                prev_yes_price=0.60, time_to_resolution_seconds=150)
    assert sig["grade"] == "SKIP"
    assert sig["skip_reason"] == "spot_disagrees_with_leader"


def test_momo_skips_low_displacement():
    sig = _momo(0.01, yes_price=0.62, no_price=0.38,
                prev_yes_price=0.60, time_to_resolution_seconds=150)
    assert sig["grade"] == "SKIP"
    assert sig["skip_reason"] == "displacement_below_gate"


def test_momo_skips_no_spot_feed():
    sig = _momo(None, yes_price=0.62, no_price=0.38,
                prev_yes_price=0.60, time_to_resolution_seconds=150)
    assert sig["grade"] == "SKIP"
    assert sig["skip_reason"] == "no_spot_margin"


def test_momo_skips_outside_ttr():
    for ttr in (60, 250):
        sig = _momo(0.06, yes_price=0.62, no_price=0.38,
                    prev_yes_price=0.60, time_to_resolution_seconds=ttr)
        assert sig["grade"] == "SKIP"
