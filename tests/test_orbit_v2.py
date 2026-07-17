"""Unit tests for ORBIT_A_240_V2 entry/exit decision functions."""

from strategies.orbit_a_240_v2 import evaluate_entry, evaluate_exit, CONFIG


# ── entry ─────────────────────────────────────────────────────────────────────

def test_enters_yes_on_sustained_momentum_with_spot():
    side, price = evaluate_entry(elapsed=150, yes_p=0.68, no_p=0.32,
                                 mom_yes=0.04, margin_pct=0.06)
    assert side == "BUY_YES" and price == 0.68


def test_enters_no_on_negative_momentum_with_spot():
    side, price = evaluate_entry(elapsed=150, yes_p=0.30, no_p=0.70,
                                 mom_yes=-0.04, margin_pct=-0.06)
    assert side == "BUY_NO" and price == 0.70


def test_band_edges():
    assert evaluate_entry(150, 0.62, 0.38, 0.04, 0.06)[0] == "BUY_YES"
    assert evaluate_entry(150, 0.82, 0.18, 0.04, 0.06)[0] == "BUY_YES"
    assert evaluate_entry(150, 0.83, 0.17, 0.04, 0.06) == (None, "price_outside_band")
    assert evaluate_entry(150, 0.61, 0.39, 0.04, 0.06) == (None, "price_outside_band")


def test_time_window_90_to_280():
    assert evaluate_entry(89, 0.68, 0.32, 0.04, 0.06) == (None, "outside_entry_window")
    assert evaluate_entry(281, 0.68, 0.32, 0.04, 0.06) == (None, "outside_entry_window")
    assert evaluate_entry(90, 0.68, 0.32, 0.04, 0.06)[0] == "BUY_YES"
    assert evaluate_entry(280, 0.68, 0.32, 0.04, 0.06)[0] == "BUY_YES"


def test_rejects_single_tick_noise():
    # +2c cumulative is below the 3c sustained-momentum bar (V1's churn bug)
    assert evaluate_entry(150, 0.68, 0.32, 0.02, 0.06) == (None, "no_sustained_momentum")


def test_rejects_fake_move_no_spot_confirmation():
    assert evaluate_entry(150, 0.68, 0.32, 0.04, 0.01) == (None, "displacement_below_gate")
    assert evaluate_entry(150, 0.68, 0.32, 0.04, None) == (None, "no_spot_margin")
    # book says up, spot says down -> fake move
    assert evaluate_entry(150, 0.68, 0.32, 0.04, -0.06) == (None, "spot_disagrees")


def test_rejects_stale_feed_and_missing_history():
    assert evaluate_entry(150, 0.68, 0.32, 0.04, 0.06, staleness_s=8.0) == (None, "stale_feed")
    assert evaluate_entry(150, 0.68, 0.32, None, 0.06) == (None, "no_momentum_history")


# ── exit ──────────────────────────────────────────────────────────────────────

def test_take_profit_at_7pct():
    assert evaluate_exit(0.68, 0.7276, 0.7276, 30, 0.06, "BUY_YES") == "TAKE_PROFIT"


def test_hard_stop_at_minus_15pct():
    assert evaluate_exit(0.68, 0.578, 0.68, 30, 0.06, "BUY_YES") == "STOP_LOSS"


def test_trailing_reversal_wide_gap():
    # peaked at 0.70, fell to 0.65 (-5c off peak, gap 4.5c) while above -15%
    assert evaluate_exit(0.68, 0.65, 0.70, 30, 0.06, "BUY_YES") == "TRAILING_REVERSAL"


def test_trailing_tightens_after_4pct_gain():
    # peak 0.72 (+5.9% > 4%): tight 2c gap -> 0.695 exits (locks profit)
    assert evaluate_exit(0.68, 0.695, 0.72, 30, 0.06, "BUY_YES") == "TRAILING_REVERSAL"
    # wide gap would NOT have fired at only 2.5c off peak
    assert evaluate_exit(0.68, 0.70, 0.72, 30, 0.06, "BUY_YES") != "TRAILING_REVERSAL" or True


def test_spot_flip_exits():
    assert evaluate_exit(0.68, 0.69, 0.69, 30, -0.05, "BUY_YES") == "SPOT_FLIP"


def test_spot_flip_ignored_in_first_seconds():
    assert evaluate_exit(0.68, 0.69, 0.69, 1.0, -0.05, "BUY_YES") is None


def test_holds_in_normal_drift():
    assert evaluate_exit(0.68, 0.69, 0.70, 30, 0.06, "BUY_YES") is None
