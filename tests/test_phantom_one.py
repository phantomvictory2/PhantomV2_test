"""
Unit tests for PHANTOM-1 (strategies/phantom_one_v1.py) entry gates and sizing.

evaluate_entry() is a pure function, so every gate from the seven-wallet study
is testable without feeds, DB, or executor:
  time window (TTR 180-270), price band 0.55-0.70, hard cap 0.85,
  volatility gate (>=5bp displacement), direction agreement, staleness.
"""

from datetime import datetime, timezone

from strategies.phantom_one_v1 import evaluate_entry, _position_size, CONFIG


# ── happy paths ───────────────────────────────────────────────────────────────

def test_enters_yes_leader_in_band_with_agreeing_displacement():
    side, price = evaluate_entry(ttr=240, yes_p=0.62, no_p=0.38, margin_pct=0.08)
    assert side == "BUY_YES"
    assert price == 0.62


def test_enters_no_leader_in_band_with_agreeing_displacement():
    side, price = evaluate_entry(ttr=200, yes_p=0.40, no_p=0.60, margin_pct=-0.06)
    assert side == "BUY_NO"
    assert price == 0.60


def test_band_edges_inclusive():
    assert evaluate_entry(240, 0.55, 0.45, 0.10)[0] == "BUY_YES"
    assert evaluate_entry(240, 0.70, 0.30, 0.10)[0] == "BUY_YES"


def test_ttr_edges_inclusive():
    assert evaluate_entry(180, 0.62, 0.38, 0.10)[0] == "BUY_YES"
    assert evaluate_entry(270, 0.62, 0.38, 0.10)[0] == "BUY_YES"


# ── time gate: only 30–120s elapsed (TTR 180–270) ────────────────────────────

def test_rejects_too_early():
    side, reason = evaluate_entry(ttr=290, yes_p=0.62, no_p=0.38, margin_pct=0.10)
    assert side is None and reason == "outside_entry_window"


def test_rejects_too_late():
    side, reason = evaluate_entry(ttr=100, yes_p=0.62, no_p=0.38, margin_pct=0.10)
    assert side is None and reason == "outside_entry_window"


# ── price gates ───────────────────────────────────────────────────────────────

def test_rejects_no_clear_leader():
    side, reason = evaluate_entry(240, 0.52, 0.48, 0.10)
    assert side is None and reason == "no_clear_leader"


def test_rejects_leader_above_band():
    side, reason = evaluate_entry(240, 0.78, 0.22, 0.10)
    assert side is None and reason == "leader_above_band"


def test_rejects_leader_above_hard_cap():
    side, reason = evaluate_entry(240, 0.92, 0.08, 0.10)
    assert side is None and reason == "leader_above_hard_cap"


def test_rejects_missing_prices():
    side, reason = evaluate_entry(240, None, 0.40, 0.10)
    assert side is None and reason == "no_price_data"


# ── volatility gate: >=5bp displacement, direction agreement ──────────────────

def test_rejects_displacement_below_gate():
    # 3bp move — below the 5bp gate
    side, reason = evaluate_entry(240, 0.62, 0.38, margin_pct=0.03)
    assert side is None and reason == "displacement_below_gate"


def test_rejects_missing_spot_margin():
    side, reason = evaluate_entry(240, 0.62, 0.38, margin_pct=None)
    assert side is None and reason == "no_spot_margin"


def test_rejects_spot_disagreeing_with_leader():
    # Market leans UP but spot is BELOW the window open — no trade.
    side, reason = evaluate_entry(240, 0.62, 0.38, margin_pct=-0.10)
    assert side is None and reason == "spot_disagrees_with_leader"

    # Market leans DOWN but spot is ABOVE the window open — no trade.
    side, reason = evaluate_entry(240, 0.38, 0.62, margin_pct=0.10)
    assert side is None and reason == "spot_disagrees_with_leader"


def test_displacement_gate_exactly_at_threshold_passes():
    # exactly 5bp == 0.05% passes (>= semantics via abs >= min)
    side, _ = evaluate_entry(240, 0.62, 0.38, margin_pct=0.05)
    assert side == "BUY_YES"


# ── staleness gate ────────────────────────────────────────────────────────────

def test_rejects_stale_feed():
    side, reason = evaluate_entry(240, 0.62, 0.38, 0.10, staleness_s=8.0)
    assert side is None and reason == "stale_feed"


def test_fresh_feed_passes():
    side, _ = evaluate_entry(240, 0.62, 0.38, 0.10, staleness_s=1.0)
    assert side == "BUY_YES"


# ── session-weighted sizing ───────────────────────────────────────────────────

def _ts_at_utc_hour(hour: int) -> float:
    now = datetime.now(timezone.utc)
    return now.replace(hour=hour, minute=30, second=0, microsecond=0).timestamp()


def test_full_size_in_prime_block():
    for h in (0, 1, 2, 3):
        assert _position_size(_ts_at_utc_hour(h)) == CONFIG["size_full_usdc"]


def test_half_size_outside_prime_block():
    for h in (4, 10, 14, 23):
        assert _position_size(_ts_at_utc_hour(h)) == round(CONFIG["size_full_usdc"] / 2.0, 2)
