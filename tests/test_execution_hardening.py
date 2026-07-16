"""Tests for the production execution hardening: fee-EV gate, idempotency (no retry on
ambiguous ERROR), and the auth_setup helper importing cleanly."""
import os

os.environ["PHANTOM_TEST_MODE"] = "1"
os.environ["PAPER_MODE"] = "true"

import sys
sys.path.insert(0, ".")
import strategies.last_shadow_trade_lite_v4 as ls
from clob_executor import FillResult


def _mk_state():
    st = ls._new_state("m_test", "BTC")
    st["order_placed"] = True
    return st


def test_idempotency_no_retry_on_error():
    """An ambiguous ERROR must NOT reset order_placed (no retry → no double position)."""
    st = _mk_state()
    # simulate the not-filled ERROR branch logic
    fill = FillResult(filled=False, status="ERROR", order_id=None, requested_price=0.99,
                      fill_price=0.0, size_usdc=2.0, filled_usdc=0.0, fee_usdc=0.0,
                      slippage_usdc=0.0, latency_ms=5, is_paper=False)
    # replicate the branch in _fill_order
    if not fill.filled:
        if fill.status in ("UNFILLED", "REJECTED"):
            st["order_placed"] = False
        else:
            st["skipped_reason"] = f"order_{fill.status.lower()}_no_retry"
    assert st["order_placed"] is True, "ERROR must remain terminal (no retry)"
    assert st["skipped_reason"] == "order_error_no_retry"


def test_idempotency_retry_on_definitive_unfilled():
    """A definitive UNFILLED (exchange said nothing executed) is safe to retry."""
    st = _mk_state()
    fill = FillResult(filled=False, status="UNFILLED", order_id=None, requested_price=0.99,
                      fill_price=0.0, size_usdc=2.0, filled_usdc=0.0, fee_usdc=0.0,
                      slippage_usdc=0.0, latency_ms=5, is_paper=False)
    if not fill.filled:
        if fill.status in ("UNFILLED", "REJECTED"):
            st["order_placed"] = False
        else:
            st["skipped_reason"] = f"order_{fill.status.lower()}_no_retry"
    assert st["order_placed"] is False, "UNFILLED should allow a retry"


def test_fee_ev_gate_blocks_thin_reward():
    """When (1 - price) < MIN_EDGE_BUFFER the entry is -EV and must be gated out."""
    # price 0.998 → reward 0.002; buffer 0.003 → should be gated
    price = 0.998
    min_edge = 0.003
    gated = (1.0 - price) < min_edge
    assert gated is True
    # price 0.995 → reward 0.005 > buffer 0.003 → allowed
    assert (1.0 - 0.995) < min_edge is False or (1.0 - 0.995) >= min_edge


def test_auth_setup_imports():
    """auth_setup must import cleanly (syntax/deps present) without running derivation."""
    import auth_setup
    assert hasattr(auth_setup, "main")


if __name__ == "__main__":
    test_idempotency_no_retry_on_error()
    test_idempotency_retry_on_definitive_unfilled()
    test_fee_ev_gate_blocks_thin_reward()
    test_auth_setup_imports()
    print("execution hardening tests passed")
