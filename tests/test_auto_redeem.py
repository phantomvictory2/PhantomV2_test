"""Tests for auto-redeem logic (paper mode — no real CLOB calls)."""
import os
import asyncio

os.environ["PHANTOM_TEST_MODE"] = "1"
os.environ["PAPER_MODE"] = "true"

import sys
sys.path.insert(0, ".")
from clob_redeemer import auto_redeem, ClobRedeemer


def test_paper_mode_returns_true():
    """Paper mode always returns True (nothing to redeem, no creds needed)."""
    result = asyncio.run(auto_redeem(condition_id="0xabc", asset="BTC", pnl=0.63))
    assert result is True


def test_missing_condition_id_returns_false():
    """No condition_id means nothing to redeem — returns False gracefully."""
    result = asyncio.run(auto_redeem(condition_id=None, asset="ETH", pnl=0.50))
    assert result is False


def test_empty_condition_id_returns_false():
    """Empty string condition_id is treated the same as None."""
    result = asyncio.run(auto_redeem(condition_id="", asset="SOL", pnl=0.40))
    assert result is False


def test_redeemer_paper_returns_true():
    """In paper mode, ClobRedeemer.redeem() logs and returns True without any network."""
    r = ClobRedeemer()
    result = asyncio.run(r.redeem(condition_id="0xdef", asset="BTC", pnl=1.0))
    assert result is True


def test_redeemer_live_not_implemented_returns_false():
    """Live redemption is on-chain (not via CLOB) and not wired yet — must return False
    (surfacing a warning), never a fake success that hides unredeemed winnings."""
    os.environ["PAPER_MODE"] = "false"
    try:
        r = ClobRedeemer()
        result = asyncio.run(r.redeem(condition_id="0xdef", asset="BTC", pnl=1.0))
        assert result is False
    finally:
        os.environ["PAPER_MODE"] = "true"


if __name__ == "__main__":
    test_paper_mode_returns_true()
    test_missing_condition_id_returns_false()
    test_empty_condition_id_returns_false()
    test_redeemer_paper_returns_true()
    test_redeemer_live_not_implemented_returns_false()
    print("auto-redeem tests passed")
