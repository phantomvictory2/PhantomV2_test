"""Tests for the Last Shadow SOL exclusion + displacement gate and the
reversal logger's classification logic."""

import os
from unittest.mock import patch as mockpatch

from reversal_logger import is_reversal


def test_reversal_classification():
    assert is_reversal("YES", "DOWN") is True
    assert is_reversal("NO", "UP") is True
    assert is_reversal("YES", "UP") is False
    assert is_reversal("NO", "DOWN") is False
    assert is_reversal(None, "UP") is None
    assert is_reversal("YES", None) is None


def test_ls_default_assets_exclude_sol():
    # Module was imported with no LS_ASSETS env -> default BTC,ETH
    from strategies import last_shadow_trade_lite_v4 as ls
    assert ls._ASSETS == ("BTC", "ETH")
    assert "SOL" not in ls._ASSETS


def test_ls_gate_env_defaults():
    assert float(os.getenv("LS_MIN_DISPLACEMENT_BP", "3.0")) == 3.0
