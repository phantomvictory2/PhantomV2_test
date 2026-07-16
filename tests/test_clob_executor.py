import os
import asyncio

os.environ["PAPER_MODE"] = "true"

import sys
sys.path.insert(0, ".")
from clob_executor import ClobExecutor, FillResult


def test_paper_fill():
    ex = ClobExecutor()
    assert ex.paper_mode is True
    assert ex.health() == {"mode": "paper", "ready": True}

    fr: FillResult = asyncio.run(
        ex.place_order(token_id="0xtok", requested_price=0.97, size_usdc=10.0, asset="BTC")
    )
    assert fr.filled is True
    assert fr.status == "PAPER"
    assert fr.fill_price == 0.97          # paper fills at requested price
    assert fr.filled_usdc == 10.0
    assert fr.slippage_usdc == 0.0
    assert fr.is_paper is True
    # journal row excludes the raw blob
    row = fr.as_row()
    assert "raw" not in row
    assert row["requested_price"] == 0.97


def test_fee_model_hook():
    os.environ["POLY_FEE_BPS"] = "20"     # 0.2%
    try:
        ex = ClobExecutor()
        fr = asyncio.run(ex.place_order(token_id="t", requested_price=0.95, size_usdc=100.0))
        assert abs(fr.fee_usdc - 0.20) < 1e-9   # 100 * 20bps = $0.20
    finally:
        os.environ["POLY_FEE_BPS"] = "0"


if __name__ == "__main__":
    test_paper_fill()
    test_fee_model_hook()
    print("clob_executor paper tests passed")
