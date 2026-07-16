"""
clob_executor.py — Polymarket CLOB execution with cost recording.

One uniform entry point, `ClobExecutor.place_order(...)`, used by strategies to enter
a position. Behaviour depends on PAPER_MODE:

  • PAPER_MODE=true  → simulates an immediate fill at the requested price and models the
    (currently zero) Polymarket trading fee. No network, no keys needed.
  • PAPER_MODE=false → posts a real marketable order via py-clob-client and records the
    ACTUAL fill price, fees, and slippage.

The point of Phase 1 is to capture, per trade: requested price vs actual fill, fee, and
slippage cost — so we can prove whether a strategy's paper edge survives real execution
costs BEFORE scaling. Every field on FillResult is written to the execution journal.

Credentials come only from environment variables (see .env.example). Nothing is hardcoded.
"""

import os
import time
import asyncio
import logging
from dataclasses import dataclass, asdict
from typing import Optional

logger = logging.getLogger("ClobExecutor")


@dataclass
class FillResult:
    filled: bool
    status: str                 # FILLED | PARTIAL | UNFILLED | REJECTED | ERROR | PAPER
    order_id: Optional[str]
    requested_price: float      # the price we expected to pay (the ask we saw)
    fill_price: float           # the price we actually paid (== requested in paper)
    size_usdc: float            # notional we tried to buy
    filled_usdc: float          # notional actually filled
    fee_usdc: float             # explicit trading fee charged
    slippage_usdc: float        # (fill_price - requested_price)/requested_price * filled_usdc
    latency_ms: int
    is_paper: bool
    raw: Optional[dict] = None  # raw exchange response (live only), for first-trade verification

    def as_row(self) -> dict:
        d = asdict(self)
        d.pop("raw", None)      # don't persist the blob in the journal row
        return d


def _env(name: str, default=None):
    return os.getenv(name, default)


class ClobExecutor:
    """Thin, defensively-parsed wrapper over py-clob-client with a paper fallback."""

    def __init__(self):
        self.paper_mode = str(_env("PAPER_MODE", "true")).lower() == "true"
        self._client = None
        self._init_error: Optional[str] = None

    # ── client lifecycle ────────────────────────────────────────────────────
    def _get_client(self):
        if self._client is not None:
            return self._client
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            host = _env("POLY_CLOB_API_URL", "https://clob.polymarket.com")
            key = _env("POLY_PRIVATE_KEY")
            funder = _env("POLY_FUNDER_ADDRESS")
            if not key or not funder:
                raise RuntimeError("POLY_PRIVATE_KEY / POLY_FUNDER_ADDRESS not set")

            creds = ApiCreds(
                api_key=_env("POLY_API_KEY"),
                api_secret=_env("POLY_API_SECRET"),
                api_passphrase=_env("POLY_API_PASSPHRASE"),
            )
            # Signature type governs how orders are signed for your wallet setup:
            #   0 = EOA (MetaMask signing for itself)
            #   1 = POLY_PROXY (legacy email/Magic-Link proxy)
            #   2 = GNOSIS_SAFE
            #   3 = POLY_1271 (modern deposit-wallet flow; funder = deposit wallet, ERC-1271)
            # MetaMask + a deposit wallet where funder != your key's address => type 3.
            sig_type = int(_env("SIGNATURE_TYPE", "3"))
            chain_id = int(_env("CHAIN_ID", "137"))
            client = ClobClient(host, key=key, chain_id=chain_id, creds=creds,
                                signature_type=sig_type, funder=funder)
            self._client = client
            logger.info(f"[CLOB] live client initialised (signature_type={sig_type}, chain_id={chain_id})")
            return client
        except Exception as e:
            self._init_error = str(e)
            logger.error(f"[CLOB] client init failed: {e}")
            return None

    def health(self) -> dict:
        """Non-trading readiness probe — safe to call at startup."""
        if self.paper_mode:
            return {"mode": "paper", "ready": True}
        client = self._get_client()
        return {"mode": "live", "ready": client is not None, "error": self._init_error}

    # ── fee model (paper) ───────────────────────────────────────────────────
    @staticmethod
    def _model_fee(price: float, size_usdc: float) -> float:
        # Polymarket CLOB trading fee is currently 0 bps. Kept as a hook so paper P&L
        # matches live if a fee is ever introduced. Override via POLY_FEE_BPS.
        fee_bps = float(_env("POLY_FEE_BPS", "0"))
        return round(size_usdc * fee_bps / 10_000.0, 6)

    # ── the one entry point ─────────────────────────────────────────────────
    async def place_order(self, *, token_id: str, requested_price: float,
                          size_usdc: float, asset: str = "?") -> FillResult:
        """
        Buy `size_usdc` of `token_id` (the YES or NO outcome token) at ~requested_price.
        Always a BUY on the CLOB — direction is encoded by which outcome token we buy.
        """
        start = time.time()
        # Re-read the mode at order time so import order can never accidentally go live;
        # anything other than an explicit PAPER_MODE=false stays in paper (safe default).
        self.paper_mode = str(_env("PAPER_MODE", "true")).lower() != "false"

        # ── PAPER ────────────────────────────────────────────────────────────
        if self.paper_mode:
            await asyncio.sleep(0.05)  # simulated latency
            fee = self._model_fee(requested_price, size_usdc)
            return FillResult(
                filled=True, status="PAPER", order_id=None,
                requested_price=requested_price, fill_price=requested_price,
                size_usdc=size_usdc, filled_usdc=size_usdc, fee_usdc=fee,
                slippage_usdc=0.0, latency_ms=int((time.time() - start) * 1000),
                is_paper=True,
            )

        # ── LIVE ─────────────────────────────────────────────────────────────
        client = self._get_client()
        if client is None:
            return FillResult(False, "ERROR", None, requested_price, 0.0, size_usdc,
                              0.0, 0.0, 0.0, int((time.time() - start) * 1000), False)
        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY
            from py_clob_client.exceptions import PolyApiException

            # Market buy for `size_usdc` notional, marketable-limit capped at requested_price.
            args = MarketOrderArgs(token_id=token_id, amount=size_usdc,
                                   side=BUY, price=requested_price)
            signed = await asyncio.to_thread(client.create_market_order, args)

            # FAK = fill-and-kill: take whatever fills immediately, cancel the rest.
            # Retry ONLY on 429 (rate limit). Resubmitting the SAME signed order is safe —
            # it has a fixed order hash, so Polymarket won't double-execute it. Delays are
            # short because Last Shadow enters in the final seconds and can't wait long.
            max_retries = int(_env("ORDER_MAX_RETRIES", "3"))
            base_delay = float(_env("ORDER_RETRY_BASE_SEC", "0.2"))
            resp = None
            for attempt in range(max_retries):
                try:
                    resp = await asyncio.to_thread(client.post_order, signed, OrderType.FAK)
                    break
                except PolyApiException as pe:
                    if getattr(pe, "status_code", None) == 429 and attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"[CLOB] {asset} rate-limited (429) — retry "
                                       f"{attempt + 1}/{max_retries} in {delay:.2f}s")
                        await asyncio.sleep(delay)
                        continue
                    raise

            latency_ms = int((time.time() - start) * 1000)
            fr = self._parse_live_response(resp, requested_price, size_usdc, latency_ms)
            if fr.raw is not None:
                logger.info(f"[CLOB] {asset} live order resp: {fr.raw}")  # verify on first trades
            return fr
        except Exception as e:
            logger.error(f"[CLOB] {asset} order error: {e}", exc_info=True)
            return FillResult(False, "ERROR", None, requested_price, 0.0, size_usdc,
                              0.0, 0.0, 0.0, int((time.time() - start) * 1000), False)


    # ── defensive response parsing ──────────────────────────────────────────
    def _parse_live_response(self, resp, requested_price, size_usdc, latency_ms) -> FillResult:
        """
        The exact response shape varies by py-clob-client version, so parse defensively
        and keep the raw blob for verification on the first live trades.
        """
        raw = resp if isinstance(resp, dict) else getattr(resp, "__dict__", {"resp": str(resp)})
        success = bool(raw.get("success", raw.get("status") in ("matched", "live", "success")))
        order_id = raw.get("orderID") or raw.get("orderId") or raw.get("id")

        # Derive actual fill price from filled amounts when present.
        making = _to_float(raw.get("makingAmount"))     # USDC we paid
        taking = _to_float(raw.get("takingAmount"))     # shares we received
        if making and taking and taking > 0:
            fill_price = making / taking
            filled_usdc = making
        else:
            fill_price = requested_price
            filled_usdc = size_usdc if success else 0.0

        fee_usdc = _to_float(raw.get("feeUsdc")) or self._model_fee(fill_price, filled_usdc)
        slippage = round((fill_price - requested_price) / requested_price * filled_usdc, 6) \
            if requested_price else 0.0
        status = "FILLED" if (success and filled_usdc >= size_usdc * 0.99) else \
                 ("PARTIAL" if filled_usdc > 0 else ("REJECTED" if not success else "UNFILLED"))

        return FillResult(
            filled=filled_usdc > 0, status=status, order_id=order_id,
            requested_price=requested_price, fill_price=round(fill_price, 6),
            size_usdc=size_usdc, filled_usdc=round(filled_usdc, 6),
            fee_usdc=fee_usdc, slippage_usdc=slippage, latency_ms=latency_ms,
            is_paper=False, raw=raw,
        )


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
