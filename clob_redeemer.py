"""
clob_redeemer.py — Redeem resolved Polymarket positions.

After a binary market resolves, winning outcome tokens are worth $1 each but sit
idle in the wallet until redeemed. Redemption on Polymarket is an ON-CHAIN call to
the Conditional Tokens Framework (CTF) contract — it is NOT a CLOB order-book
operation, so py-clob-client cannot perform it.

Paper mode: logs what WOULD be redeemed, takes no action (no creds needed).
Live mode:  on-chain redemption is not wired yet — logs a clear warning so winnings
            are redeemed via the Polymarket UI / an on-chain CTF.redeemPositions call.
            (A web3 implementation is a separate, deliberately-scoped task.)

Called from last_shadow_settlement_sweep() and resolve_market() after a WIN.
"""

import os
import logging
from typing import Optional

logger = logging.getLogger("ClobRedeemer")


def _env(name: str, default=None):
    return os.getenv(name, default)


def _is_paper() -> bool:
    return str(_env("PAPER_MODE", "true")).lower() != "false"


class ClobRedeemer:
    """Handles redemption of resolved winning positions (paper logs; live pending web3)."""

    async def redeem(self, *, condition_id: str, asset: str, pnl: float) -> bool:
        """
        Redeem a resolved winning position.

        condition_id — Polymarket conditionId for the resolved market.
        asset        — BTC / ETH / SOL (for log clarity).
        pnl          — expected USDC payout (for log clarity; the chain decides actual).

        Returns True if redemption was handled (or paper mode), else False.

        NOTE: Redemption on Polymarket is an ON-CHAIN operation against the Conditional
        Tokens Framework (CTF) contract — it is NOT part of the CLOB order-book API, so
        py-clob-client cannot do it. Live auto-redeem therefore requires a separate web3
        implementation (CTF.redeemPositions) which is intentionally NOT wired yet. Until
        then, live winnings must be redeemed via the Polymarket UI or an on-chain call.
        Winnings are safe either way — resolved tokens sit in the wallet until redeemed.
        """
        if _is_paper():
            logger.info(
                f"[REDEEMER] PAPER — would redeem {asset} conditionId={condition_id} "
                f"expected_pnl=+{pnl:.4f} USDC"
            )
            return True

        # Live: on-chain redemption is not implemented. Do NOT pretend success — surface
        # a clear, actionable warning so winnings are redeemed manually until web3 is wired.
        logger.warning(
            f"[REDEEMER] LIVE redeem NOT IMPLEMENTED for {asset} conditionId={condition_id} "
            f"(expected +{pnl:.4f} USDC). Winnings are safe in the wallet — redeem via the "
            f"Polymarket UI or an on-chain CTF.redeemPositions call until web3 auto-redeem is added."
        )
        return False


# Module-level singleton — shared with the settlement sweep.
_redeemer = ClobRedeemer()


async def auto_redeem(*, condition_id: Optional[str], asset: str, pnl: float) -> bool:
    """
    Convenience wrapper. Returns True if redemption was submitted or skipped (paper),
    False if it failed or condition_id is unavailable.

    condition_id is None when the market metadata didn't expose it (old positions);
    in that case we log and skip gracefully rather than crash.
    """
    if not condition_id:
        logger.debug(f"[REDEEMER] {asset} — no condition_id available, skipping redeem")
        return False
    return await _redeemer.redeem(condition_id=condition_id, asset=asset, pnl=pnl)
