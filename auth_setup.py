"""
auth_setup.py — one-time L2 API credential derivation for the Polymarket CLOB.

Run this ONCE (after your wallet is funded and has logged into polymarket.com at least
once) to derive the L2 API credentials for YOUR wallet + signature type. Copy the printed
values into your .env / Railway as POLY_API_KEY / POLY_API_SECRET / POLY_API_PASSPHRASE.

Reads from env:
    POLY_PRIVATE_KEY     — wallet signing key (L1)
    POLY_FUNDER_ADDRESS  — deposit wallet address (the funder)
    SIGNATURE_TYPE       — 0/1/2/3 (default 3 = POLY_1271 deposit-wallet flow)
    CHAIN_ID             — default 137 (Polygon)
    POLY_CLOB_API_URL    — default https://clob.polymarket.com

SECURITY: prints the derived creds to the console ONCE. Never writes them to disk or logs.
Do NOT commit the output. Do NOT paste it into chat. Clear your terminal after copying.

Usage:
    python auth_setup.py
"""

import os
import sys


def main():
    key = os.getenv("POLY_PRIVATE_KEY")
    funder = os.getenv("POLY_FUNDER_ADDRESS")
    if not key or not funder:
        print("ERROR: set POLY_PRIVATE_KEY and POLY_FUNDER_ADDRESS in your environment first.")
        sys.exit(1)

    sig_type = int(os.getenv("SIGNATURE_TYPE", "3"))
    chain_id = int(os.getenv("CHAIN_ID", "137"))
    host = os.getenv("POLY_CLOB_API_URL", "https://clob.polymarket.com")

    print(f"Deriving L2 creds  (signature_type={sig_type}, chain_id={chain_id}, funder={funder}) ...")

    try:
        from py_clob_client.client import ClobClient
    except ImportError:
        print("ERROR: py-clob-client not installed. Run: pip install -r requirements.txt")
        sys.exit(1)

    try:
        client = ClobClient(host, key=key, chain_id=chain_id,
                            signature_type=sig_type, funder=funder)
        creds = client.create_or_derive_api_creds()
    except Exception as e:
        print(f"\nERROR deriving credentials: {e}\n")
        print("Most common cause: this wallet has never logged into polymarket.com.")
        print("Log in once at polymarket.com with this wallet, then re-run this script.")
        print("Also verify SIGNATURE_TYPE matches your wallet (3 = MetaMask deposit-wallet flow).")
        sys.exit(1)

    print("\n" + "=" * 70)
    print("Derived L2 API credentials — copy into .env / Railway, then clear terminal:")
    print("=" * 70)
    print(f"POLY_API_KEY={creds.api_key}")
    print(f"POLY_API_SECRET={creds.api_secret}")
    print(f"POLY_API_PASSPHRASE={creds.api_passphrase}")
    print("=" * 70)
    print("Do NOT commit these. Do NOT paste them in chat.")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    main()
