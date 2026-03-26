#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv
from eth_account import Account
from web3 import Web3

from real_arb_bot.clients import CTF_ADDRESS, POLYGON_RPCS
from src.api.gamma import GammaClient

ERC1155_BALANCE_ABI = [
    {
        "constant": True,
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    }
]


def _load_config(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text())


def _market_ids_from_db(db_path: str) -> list[str]:
    import sqlite3

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT DISTINCT market_yes, market_no, venue_yes, venue_no
        FROM positions
        """
    ).fetchall()
    conn.close()

    market_ids: set[str] = set()
    for market_yes, market_no, venue_yes, venue_no in rows:
        if venue_yes == "polymarket" and market_yes:
            market_ids.add(str(market_yes))
        if venue_no == "polymarket" and market_no:
            market_ids.add(str(market_no))
    return sorted(market_ids)


def _get_w3() -> Web3:
    last_error = None
    for rpc in POLYGON_RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
            if w3.is_connected():
                return w3
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Все Polygon RPC недоступны: {last_error}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Проверка реальных PM outcome token balances на кошельке")
    parser.add_argument("--config", default="fast_arb_bot/config.yaml")
    parser.add_argument("--db", default="data/fast_arb_bot.db")
    parser.add_argument("--market-id", action="append", help="Проверить конкретный PM market id; можно указать несколько раз")
    parser.add_argument("--show-zero", action="store_true", help="Показывать и нулевые балансы тоже")
    args = parser.parse_args()

    load_dotenv()
    config = _load_config(args.config)

    private_key = os.environ["WALLET_PRIVATE_KEY"]
    address = Account.from_key(private_key).address
    gamma = GammaClient(
        base_url=config["polymarket"]["gamma_base_url"],
        page_size=config["polymarket"]["page_size"],
        delay_ms=config["polymarket"]["request_delay_ms"],
    )
    w3 = _get_w3()
    ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=ERC1155_BALANCE_ABI)

    market_ids = args.market_id or _market_ids_from_db(args.db)
    if not market_ids:
        print("Нет PM market id для проверки.")
        return

    print(f"Wallet: {address}")
    print(f"PM markets to scan: {len(market_ids)}")

    non_zero_found = 0
    for market_id in market_ids:
        market = gamma.fetch_market(str(market_id))
        if market is None:
            print(f"[skip] market {market_id}: unavailable")
            continue

        printed_market = False
        for outcome, token_id in zip(market.outcomes, market.clob_token_ids):
            try:
                raw_balance = ctf.functions.balanceOf(
                    Web3.to_checksum_address(address),
                    int(str(token_id)),
                ).call()
                balance = raw_balance / 1e6
            except Exception as exc:
                if not printed_market:
                    print(f"\nMarket {market_id} | {market.question}")
                    printed_market = True
                print(f"  {outcome:<6} token={token_id} | ERROR: {exc}")
                continue

            if balance > 0:
                non_zero_found += 1
            if balance > 0 or args.show_zero:
                if not printed_market:
                    print(f"\nMarket {market_id} | {market.question}")
                    printed_market = True
                print(
                    f"  {outcome:<6} token={token_id} | balance={balance:.6f} | "
                    f"active={market.active} closed={market.closed} prices={market.outcome_prices}"
                )

    if non_zero_found == 0:
        print("\nНенулевых PM outcome token balances не найдено.")


if __name__ == "__main__":
    main()
