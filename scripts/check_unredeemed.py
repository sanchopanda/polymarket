#!/usr/bin/env python3
"""
Проверяет все CLOB-трейды на Polymarket, находит токены в кошельке,
определяет какие из них redeemable (resolved + winning side).

Использование:
    python3 scripts/check_unredeemed.py              # только проверка
    python3 scripts/check_unredeemed.py --redeem      # проверка + redeem выигрышных
    python3 scripts/check_unredeemed.py --redeem --yes # без подтверждения
"""
from __future__ import annotations

import json
import sys
import time
import warnings
from collections import defaultdict
from pathlib import Path

warnings.filterwarnings("ignore")

# Добавляем корень проекта в path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests
requests.packages.urllib3.disable_warnings()

from web3 import Web3
from real_arb_bot.clients import PolymarketTrader

# ── Контракты ──────────────────────────────────────────────────────────
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
]

BALANCE_OF_ABI = [{
    "inputs": [
        {"name": "account", "type": "address"},
        {"name": "id", "type": "uint256"},
    ],
    "name": "balanceOf",
    "outputs": [{"name": "", "type": "uint256"}],
    "stateMutability": "view",
    "type": "function",
}]

PAYOUT_ABI = [
    {
        "inputs": [{"name": "conditionId", "type": "bytes32"}],
        "name": "payoutDenominator",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "index", "type": "uint256"},
        ],
        "name": "payoutNumerators",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

REDEEM_ABI = [{
    "inputs": [
        {"name": "collateralToken", "type": "address"},
        {"name": "parentCollectionId", "type": "bytes32"},
        {"name": "conditionId", "type": "bytes32"},
        {"name": "indexSets", "type": "uint256[]"},
    ],
    "name": "redeemPositions",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function",
}]


def get_w3(rpc_idx: int = 0) -> Web3:
    return Web3(Web3.HTTPProvider(RPCS[rpc_idx % len(RPCS)], request_kwargs={"timeout": 10}))


def rpc_call(func, *args, retries: int = 3):
    """Вызов контракта с retry + смена RPC."""
    for attempt in range(retries):
        try:
            return func(*args).call()
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(1)


def fetch_clob_trades(client) -> list:
    """Все CONFIRMED trades из CLOB API."""
    all_trades = []
    cursor = "MA=="
    for _ in range(30):
        for attempt in range(4):
            try:
                resp = client.get_trades(next_cursor=cursor)
                break
            except Exception as e:
                if attempt == 3:
                    raise
                print(f"  retry trades {attempt+1}: {e}")
                time.sleep(3 * (attempt + 1))
        if isinstance(resp, list):
            all_trades.extend(resp)
            break
        trades = resp.get("data", [])
        all_trades.extend(trades)
        nc = resp.get("next_cursor", "")
        if nc and nc != cursor:
            cursor = nc
        else:
            break
    return [t for t in all_trades if t.get("status") == "CONFIRMED"]


def main():
    do_redeem = "--redeem" in sys.argv
    auto = "--yes" in sys.argv

    pm = PolymarketTrader()
    client = pm._client
    wallet = pm._address
    wallet_cs = Web3.to_checksum_address(wallet)

    print(f"Wallet: {wallet}")
    print(f"Balance: ${pm.get_balance():.2f}")

    # ── 1. Собираем все трейды ─────────────────────────────────────────
    print("\n[1/3] Загружаю трейды из CLOB...", flush=True)
    trades = fetch_clob_trades(client)
    print(f"  Confirmed trades: {len(trades)}")

    # Группируем по asset_id
    by_asset: dict[str, dict] = {}
    for t in trades:
        aid = t.get("asset_id", "")
        if aid not in by_asset:
            by_asset[aid] = {
                "shares": 0.0, "cost": 0.0,
                "market": t.get("market", ""),
                "outcome": t.get("outcome", ""),
            }
        size = float(t.get("size", 0))
        price = float(t.get("price", 0))
        if t.get("side") == "BUY":
            by_asset[aid]["shares"] += size
            by_asset[aid]["cost"] += size * price
        else:
            by_asset[aid]["shares"] -= size
            by_asset[aid]["cost"] -= size * price

    print(f"  Unique tokens: {len(by_asset)}")

    # ── 2. Проверяем on-chain баланс ───────────────────────────────────
    print(f"\n[2/3] Проверяю балансы токенов on-chain...", flush=True)
    w3 = get_w3()
    ctf = w3.eth.contract(
        address=Web3.to_checksum_address(CTF_ADDRESS),
        abi=BALANCE_OF_ABI + PAYOUT_ABI,
    )

    tokens_with_balance = []
    errors = 0
    for i, (aid, info) in enumerate(by_asset.items()):
        for attempt in range(2):
            try:
                bal = ctf.functions.balanceOf(wallet_cs, int(aid)).call()
                if bal > 0:
                    tokens_with_balance.append({
                        "asset_id": aid,
                        "balance_raw": bal,
                        "balance": bal / 1e6,
                        **info,
                    })
                break
            except Exception:
                if attempt == 0:
                    w3 = get_w3(1)
                    ctf = w3.eth.contract(
                        address=Web3.to_checksum_address(CTF_ADDRESS),
                        abi=BALANCE_OF_ABI + PAYOUT_ABI,
                    )
                    time.sleep(0.5)
                else:
                    errors += 1
        time.sleep(0.05)
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(by_asset)}... found={len(tokens_with_balance)} err={errors}", flush=True)

    print(f"  Tokens with balance: {len(tokens_with_balance)} (errors: {errors})")

    # Группируем по market (conditionId)
    by_market: dict[str, list] = defaultdict(list)
    for t in tokens_with_balance:
        by_market[t["market"]].append(t)

    # ── 3. Проверяем payoutDenominator ─────────────────────────────────
    print(f"\n[3/3] Проверяю статус {len(by_market)} рынков...", flush=True)

    redeemable = []
    unresolved_tokens = []
    resolved_loss_shares = 0.0
    check_errors = 0

    for i, (market_hex, tokens) in enumerate(by_market.items()):
        cond_bytes = bytes.fromhex(market_hex.replace("0x", ""))

        for attempt in range(3):
            try:
                denom = ctf.functions.payoutDenominator(cond_bytes).call()
                break
            except Exception:
                if attempt < 2:
                    w3 = get_w3(attempt + 1)
                    ctf = w3.eth.contract(
                        address=Web3.to_checksum_address(CTF_ADDRESS),
                        abi=BALANCE_OF_ABI + PAYOUT_ABI,
                    )
                    time.sleep(1)
                else:
                    denom = -1
                    check_errors += 1

        if denom == -1:
            continue

        if denom == 0:
            # Рынок ещё не resolved
            for t in tokens:
                unresolved_tokens.append(t)
            continue

        # Resolved — проверяем payout
        try:
            payout_0 = ctf.functions.payoutNumerators(cond_bytes, 0).call()
            payout_1 = ctf.functions.payoutNumerators(cond_bytes, 1).call()
        except Exception:
            check_errors += 1
            continue

        for t in tokens:
            outcome = t["outcome"]
            if outcome in ("Yes", "Up"):
                payout = payout_0
            else:
                payout = payout_1

            value = t["balance"] * payout / denom

            if value > 0.001:
                redeemable.append({
                    **t,
                    "value": value,
                    "condition_id": market_hex,
                })
            else:
                resolved_loss_shares += t["balance"]

        time.sleep(0.05)
        if (i + 1) % 20 == 0:
            found = len(redeemable)
            print(f"  {i+1}/{len(by_market)}... redeemable={found} unresolved={len(unresolved_tokens)} err={check_errors}", flush=True)

    # ── Результаты ─────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"РЕЗУЛЬТАТ:")
    print(f"  Токены в кошельке: {len(tokens_with_balance)}")
    print(f"  Unresolved (ещё не закрылись): {len(unresolved_tokens)}")
    print(f"  Resolved losses (стоят $0): {resolved_loss_shares:.0f} shares")
    print(f"  Ошибки проверки: {check_errors}")

    total_value = sum(r["value"] for r in redeemable)
    print(f"\n  REDEEMABLE (можно вернуть): {len(redeemable)} позиций, ${total_value:.2f}")

    if redeemable:
        print(f"\n  Детали:")
        for r in sorted(redeemable, key=lambda x: -x["value"]):
            print(
                f"    ${r['value']:.2f} | {r['outcome']} "
                f"| shares={r['balance']:.2f} cost=${r['cost']:.2f} "
                f"| market={r['condition_id'][:30]}..."
            )

    if unresolved_tokens:
        total_unresolved = sum(t["balance"] for t in unresolved_tokens)
        print(f"\n  Unresolved (ждём закрытия): {len(unresolved_tokens)} позиций, {total_unresolved:.0f} shares")
        for t in sorted(unresolved_tokens, key=lambda x: -x["balance"])[:10]:
            print(
                f"    {t['outcome']} | shares={t['balance']:.2f} cost=${t['cost']:.2f} "
                f"| market={t['market'][:30]}..."
            )

    # ── Redeem ─────────────────────────────────────────────────────────
    if not do_redeem or not redeemable:
        if redeemable:
            print(f"\nДля redeem запусти: python3 scripts/check_unredeemed.py --redeem")
        return

    if not auto:
        ans = input(f"\nRedeem {len(redeemable)} позиций на ${total_value:.2f}? [y/N] ").strip().lower()
        if ans != "y":
            print("Отменено.")
            return

    # Группируем redeemable по condition_id (один redeem на market)
    redeem_markets: dict[str, list] = defaultdict(list)
    for r in redeemable:
        redeem_markets[r["condition_id"]].append(r)

    balance_before = pm.get_balance()
    total_payout = 0.0

    for cond_id, tokens in redeem_markets.items():
        total_val = sum(t["value"] for t in tokens)
        outcomes = ", ".join(t["outcome"] for t in tokens)
        print(f"\n>>> Redeem: {cond_id[:30]}... | {outcomes} | ~${total_val:.2f}")

        try:
            result = pm.redeem(
                market_id="",  # не используется если condition_id передан
                condition_id=cond_id,
                neg_risk=False,
            )
            if result.success:
                total_payout += result.payout_usdc
                print(f"    OK | payout=${result.payout_usdc:.4f} | gas={result.gas_used}")
            elif result.pending:
                print(f"    PENDING tx={result.tx_hash}")
            else:
                print(f"    FAILED: {result.error}")
        except Exception as e:
            print(f"    EXCEPTION: {e}")
        time.sleep(2)

    balance_after = pm.get_balance()
    diff = balance_after - balance_before
    print(f"\n{'='*60}")
    print(f"Redeemed: ${total_payout:.4f}")
    print(f"Balance before: ${balance_before:.4f}")
    print(f"Balance after:  ${balance_after:.4f} ({'+' if diff >= 0 else ''}{diff:.4f})")


if __name__ == "__main__":
    main()
