#!/usr/bin/env python3
"""
Находит все CONFIRMED trades старше 1 часа, определяет выигранные,
делает redeem для каждого. Показывает баланс до и после.

Использование:
    python3 -m oracle_arb_bot.redeem_all          # все trades старше 1 часа
    python3 -m oracle_arb_bot.redeem_all --yes     # без подтверждения
"""
from __future__ import annotations

import json
import sys
import time
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests
requests.packages.urllib3.disable_warnings()

from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
from real_arb_bot.clients import PolymarketTrader

GAMMA = "https://gamma-api.polymarket.com"
SESSION = requests.Session()
SESSION.verify = False


def _get(url, **kwargs) -> dict | list:
    for attempt in range(4):
        try:
            r = SESSION.get(url, timeout=20, **kwargs)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 3:
                raise
            print(f"    retry {attempt+1}: {e}")
            time.sleep(2 * (attempt + 1))


def fetch_trades(client) -> list:
    """Забирает все trades через py_clob_client с retry."""
    all_trades = []
    cursor = "MA=="
    for _ in range(30):
        resp = None
        for attempt in range(4):
            try:
                resp = client.get_trades(next_cursor=cursor)
                break
            except Exception as e:
                if attempt == 3:
                    raise
                print(f"    retry {attempt+1}: {e}")
                time.sleep(3 * (attempt + 1))
        if resp is None:
            break
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
    return all_trades


def main():
    auto = "--yes" in sys.argv

    pm = PolymarketTrader()
    client = pm._client

    # Баланс до
    try:
        client.update_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    except Exception:
        pass
    wallet_before = pm.get_balance()
    wallet_addr = pm._client._signer.address if hasattr(pm._client, "_signer") else ""
    print(f"Wallet: ${wallet_before:.4f}  addr={wallet_addr[:10]}...")

    # Загружаем trades через requests (минуем py_clob_client с таймаутом)
    print("Загружаю историю trades...", flush=True)
    try:
        all_trades = fetch_trades(client)
    except Exception as e:
        print(f"Ошибка загрузки trades: {e}")
        return

    now = time.time()
    confirmed = [
        t for t in all_trades
        if t.get("status") == "CONFIRMED"
        and 3600 <= now - float(t.get("match_time", 0)) <= 2 * 86400
    ]
    print(f"\nCONFIRMED trades (1h–2d old): {len(confirmed)}")

    # Группируем по рынку
    by_market: dict[str, dict] = {}
    for t in confirmed:
        mid = t["market"]
        if mid not in by_market:
            by_market[mid] = {"asset_ids": set(), "cost": 0.0, "shares": 0.0, "outcome": None}
        by_market[mid]["asset_ids"].add(t.get("asset_id", ""))
        by_market[mid]["cost"] += float(t["size"]) * float(t["price"])
        by_market[mid]["shares"] += float(t["size"])
        by_market[mid]["outcome"] = t.get("outcome", "?")

    # Проверяем каждый рынок через Gamma
    winners = []
    total_cost = 0.0
    total_loss = 0.0

    for mid, info in by_market.items():
        aid = list(info["asset_ids"])[0]
        total_cost += info["cost"]
        try:
            gdata = _get(f"{GAMMA}/markets", params={"clob_token_ids": aid})
            if not gdata:
                print(f"  ? no gamma data for {mid[:20]}...")
                continue
            g = gdata[0]
            prices = g.get("outcomePrices", "[]")
            outs = g.get("outcomes", "[]")
            if isinstance(prices, str):
                prices = json.loads(prices)
            if isinstance(outs, str):
                outs = json.loads(outs)
            winner = None
            for i, p in enumerate(prices):
                if float(p) > 0.9:
                    winner = outs[i] if i < len(outs) else None

            q = g.get("question", "?")[:60]
            gamma_id = str(g["id"])
            won = winner is not None and winner == info["outcome"]
            resolved = winner is not None
            tag = "WIN" if won else ("LOSS" if resolved else "OPEN")

            print(f"  [{tag}] {q}")
            print(f"        cost=${info['cost']:.2f} shares={info['shares']:.1f} our={info['outcome']} winner={winner} id={gamma_id}")

            if won:
                winners.append({
                    "gamma_id": gamma_id, "question": q,
                    "shares": info["shares"], "cost": info["cost"],
                    "condition_id": g.get("conditionId", ""),
                    "neg_risk": g.get("negRisk", False),
                })
            elif resolved:
                total_loss += info["cost"]
        except Exception as e:
            print(f"  Error {mid[:20]}: {e}")

    print(f"\nTotal cost: ${total_cost:.2f} | Losses: ${total_loss:.2f}")
    print(f"Winners to redeem: {len(winners)}")

    if not winners:
        print("Нечего redeemить.")
        return

    for w in winners:
        print(f"  {w['question']} | shares={w['shares']:.1f} cost=${w['cost']:.2f}")

    if not auto:
        ans = input("\nRedeem? [y/N] ").strip().lower()
        if ans != "y":
            print("Отменено.")
            return

    # Redeem
    total_payout = 0.0
    for w in winners:
        print(f"\n>>> Redeem: {w['question']}")
        try:
            result = pm.redeem(w["gamma_id"], condition_id=w.get("condition_id"), neg_risk=w.get("neg_risk"))
            if result.success:
                total_payout += result.payout_usdc
                print(f"    OK | payout=${result.payout_usdc:.4f} | gas={result.gas_used}")
            elif result.pending:
                print(f"    PENDING tx={result.tx_hash}")
                print(f"    Подожди 2 минуты и запусти скрипт снова")
            else:
                print(f"    FAILED: {result.error}")
        except Exception as e:
            print(f"    EXCEPTION: {e}")
        time.sleep(3)

    # Баланс после
    try:
        client.update_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    except Exception:
        pass
    wallet_after = pm.get_balance()
    diff = wallet_after - wallet_before
    print(f"\n{'='*50}")
    print(f"Redeemed: ${total_payout:.4f}")
    print(f"Wallet before: ${wallet_before:.4f}")
    print(f"Wallet after:  ${wallet_after:.4f} ({'+' if diff >= 0 else ''}{diff:.4f})")


if __name__ == "__main__":
    main()
