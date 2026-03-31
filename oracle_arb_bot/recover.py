#!/usr/bin/env python3
"""
Recovery: найти неучтённые trades на Polymarket, зарезолвить, сделать redeem, обновить депозит.
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import httpx
from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

from real_arb_bot.clients import PolymarketTrader

GAMMA = "https://gamma-api.polymarket.com"


def main():
    pm = PolymarketTrader()
    client = pm._client

    # 1. Refresh CLOB
    try:
        client.update_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    except:
        pass
    print(f"Wallet: ${pm.get_balance():.4f}")

    # 2. Fetch all trades
    all_trades = []
    cursor = "MA=="
    for _ in range(20):
        resp = client.get_trades(next_cursor=cursor)
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

    now = time.time()
    hours = float(sys.argv[1]) if len(sys.argv) > 1 else 4
    confirmed = [
        t for t in all_trades
        if t.get("status") == "CONFIRMED"
        and now - float(t.get("match_time", 0)) < hours * 3600
    ]
    print(f"\nCONFIRMED trades ({hours}h): {len(confirmed)}")

    # 3. Group by market
    by_market = defaultdict(lambda: {
        "asset_ids": set(), "total_cost": 0.0,
        "total_shares": 0.0, "outcome": None,
    })
    for t in confirmed:
        mid = t["market"]
        by_market[mid]["asset_ids"].add(t.get("asset_id", ""))
        by_market[mid]["total_cost"] += float(t["size"]) * float(t["price"])
        by_market[mid]["total_shares"] += float(t["size"])
        by_market[mid]["outcome"] = t.get("outcome", "?")

    # 4. Check resolution, find winners
    winners = []
    total_cost = 0.0
    total_win_shares = 0.0

    for mid, info in by_market.items():
        aid = list(info["asset_ids"])[0]
        total_cost += info["total_cost"]
        try:
            r = httpx.get(f"{GAMMA}/markets?clob_token_ids={aid}", timeout=10)
            gdata = r.json()
            if not gdata:
                print(f"  ? (no gamma data) cost=${info['total_cost']:.2f}")
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
            won = winner == info["outcome"]
            tag = "WIN" if won else "LOSS"
            print(f"  [{tag}] {q} | cost=${info['total_cost']:.2f} shares={info['total_shares']:.1f}")
            if won:
                total_win_shares += info["total_shares"]
                winners.append({
                    "gamma_id": gamma_id,
                    "question": q,
                    "shares": info["total_shares"],
                    "cost": info["total_cost"],
                })
        except Exception as e:
            print(f"  Error: {e}")

    print(f"\nTotal cost: ${total_cost:.2f}")
    print(f"Winners: {len(winners)} | Win shares: {total_win_shares:.1f}")

    if not winners:
        print("Nothing to redeem.")
        return

    # 5. Redeem
    auto = "--yes" in sys.argv
    if not auto:
        ans = input("\nRedeem? [y/N] ").strip().lower()
        if ans != "y":
            return

    total_payout = 0.0
    for w in winners:
        print(f"\n  Redeem: {w['question']} (gamma_id={w['gamma_id']})")
        try:
            result = pm.redeem(w["gamma_id"])
            if result.success:
                total_payout += result.payout_usdc
                print(f"    OK payout=${result.payout_usdc:.4f}")
            elif result.pending:
                print(f"    PENDING tx")
            else:
                print(f"    FAILED: {result.error}")
        except Exception as e:
            print(f"    EXCEPTION: {e}")
        time.sleep(3)

    # 6. Final balance
    try:
        client.update_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    except:
        pass
    final = pm.get_balance()
    print(f"\nRedeemed: ${total_payout:.2f}")
    print(f"Final wallet: ${final:.4f}")

    # 7. Update oracle DB deposit
    if not auto:
        ans2 = input(f"\nОбновить oracle DB deposit на ${final:.2f}? [y/N] ").strip().lower()
        if ans2 != "y":
            return
    else:
        pass  # auto mode — update
        from oracle_arb_bot.db import OracleDB
        db = OracleDB("data/oracle_arb_bot.db")
        db._update_real_deposit(final, final)
        print(f"DB deposit обновлён: balance=${final:.2f} peak=${final:.2f}")


if __name__ == "__main__":
    main()
