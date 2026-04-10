#!/usr/bin/env python3
"""
Полный аудит денежного потока: PM (CLOB trades) + Kalshi (fills + settlements).
Реконструирует реальный PnL независимо от БД.

Использование:
    python3 scripts/audit_balances.py
    python3 scripts/audit_balances.py --since 2026-04-09   # только с этой даты
"""
from __future__ import annotations

import base64
import os
import sqlite3
import sys
import time
import warnings
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import httpx
import requests
requests.packages.urllib3.disable_warnings()

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from real_arb_bot.clients import PolymarketTrader


# ── Kalshi auth (отдельно от KalshiTrader из-за бага PSS.DIGEST_LENGTH) ──

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

def _kalshi_init():
    api_key = os.environ["KALSHI_API_KEY_ID"]
    with open(os.environ["KALSHI_PRIVATE_KEY_PATH"], "rb") as f:
        pk = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    return api_key, pk

def _kalshi_get(api_key, pk, path, params=None):
    url = KALSHI_BASE + path
    sp = urlparse(url).path
    ts = str(int(datetime.now().timestamp() * 1000))
    msg = f"{ts}GET{sp}".encode()
    sig = pk.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    headers = {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "KALSHI-ACCESS-TIMESTAMP": ts,
    }
    r = httpx.get(url, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


# ── Polymarket ─────────────────────────────────────────────────────────

def fetch_pm_trades(client) -> list:
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
                print(f"  pm retry {attempt+1}: {e}")
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


# ── Kalshi fills + settlements ─────────────────────────────────────────

def fetch_kalshi_fills(api_key, pk) -> list:
    all_fills = []
    cursor = None
    for _ in range(50):
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        data = _kalshi_get(api_key, pk, "/portfolio/fills", params=params)
        fills = data.get("fills", [])
        all_fills.extend(fills)
        cursor = data.get("cursor")
        if not cursor or not fills:
            break
        time.sleep(0.2)
    return all_fills


def fetch_kalshi_settlements(api_key, pk) -> list:
    all_items = []
    cursor = None
    for _ in range(50):
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        data = _kalshi_get(api_key, pk, "/portfolio/settlements", params=params)
        items = data.get("settlements", [])
        all_items.extend(items)
        cursor = data.get("cursor")
        if not cursor or not items:
            break
        time.sleep(0.2)
    return all_items


def main():
    since_str = None
    since_ts = 0
    for i, a in enumerate(sys.argv):
        if a == "--since" and i + 1 < len(sys.argv):
            since_str = sys.argv[i + 1]
            since_ts = datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()

    pm = PolymarketTrader()
    api_key, pk = _kalshi_init()
    print(f"[auth] Kalshi key: {api_key[:12]}...")

    pm_balance = pm.get_balance()
    k_data = _kalshi_get(api_key, pk, "/portfolio/balance")
    k_balance_raw = k_data.get("balance_dollars", k_data.get("balance", 0))
    k_balance = k_balance_raw / 100.0 if isinstance(k_balance_raw, int) else float(k_balance_raw)

    print(f"Текущие балансы: PM=${pm_balance:.2f} | Kalshi=${k_balance:.2f}")
    print(f"Сумма: ${pm_balance + k_balance:.2f}")
    if since_str:
        print(f"Фильтр: с {since_str}")

    # ── 1. POLYMARKET ──────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("POLYMARKET (CLOB trades)")
    print(f"{'='*60}")

    all_pm_trades = fetch_pm_trades(pm._client)
    if since_ts:
        pm_trades = [t for t in all_pm_trades if float(t.get("match_time", 0)) >= since_ts]
    else:
        pm_trades = all_pm_trades
    print(f"Confirmed trades: {len(pm_trades)} (всего: {len(all_pm_trades)})")

    pm_total_buy = 0.0
    pm_total_sell = 0.0
    pm_total_fee = 0.0
    pm_periods = defaultdict(lambda: {"buy": 0.0, "sell": 0.0, "fee": 0.0, "count": 0})

    for t in pm_trades:
        size = float(t.get("size", 0))
        price = float(t.get("price", 0))
        fee = float(t.get("fee_rate_bps", 0)) / 10000 * size * price
        ts = float(t.get("match_time", 0))
        dt = datetime.utcfromtimestamp(ts) if ts > 0 else None
        period = dt.strftime("%Y-%m-%d") if dt else "unknown"

        if t.get("side") == "BUY":
            pm_total_buy += size * price
            pm_periods[period]["buy"] += size * price
        else:
            pm_total_sell += size * price
            pm_periods[period]["sell"] += size * price
        pm_total_fee += fee
        pm_periods[period]["fee"] += fee
        pm_periods[period]["count"] += 1

    print(f"\nTotal BUY:  ${pm_total_buy:.2f}")
    print(f"Total SELL: ${pm_total_sell:.2f}")
    print(f"Total fees: ${pm_total_fee:.2f}")
    print(f"Net spend:  ${pm_total_buy - pm_total_sell:.2f}")

    print(f"\nПо дням:")
    for period in sorted(pm_periods.keys()):
        p = pm_periods[period]
        print(f"  {period}: {p['count']} trades | buy=${p['buy']:.2f} sell=${p['sell']:.2f} fee=${p['fee']:.2f}")

    # ── 2. KALSHI ──────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("KALSHI (fills + settlements)")
    print(f"{'='*60}")

    all_fills = fetch_kalshi_fills(api_key, pk)
    if since_str:
        fills = [f for f in all_fills if f.get("created_time", "") >= since_str]
    else:
        fills = all_fills
    print(f"Fills: {len(fills)} (всего: {len(all_fills)})")

    k_total_buy = 0.0
    k_total_sell = 0.0
    k_total_fee = 0.0
    k_periods = defaultdict(lambda: {"buy": 0.0, "sell": 0.0, "fee": 0.0, "count": 0})

    for f in fills:
        count = float(f.get("count_fp", f.get("count", 0)))
        yes_price = float(f.get("yes_price_dollars", 0))
        no_price = float(f.get("no_price_dollars", 0))
        fee = float(f.get("fee_cost", 0))
        action = f.get("action", "")
        side = f.get("side", "")

        if side == "yes":
            price_per = yes_price
        else:
            price_per = no_price

        cost = count * price_per

        if action == "buy":
            k_total_buy += cost
        elif action == "sell":
            k_total_sell += cost
        k_total_fee += fee

        ts = f.get("created_time", "")
        period = ts[:10] if ts else "unknown"
        k_periods[period]["count"] += 1
        k_periods[period]["fee"] += fee
        if action == "buy":
            k_periods[period]["buy"] += cost
        else:
            k_periods[period]["sell"] += cost

    print(f"\nTotal BUY:  ${k_total_buy:.2f}")
    print(f"Total SELL: ${k_total_sell:.2f}")
    print(f"Total fees: ${k_total_fee:.2f}")
    print(f"Net spend:  ${k_total_buy - k_total_sell:.2f}")

    # Settlements
    all_settlements = fetch_kalshi_settlements(api_key, pk)
    if since_str:
        settlements = [s for s in all_settlements if s.get("settled_time", "") >= since_str]
    else:
        settlements = all_settlements
    print(f"\nSettlements: {len(settlements)} (всего: {len(all_settlements)})")

    k_settle_won = 0.0
    k_settle_lost = 0.0
    k_settle_cost = 0.0

    for s in settlements:
        result = s.get("market_result", "")
        yes_count = float(s.get("yes_count_fp", 0))
        no_count = float(s.get("no_count_fp", 0))
        yes_cost = float(s.get("yes_total_cost_dollars", 0))
        no_cost = float(s.get("no_total_cost_dollars", 0))
        fee = float(s.get("fee_cost", 0))

        total_cost = yes_cost + no_cost + fee
        k_settle_cost += total_cost

        # Payout: winning side gets $1 per contract
        if result == "yes":
            payout = yes_count * 1.0  # YES holders get $1 each
            # NO holders get $0
        elif result == "no":
            payout = no_count * 1.0   # NO holders get $1 each
        else:
            payout = 0.0

        pnl = payout - total_cost
        if pnl >= 0:
            k_settle_won += pnl
        else:
            k_settle_lost += pnl

    print(f"Settlement wins:   +${k_settle_won:.2f}")
    print(f"Settlement losses: ${k_settle_lost:.2f}")
    print(f"Settlement total cost: ${k_settle_cost:.2f}")
    print(f"Net settlement PnL: ${k_settle_won + k_settle_lost:.2f}")

    print(f"\nПо дням (fills):")
    for period in sorted(k_periods.keys()):
        p = k_periods[period]
        print(f"  {period}: {p['count']} fills | buy=${p['buy']:.2f} sell=${p['sell']:.2f} fee=${p['fee']:.2f}")

    # ── 3. БД ──────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("БД fast_arb_bot")
    print(f"{'='*60}")

    db_path = Path("data/fast_arb_bot.db")
    if db_path.exists():
        db = sqlite3.connect(str(db_path))
        db.row_factory = sqlite3.Row

        row = db.execute(
            "SELECT COUNT(*) as cnt, SUM(pnl) as pnl FROM positions WHERE is_paper=0 AND status='resolved'"
        ).fetchone()
        print(f"Resolved: {row['cnt']} позиций, PnL=${row['pnl']:.2f}")

        row2 = db.execute(
            "SELECT COUNT(*) as cnt, SUM(total_cost) as cost FROM positions WHERE is_paper=0 AND status='open'"
        ).fetchone()
        print(f"Open: {row2['cnt']} позиций, cost=${row2['cost']:.2f}")

        row3 = db.execute(
            "SELECT COUNT(*) as cnt FROM positions WHERE is_paper=0 AND kalshi_fill_shares > 0"
        ).fetchone()
        row4 = db.execute(
            "SELECT COUNT(*) as cnt FROM positions WHERE is_paper=0 AND polymarket_fill_shares > 0"
        ).fetchone()
        print(f"С Kalshi fill: {row3['cnt']} | С PM fill: {row4['cnt']}")

        # DB Kalshi total cost
        k_db = db.execute("""
            SELECT SUM(kalshi_fill_shares * kalshi_fill_price + COALESCE(kalshi_order_fee, 0)) as k_cost
            FROM positions WHERE is_paper=0 AND kalshi_fill_shares > 0
        """).fetchone()
        print(f"DB Kalshi total cost: ${k_db['k_cost'] or 0:.2f}")

        pm_db = db.execute("""
            SELECT SUM(polymarket_fill_shares * polymarket_fill_price + COALESCE(polymarket_order_fee, 0)) as pm_cost
            FROM positions WHERE is_paper=0 AND polymarket_fill_shares > 0
        """).fetchone()
        print(f"DB PM total cost: ${pm_db['pm_cost'] or 0:.2f}")

        db.close()

    # ── 4. ИТОГ ────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("ИТОГОВАЯ РЕКОНСТРУКЦИЯ")
    print(f"{'='*60}")

    start_pm = 195.22
    start_k = 65.25

    # PM: start + redeems - buys = current => redeems = current + buys - start
    pm_implied_redeems = pm_balance + pm_total_buy - pm_total_sell - start_pm
    pm_pnl = pm_implied_redeems - (pm_total_buy - pm_total_sell)

    print(f"\nPOLYMARKET:")
    print(f"  Старт:            ${start_pm:.2f}")
    print(f"  Потрачено на buy: ${pm_total_buy:.2f}")
    print(f"  Получено redeems: ${pm_implied_redeems:.2f}")
    print(f"  Сейчас:           ${pm_balance:.2f}")
    print(f"  PM PnL:           ${pm_pnl:.2f}")

    # Kalshi: start + topups + settlement_payouts - buys - fees = current
    # => topups = current + buys + fees - settlement_payouts - start
    k_net_pnl = k_settle_won + k_settle_lost
    k_implied_topup = k_balance + k_total_buy + k_total_fee - k_total_sell - start_k - k_net_pnl
    # Упрощённо: всё что не объясняется трейдами = topup

    print(f"\nKALSHI:")
    print(f"  Старт:            ${start_k:.2f}")
    print(f"  Потрачено на buy: ${k_total_buy:.2f}")
    print(f"  Fees:             ${k_total_fee:.2f}")
    print(f"  Settlement PnL:   ${k_net_pnl:.2f}")
    print(f"  Topup (расч):     ${k_implied_topup:.2f}")
    print(f"  Сейчас:           ${k_balance:.2f}")

    total_pnl = pm_pnl + k_net_pnl
    total_start = start_pm + start_k

    print(f"\n{'─'*40}")
    print(f"ОБЩИЙ PnL:  ${total_pnl:.2f}")
    print(f"Старт:      ${total_start:.2f}")
    print(f"Topups:     ${k_implied_topup:.2f}")
    print(f"Сейчас:     ${pm_balance + k_balance:.2f} (+ open ${row2['cost']:.2f})")
    print(f"Проверка:   ${total_start:.2f} + ${k_implied_topup:.2f} + ${total_pnl:.2f} = ${total_start + k_implied_topup + total_pnl:.2f}")
    print(f"            Факт (с open): ${pm_balance + k_balance + (row2['cost'] or 0):.2f}")


if __name__ == "__main__":
    main()
