#!/usr/bin/env python3
"""Реконсиляция recovery_bot positions со статусом 'error' vs реальные PM трейды.

Ситуация: place_limit_buy_order() иногда падает с Request exception (сетевой
таймаут) ДО получения ответа, но ордер уже принят биржей и исполнился.
Бот записывает status='error', filled=0 — деньги теряются в статистике.

Этот скрипт:
1. Читает positions WHERE mode='real' AND status='error'.
2. Тянет все последние трейды с CLOB (authenticated — возвращает только наши).
3. Для каждой error-позиции ищет трейд с тем же asset_id, side=BUY, размером
   ≤ requested_shares, ценой ≤ top_price (0.68), временем match_time ≈ opened_at
   (±300с).
4. В --apply режиме обновляет БД: status='open', filled_shares/total_cost/
   entry_price/fee/pm_order_id. Если рынок уже закрылся — резолвит по winner.
5. Без --apply только печатает план.
"""
from __future__ import annotations

import argparse
import calendar
import os
import sqlite3
import sys
import warnings
from datetime import datetime, timezone, timedelta
from pathlib import Path

warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from real_arb_bot.clients import PolymarketTrader
from recovery_bot.telegram_notify import RecoveryTelegramNotifier


DB_PATH = "data/recovery_bot.db"


def market_url_for(symbol: str, iv: int, market_start_iso: str) -> str:
    dt = datetime.fromisoformat(market_start_iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts = calendar.timegm(dt.utctimetuple())
    return f"https://polymarket.com/event/{symbol.lower()}-updown-{iv}m-{ts}"
TIME_WINDOW_SEC = 300   # ±5 минут
PRICE_TOLERANCE = 0.02  # фактическая цена ≤ top_price+tol
MAX_TRADE_PAGES = 50    # ~100 трейдов/страница


def fetch_all_trades(client) -> list[dict]:
    all_trades = []
    cursor = "MA=="
    for _ in range(MAX_TRADE_PAGES):
        resp = client.get_trades(next_cursor=cursor)
        if isinstance(resp, list):
            all_trades.extend(resp)
            break
        data = resp.get("data", [])
        all_trades.extend(data)
        nc = resp.get("next_cursor", "")
        if nc and nc != cursor:
            cursor = nc
        else:
            break
    return [t for t in all_trades if t.get("status") == "CONFIRMED"]


def score_match(pos: dict, trade: dict) -> tuple[bool, str]:
    """Возвращает (match, reason)."""
    if trade.get("asset_id") != pos["pm_token_id"]:
        return False, "asset_id mismatch"
    if trade.get("side") != "BUY":
        return False, f"side={trade.get('side')}"
    try:
        tsize = float(trade.get("size", 0) or 0)
        tprice = float(trade.get("price", 0) or 0)
        tmatch = int(trade.get("match_time", 0) or 0)
    except (TypeError, ValueError):
        return False, "parse error"
    req_shares = float(pos["requested_shares"])
    # Частичный филл — size может быть меньше. Больше не должен.
    if tsize > req_shares + 0.01:
        return False, f"size={tsize} > requested={req_shares}"
    # Цена ≤ лимит
    top_price = 0.68  # TODO: взять из cfg, но у нас все real_bot входы по 0.68
    if tprice > top_price + PRICE_TOLERANCE:
        return False, f"price={tprice} > limit={top_price}"
    # Время
    opened = datetime.fromisoformat(pos["opened_at"]).replace(tzinfo=timezone.utc)
    tm_utc = datetime.fromtimestamp(tmatch, tz=timezone.utc)
    delta = abs((tm_utc - opened).total_seconds())
    if delta > TIME_WINDOW_SEC:
        return False, f"time delta={delta:.0f}s > {TIME_WINDOW_SEC}s"
    return True, (
        f"size={tsize} px={tprice} dt={delta:+.0f}s"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="применить изменения в БД")
    ap.add_argument("--since-hours", type=float, default=168.0, help="окно поиска error-позиций в часах")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    since_iso = (datetime.utcnow() - timedelta(hours=args.since_hours)).isoformat()
    rows = list(con.execute(
        "SELECT * FROM positions WHERE mode='real' AND status='error'"
        " AND opened_at >= ? AND pm_token_id IS NOT NULL"
        " ORDER BY opened_at DESC",
        (since_iso,),
    ))
    print(f"[reconcile] found {len(rows)} error positions since {since_iso}")
    if not rows:
        return

    pt = PolymarketTrader()
    print("[reconcile] fetching all PM trades...")
    trades = fetch_all_trades(pt._client)
    print(f"[reconcile] fetched {len(trades)} confirmed trades")

    updates: list[tuple[dict, dict]] = []  # (pos_row, trade)
    unmatched: list[dict] = []

    for row in rows:
        pos = dict(row)
        matched: list[tuple[dict, str]] = []
        for t in trades:
            ok, why = score_match(pos, t)
            if ok:
                matched.append((t, why))
        if not matched:
            unmatched.append(pos)
            continue
        # Если несколько — берём ближайший по времени
        opened = datetime.fromisoformat(pos["opened_at"]).replace(tzinfo=timezone.utc)
        matched.sort(key=lambda mt: abs(
            datetime.fromtimestamp(int(mt[0]["match_time"]), tz=timezone.utc) - opened
        ).total_seconds())
        best_trade, best_why = matched[0]
        updates.append((pos, best_trade))
        tag = "MULTI" if len(matched) > 1 else "OK"
        print(
            f"  [{tag}] {pos['symbol']} {pos['interval_minutes']}m {pos['side']}"
            f" opened={pos['opened_at'][:19]} req={pos['requested_shares']} → {best_why}"
            f" txid={best_trade['taker_order_id'][:14]}..."
        )

    for pos in unmatched:
        print(
            f"  [NONE] {pos['symbol']} {pos['interval_minutes']}m {pos['side']}"
            f" opened={pos['opened_at'][:19]} req={pos['requested_shares']} — нет трейда"
        )

    print(f"\n[reconcile] matched: {len(updates)} / unmatched: {len(unmatched)}")
    if not updates:
        return

    if not args.apply:
        print("[reconcile] dry-run. Для применения: --apply")
        return

    print("[reconcile] APPLYING...")
    notifier = RecoveryTelegramNotifier(get_status_fn=lambda: "")
    for pos, trade in updates:
        size = float(trade["size"])
        price = float(trade["price"])
        fee_bps = float(trade.get("fee_rate_bps", 0) or 0)
        fee = size * price * fee_bps / 10000.0
        total_cost = size * price
        order_id = trade["taker_order_id"]
        note = (
            f"reconciled from orphan error | tx={trade.get('transaction_hash','')[:14]}"
            f" match_time={trade['match_time']} px={price} sz={size}"
        )
        con.execute(
            """
            UPDATE positions
            SET status='open', filled_shares=?, total_cost=?, entry_price=?,
                fee=?, pm_order_id=?, note=?
            WHERE id=?
            """,
            (size, total_cost, price, fee, order_id, note, pos["id"]),
        )
        msg_id = None
        try:
            url = market_url_for(pos["symbol"], int(pos["interval_minutes"]), pos["market_start"])
            link = f'\n<a href="{url}">market</a>'
            msg_id = notifier.send(
                f"♻️ <b>RECONCILED OPEN {pos['symbol']} {pos['interval_minutes']}m {pos['side'].upper()}</b>\n"
                f"<i>backfilled from orphan error — ставка уже исполнена в прошлом</i>\n"
                f"opened_at={pos['opened_at'][:19]}\n"
                f"strategy={pos['strategy_name']}\n"
                f"entry={price:.3f} | shares={size:.2f}\n"
                f"cost=${total_cost:.2f}{link}"
            )
        except Exception as exc:
            print(f"  [tg fail] {exc}")
        if msg_id is not None:
            con.execute(
                "UPDATE positions SET tg_open_message_id=? WHERE id=?",
                (int(msg_id), pos["id"]),
            )
        print(
            f"  ✓ {pos['symbol']} {pos['interval_minutes']}m {pos['side']}"
            f" → filled={size}@{price} cost=${total_cost:.2f} fee=${fee:.4f}"
            f" tg_msg={msg_id}"
        )
    con.commit()
    print(f"[reconcile] done. Далее recovery_bot.resolve() подтянет winner и запишет pnl.")


if __name__ == "__main__":
    main()
