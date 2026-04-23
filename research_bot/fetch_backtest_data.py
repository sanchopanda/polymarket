"""
research_bot/fetch_backtest_data.py

Единый скрипт подготовки данных для бэктеста. Выполняет три шага:
  1. Скачивает закрытые рынки из Gamma API за указанный период
  2. Скачивает Binance 1s klines для покрытых рынком временных диапазонов
  3. Скачивает PM сделки для каждого рынка

Запуск:
  python3 -m research_bot.fetch_backtest_data --days 7
  python3 -m research_bot.fetch_backtest_data --from 2026-03-01 --to 2026-04-01
  python3 -m research_bot.fetch_backtest_data --days 30 --symbols BTC ETH
  python3 -m research_bot.fetch_backtest_data --days 7 --force --skip-trades
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from research_bot.backtest_db import get_connection
from research_bot.fetch_markets import (
    _iter_closed_pages,
    _extract_interval,
    _winning_side,
    _parse_outcomes,
    ALLOWED_SYMBOLS,
    ALLOWED_INTERVALS,
    SYMBOL_MAP,
)
from research_bot.fetch_trades import (
    enrich_condition_ids,
    fetch_market_trades,
)
from research_bot.backtest_binance_momentum import (
    ensure_binance_1s,
    BINANCE_SYMBOLS,
)

from src.api.gamma import _parse_end_date


def step_markets(
    conn,
    http: httpx.Client,
    date_from: datetime,
    date_to: datetime,
    symbols: set[str],
    force: bool,
) -> list[str]:
    """
    Шаг 1: скачать закрытые рынки из Gamma API за [date_from, date_to].
    Возвращает список market_id которые были добавлены/уже были в DB.
    """
    existing = {r[0] for r in conn.execute("SELECT market_id FROM markets").fetchall()}
    print(f"[markets] уже в DB: {len(existing)}")
    print(f"[markets] диапазон: {date_from.date()} – {date_to.date()}, символы: {sorted(symbols)}")

    saved = skipped_dupe = skipped_filter = skipped_range = 0
    market_ids_in_range: list[str] = []

    for raw in _iter_closed_pages(
        http,
        end_date_min=date_from.strftime("%Y-%m-%d"),
        end_date_max=(date_to + timedelta(days=1)).strftime("%Y-%m-%d"),
        ascending=True,
    ):
        end_date = _parse_end_date(raw.get("endDate"))
        if end_date is None:
            skipped_filter += 1
            continue

        # ascending: старые → новые. Сервер уже фильтрует end_date_min/max,
        # дополнительно подстраховка: выход за верх → стоп, за низ → skip.
        if end_date > date_to:
            break
        if end_date < date_from:
            skipped_range += 1
            continue

        mid = str(raw.get("id", ""))
        if not mid:
            continue

        # Рынок уже есть — считаем его в диапазон, но не перезаписываем
        if mid in existing and not force:
            skipped_dupe += 1
            market_ids_in_range.append(mid)
            continue

        outcomes, prices = _parse_outcomes(raw)
        if not outcomes or not prices:
            skipped_filter += 1
            continue

        question = raw.get("question", "")
        import re
        from research_bot.fetch_markets import UPDOWN_RE
        match = UPDOWN_RE.match(question)
        if not match:
            skipped_filter += 1
            continue

        symbol = SYMBOL_MAP.get(match.group("symbol").upper(), match.group("symbol").upper())
        if symbol not in symbols:
            skipped_filter += 1
            continue

        interval = _extract_interval(question)
        if interval not in ALLOWED_INTERVALS:
            skipped_filter += 1
            continue

        winning = _winning_side(outcomes, prices)
        if winning is None:
            skipped_filter += 1
            continue

        market_start = end_date - timedelta(minutes=interval)

        conn.execute(
            """INSERT OR IGNORE INTO markets
               (market_id, condition_id, symbol, interval_minutes,
                market_start, market_end, winning_side)
               VALUES (?,?,?,?,?,?,?)""",
            (
                mid,
                raw.get("conditionId", ""),
                symbol,
                interval,
                market_start.strftime("%Y-%m-%d %H:%M:%S"),
                end_date.strftime("%Y-%m-%d %H:%M:%S"),
                winning,
            ),
        )
        conn.commit()
        saved += 1
        existing.add(mid)
        market_ids_in_range.append(mid)

        if saved % 200 == 0:
            print(f"  [{saved}] сохранено | dupes={skipped_dupe} | filter={skipped_filter}")

    print(f"[markets] готово: новых={saved} dupes={skipped_dupe} filter={skipped_filter} out_of_range={skipped_range}")
    print(f"[markets] рынков в диапазоне: {len(market_ids_in_range)}")
    return market_ids_in_range


def step_binance(conn, http: httpx.Client, market_ids: list[str], force: bool) -> None:
    """
    Шаг 2: скачать Binance 1s klines для всех рынков из market_ids.
    Группирует по символу, определяет общий диапазон, загружает одним куском.
    """
    if not market_ids:
        print("[binance] нет рынков — пропуск")
        return

    placeholders = ",".join("?" * len(market_ids))
    rows = conn.execute(
        f"SELECT symbol, market_start, market_end FROM markets WHERE market_id IN ({placeholders})",
        market_ids,
    ).fetchall()

    # Определяем диапазон по символу
    sym_ranges: dict[str, tuple[int, int]] = {}
    for symbol, ms_str, me_str in rows:
        if symbol not in BINANCE_SYMBOLS:
            continue
        ms_dt = datetime.strptime(ms_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        me_dt = datetime.strptime(me_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        start_ts = int(ms_dt.timestamp()) - 60  # небольшой буфер
        end_ts = int(me_dt.timestamp()) + 60
        if symbol not in sym_ranges:
            sym_ranges[symbol] = (start_ts, end_ts)
        else:
            lo, hi = sym_ranges[symbol]
            sym_ranges[symbol] = (min(lo, start_ts), max(hi, end_ts))

    print(f"[binance] символов: {len(sym_ranges)}")
    for sym, (start_ts, end_ts) in sorted(sym_ranges.items()):
        duration_min = (end_ts - start_ts) // 60
        print(f"  {sym}: {datetime.utcfromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M')} "
              f"→ {datetime.utcfromtimestamp(end_ts).strftime('%Y-%m-%d %H:%M')} ({duration_min} мин)")
        ensure_binance_1s(conn, sym, start_ts, end_ts, http, force=force)


def step_trades(conn, http: httpx.Client, market_ids: list[str], force: bool) -> None:
    """
    Шаг 3: скачать PM сделки для рынков из market_ids.
    Использует 20 потоков для параллельной загрузки.
    """
    import concurrent.futures
    import threading

    WORKERS = 20

    if not market_ids:
        print("[trades] нет рынков — пропуск")
        return

    # Дозаполняем condition_id если не хватает
    enrich_condition_ids(conn, http)

    placeholders = ",".join("?" * len(market_ids))
    markets = conn.execute(
        f"""SELECT market_id, condition_id, market_start, market_end
            FROM markets
            WHERE market_id IN ({placeholders})
              AND condition_id IS NOT NULL AND condition_id != ''
              AND winning_side IN ('yes', 'no')""",
        market_ids,
    ).fetchall()

    print(f"[trades] рынков с condition_id: {len(markets)}")

    existing_mids: set[str] = set()
    if not force:
        existing_mids = {r[0] for r in conn.execute(
            "SELECT DISTINCT market_id FROM pm_trades"
        ).fetchall()}
        print(f"[trades] уже есть трейды для {len(existing_mids)} рынков")

    # Подготовим задания (фильтруем уже скачанные)
    tasks: list[tuple[str, str, int, int]] = []
    skipped = 0
    parse_errors = 0
    for row in markets:
        mid, cid, ms_str, me_str = row
        if not force and mid in existing_mids:
            skipped += 1
            continue
        try:
            ms = int(datetime.strptime(ms_str, "%Y-%m-%d %H:%M:%S")
                     .replace(tzinfo=timezone.utc).timestamp())
            me = int(datetime.strptime(me_str, "%Y-%m-%d %H:%M:%S")
                     .replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            parse_errors += 1
            continue
        tasks.append((mid, cid, ms, me))

    print(f"[trades] к загрузке: {len(tasks)} | skip={skipped} | parse_err={parse_errors}")
    if not tasks:
        return

    # Каждый воркер — свой httpx.Client (не thread-safe)
    _local = threading.local()

    def _get_http() -> httpx.Client:
        if not hasattr(_local, "http"):
            _local.http = httpx.Client(timeout=15.0)
        return _local.http

    def _fetch_one(item: tuple[str, str, int, int]):
        mid, cid, ms, me = item
        try:
            trades = fetch_market_trades(cid, ms, me, _get_http())
            return (mid, trades, None)
        except Exception as e:
            return (mid, None, e)

    saved = errors = 0
    t0 = time.time()
    total = len(tasks)
    batch_rows: list[tuple] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for i, result in enumerate(pool.map(_fetch_one, tasks)):
            mid, trades, err = result

            if err is not None:
                errors += 1
                continue

            if trades:
                batch_rows.extend(
                    (mid, ts, outcome, price, size) for ts, outcome, price, size in trades
                )
            saved += 1

            # Пишем в DB пачками по 200 рынков
            if saved % 200 == 0 and batch_rows:
                if force:
                    mids_batch = {r[0] for r in batch_rows}
                    for m in mids_batch:
                        conn.execute("DELETE FROM pm_trades WHERE market_id = ?", (m,))
                conn.executemany(
                    "INSERT INTO pm_trades (market_id, ts, outcome, price, size) VALUES (?,?,?,?,?)",
                    batch_rows,
                )
                conn.commit()
                batch_rows.clear()

            if (saved + errors) % 200 == 0 or i == total - 1:
                elapsed = time.time() - t0
                done = saved + errors
                rate = done / elapsed if elapsed > 0 else 0
                remaining = (total - done) / rate / 60 if rate > 0 else 0
                print(f"  [{done}/{total}] saved={saved} err={errors} "
                      f"({rate:.1f}/s, ~{remaining:.0f}m left)", flush=True)

    # Дозаписываем остаток
    if batch_rows:
        if force:
            mids_batch = {r[0] for r in batch_rows}
            for m in mids_batch:
                conn.execute("DELETE FROM pm_trades WHERE market_id = ?", (m,))
        conn.executemany(
            "INSERT INTO pm_trades (market_id, ts, outcome, price, size) VALUES (?,?,?,?,?)",
            batch_rows,
        )
        conn.commit()

    print(f"[trades] готово: saved={saved} skipped={skipped} errors={errors}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Скачать данные для бэктеста: рынки + Binance 1s + PM сделки"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--days", type=int, help="последние N дней")
    group.add_argument("--from", dest="date_from", help="начало периода YYYY-MM-DD")

    parser.add_argument("--to", dest="date_to", help="конец периода YYYY-MM-DD (default: сегодня)")
    parser.add_argument("--symbols", nargs="+", default=list(ALLOWED_SYMBOLS),
                        help="символы (default: BTC ETH SOL XRP)")
    parser.add_argument("--force", action="store_true",
                        help="перескачать уже имеющиеся данные")
    parser.add_argument("--skip-binance", action="store_true",
                        help="пропустить загрузку Binance 1s данных")
    parser.add_argument("--skip-trades", action="store_true",
                        help="пропустить загрузку PM сделок")
    args = parser.parse_args()

    now_utc = datetime.utcnow()

    if args.days:
        date_from = now_utc - timedelta(days=args.days)
        date_to = now_utc
    else:
        date_from = datetime.strptime(args.date_from, "%Y-%m-%d")
        date_to = (datetime.strptime(args.date_to, "%Y-%m-%d")
                   if args.date_to else now_utc)

    symbols = set(s.upper() for s in args.symbols) & ALLOWED_SYMBOLS

    print(f"=== fetch_backtest_data ===")
    print(f"Период:  {date_from.strftime('%Y-%m-%d %H:%M')} → {date_to.strftime('%Y-%m-%d %H:%M')}")
    print(f"Символы: {sorted(symbols)}")
    print(f"Force:   {args.force}")
    print()

    conn = get_connection()
    http = httpx.Client(timeout=30.0)

    try:
        # Шаг 1: рынки
        print("── Шаг 1: рынки ──────────────────────────────")
        market_ids = step_markets(conn, http, date_from, date_to, symbols, args.force)
        print()

        # Шаг 2: Binance 1s
        if not args.skip_binance:
            print("── Шаг 2: Binance 1s klines ───────────────────")
            step_binance(conn, http, market_ids, args.force)
            print()

        # Шаг 3: PM сделки
        if not args.skip_trades:
            print("── Шаг 3: PM сделки ───────────────────────────")
            step_trades(conn, http, market_ids, args.force)
            print()

    finally:
        http.close()
        conn.close()

    print("=== готово ===")


if __name__ == "__main__":
    main()
