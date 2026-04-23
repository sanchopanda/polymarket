#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone

import httpx

from research_bot.backtest_db import get_connection
from research_bot.fetch_trades import enrich_condition_ids, fetch_market_trades


def parse_ts(s: str) -> int:
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill pm_trades.size in data/backtest.db")
    parser.add_argument("--symbol", default=None, help="например BTC")
    parser.add_argument("--interval", type=int, default=None, help="например 5")
    parser.add_argument("--limit", type=int, default=None, help="максимум рынков")
    parser.add_argument(
        "--only-missing-size",
        action="store_true",
        help="обрабатывать только рынки, где есть pm_trades с size IS NULL",
    )
    parser.add_argument(
        "--force-market-ids",
        nargs="*",
        default=None,
        help="явный список market_id для backfill",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    conn = get_connection()
    http = httpx.Client(timeout=20.0)

    try:
        enrich_condition_ids(conn, http)

        query = """
        SELECT m.market_id, m.condition_id, m.market_start, m.market_end
        FROM markets m
        WHERE m.condition_id IS NOT NULL AND m.condition_id != ''
        """
        params: list = []

        if args.force_market_ids:
            query += f" AND m.market_id IN ({','.join('?' for _ in args.force_market_ids)})"
            params.extend(args.force_market_ids)
        else:
            if args.symbol:
                query += " AND m.symbol=?"
                params.append(args.symbol)
            if args.interval is not None:
                query += " AND m.interval_minutes=?"
                params.append(args.interval)
            if args.only_missing_size:
                query += """
                AND EXISTS (
                    SELECT 1 FROM pm_trades p
                    WHERE p.market_id = m.market_id AND p.size IS NULL
                )
                """

        query += " ORDER BY m.market_end"
        if args.limit is not None:
            query += " LIMIT ?"
            params.append(args.limit)

        markets = conn.execute(query, params).fetchall()
        print(f"markets_to_backfill={len(markets)}")

        done = 0
        skipped = 0
        rows_written = 0

        for row in markets:
            market_id = str(row["market_id"])
            start_ts = parse_ts(str(row["market_start"]))
            end_ts = parse_ts(str(row["market_end"]))
            trades = fetch_market_trades(str(row["condition_id"]), start_ts, end_ts, http)
            if not trades:
                skipped += 1
                continue

            conn.execute("DELETE FROM pm_trades WHERE market_id=?", (market_id,))
            conn.executemany(
                "INSERT INTO pm_trades (market_id, ts, outcome, price, size) VALUES (?,?,?,?,?)",
                [(market_id, ts, outcome, price, size) for ts, outcome, price, size in trades],
            )
            conn.commit()

            done += 1
            rows_written += len(trades)
            if done % 20 == 0:
                print(f"  done={done} skipped={skipped} rows={rows_written}", flush=True)

        print(f"backfill_done={done} skipped={skipped} rows_written={rows_written}")
    finally:
        http.close()
        conn.close()


if __name__ == "__main__":
    main()
