#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import httpx

from research_bot.fetch_markets import _parse_outcomes, _winning_side
from research_bot.fetch_trades import fetch_market_trades

RECOVERY_DB = "data/recovery_bot.db"
BACKTEST_DB = "data/backtest.db"
GAMMA_URL = "https://gamma-api.polymarket.com"


@dataclass(frozen=True)
class StrategyConfig:
    bottom: float = 0.38
    entry: float = 0.65
    top: float = 0.68
    confirm_delay: float = 0.2
    confirm_min: float = 0.60
    min_seconds_to_expiry: float = 20.0
    max_seconds_to_expiry: float = 240.0


@dataclass(frozen=True)
class AnalysisMarket:
    market_id: str
    symbol: str
    interval_minutes: int
    market_start_ts: int
    market_end_ts: int
    live_min_ts: int
    live_max_ts: int
    trade_count: int


@dataclass(frozen=True)
class Signal:
    side: str
    touch_ts: float
    armed_ts: float
    armed_price: float
    confirm_ts: float
    confirm_price: float


def utc_dt(ts: int | float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def iso_utc(ts: int | float) -> str:
    return utc_dt(ts).strftime("%Y-%m-%d %H:%M:%S")


def parse_any_dt(s: str) -> int:
    if "T" in s:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp())
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())


def load_market_bounds(
    recovery: sqlite3.Connection,
    backtest: sqlite3.Connection,
    market_ids: list[str],
    symbol: str,
    interval_minutes: int,
) -> dict[str, tuple[int, int]]:
    if not market_ids:
        return {}
    bounds: dict[str, tuple[int, int]] = {}

    rows = recovery.execute(
        f"""
        SELECT market_id, market_start, market_end
        FROM positions
        WHERE market_id IN ({','.join('?' for _ in market_ids)})
          AND symbol=?
          AND interval_minutes=?
        GROUP BY market_id, market_start, market_end
        """,
        [*market_ids, symbol, interval_minutes],
    ).fetchall()
    for row in rows:
        bounds[str(row["market_id"])] = (
            parse_any_dt(str(row["market_start"])),
            parse_any_dt(str(row["market_end"])),
        )

    missing = [mid for mid in market_ids if mid not in bounds]
    if missing:
        rows = backtest.execute(
            f"""
            SELECT market_id, market_start, market_end
            FROM markets
            WHERE market_id IN ({','.join('?' for _ in missing)})
              AND symbol=?
              AND interval_minutes=?
            """,
            [*missing, symbol, interval_minutes],
        ).fetchall()
        for row in rows:
            bounds[str(row["market_id"])] = (
                parse_any_dt(str(row["market_start"])),
                parse_any_dt(str(row["market_end"])),
            )
    return bounds


def iter_live_markets(
    recovery: sqlite3.Connection,
    backtest: sqlite3.Connection,
    symbol: str,
    interval_minutes: int,
    since_ts: int | None = None,
) -> list[AnalysisMarket]:
    if since_ts is not None:
        since_str = datetime.utcfromtimestamp(since_ts).strftime("%Y-%m-%d %H:%M:%S")
        rows = recovery.execute(
            """
            SELECT
                market_id,
                symbol,
                MIN(CAST(strftime('%s', ts) AS INTEGER)) AS min_ts,
                MAX(CAST(strftime('%s', ts) AS INTEGER)) AS max_ts,
                COUNT(*) AS trade_count
            FROM market_trade_history
            WHERE symbol = ? AND ts >= ?
            GROUP BY market_id, symbol
            ORDER BY min_ts
            """,
            (symbol, since_str),
        ).fetchall()
    else:
        rows = recovery.execute(
            """
            SELECT
                market_id,
                symbol,
                MIN(CAST(strftime('%s', ts) AS INTEGER)) AS min_ts,
                MAX(CAST(strftime('%s', ts) AS INTEGER)) AS max_ts,
                COUNT(*) AS trade_count
            FROM market_trade_history
            WHERE symbol = ?
            GROUP BY market_id, symbol
            ORDER BY min_ts
            """,
            (symbol,),
        ).fetchall()
    market_ids = [str(row["market_id"]) for row in rows]
    bounds = load_market_bounds(recovery, backtest, market_ids, symbol, interval_minutes)

    eligible: list[AnalysisMarket] = []
    bucket_seconds = interval_minutes * 60
    for row in rows:
        market_id = str(row["market_id"])
        min_ts = int(row["min_ts"])
        max_ts = int(row["max_ts"])
        bound = bounds.get(market_id)
        if bound is None:
            market_start_ts = (min_ts // bucket_seconds) * bucket_seconds
            market_end_ts = market_start_ts + bucket_seconds
        else:
            market_start_ts, market_end_ts = bound
        if since_ts is not None and market_end_ts < since_ts:
            continue
        eligible.append(
            AnalysisMarket(
                market_id=market_id,
                symbol=str(row["symbol"]),
                interval_minutes=interval_minutes,
                market_start_ts=market_start_ts,
                market_end_ts=market_end_ts,
                live_min_ts=min_ts,
                live_max_ts=max_ts,
                trade_count=int(row["trade_count"]),
            )
        )
    return eligible


def fetch_gamma_market(http: httpx.Client, market_id: str) -> dict | None:
    try:
        resp = http.get(f"{GAMMA_URL}/markets/{market_id}", timeout=20.0)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data[0] if data else None
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def ensure_backtest_markets(
    backtest: sqlite3.Connection,
    eligible: Iterable[AnalysisMarket],
    sync: bool,
) -> dict[str, int]:
    stats = {
        "eligible": 0,
        "missing_market": 0,
        "missing_trades": 0,
        "inserted_markets": 0,
        "inserted_trades": 0,
        "trade_rows_inserted": 0,
    }
    by_id = {m.market_id: m for m in eligible}
    stats["eligible"] = len(by_id)
    existing = {
        row["market_id"]: row
        for row in backtest.execute(
            f"SELECT market_id, condition_id FROM markets WHERE market_id IN ({','.join('?' for _ in by_id)})",
            list(by_id),
        ).fetchall()
    } if by_id else {}

    trade_presence = {
        row["market_id"]
        for row in backtest.execute(
            f"SELECT DISTINCT market_id FROM pm_trades WHERE market_id IN ({','.join('?' for _ in by_id)})",
            list(by_id),
        ).fetchall()
    } if by_id else set()

    missing_market_ids = [mid for mid in by_id if mid not in existing]
    missing_trade_ids = [mid for mid in by_id if mid not in trade_presence]
    stats["missing_market"] = len(missing_market_ids)
    stats["missing_trades"] = len(missing_trade_ids)

    if not sync or not by_id:
        return stats

    http = httpx.Client(timeout=20.0)
    try:
        for market_id in by_id:
            market = by_id[market_id]
            row = existing.get(market_id)
            condition_id = ""
            if row is not None:
                condition_id = str(row["condition_id"] or "")

            raw = None
            if row is None or not condition_id or market_id in trade_presence:
                raw = fetch_gamma_market(http, market_id)
                if raw:
                    condition_id = str(raw.get("conditionId") or condition_id or "")

            if row is None:
                winning_side = None
                if raw:
                    outcomes, prices = _parse_outcomes(raw)
                    if outcomes and prices:
                        winning_side = _winning_side(outcomes, prices)
                backtest.execute(
                    """
                    INSERT OR IGNORE INTO markets
                    (market_id, condition_id, symbol, interval_minutes, market_start, market_end, winning_side)
                    VALUES (?,?,?,?,?,?,?)
                    """,
                    (
                        market_id,
                        condition_id,
                        market.symbol,
                        market.interval_minutes,
                        iso_utc(market.market_start_ts),
                        iso_utc(market.market_end_ts),
                        winning_side,
                    ),
                )
                backtest.commit()
                stats["inserted_markets"] += 1
            elif condition_id and not row["condition_id"]:
                backtest.execute(
                    "UPDATE markets SET condition_id=? WHERE market_id=?",
                    (condition_id, market_id),
                )
                backtest.commit()

            if market_id in trade_presence:
                continue
            if not condition_id:
                continue
            trades = fetch_market_trades(
                condition_id=condition_id,
                market_start_ts=market.market_start_ts,
                market_end_ts=market.market_end_ts,
                http=http,
            )
            if not trades:
                continue
            backtest.executemany(
                "INSERT INTO pm_trades (market_id, ts, outcome, price, size) VALUES (?,?,?,?,?)",
                [(market_id, ts, outcome, price, size) for ts, outcome, price, size in trades],
            )
            backtest.commit()
            stats["inserted_trades"] += 1
            stats["trade_rows_inserted"] += len(trades)
    finally:
        http.close()
    return stats


def run_strategy(
    rows_by_side: dict[str, list[tuple[float, float]]],
    market_end_ts: float,
    cfg: StrategyConfig,
) -> Signal | None:
    candidates: dict[str, Signal] = {}
    for side in ("yes", "no"):
        rows = rows_by_side.get(side, [])
        touch: tuple[float, float] | None = None
        armed: tuple[float, float] | None = None
        for ts, price in rows:
            seconds_left = market_end_ts - ts
            if seconds_left > 300:
                continue
            if touch is None:
                if price <= cfg.bottom:
                    touch = (ts, price)
                continue
            if price > cfg.top:
                touch = None
                continue
            if price < cfg.entry:
                continue
            if seconds_left > cfg.max_seconds_to_expiry or seconds_left < cfg.min_seconds_to_expiry:
                touch = None
                continue
            armed = (ts, price)
            break
        if armed is None or touch is None:
            continue
        confirm = next(((ts, price) for ts, price in rows if ts >= armed[0] + cfg.confirm_delay), None)
        if confirm is None or confirm[1] < cfg.confirm_min:
            continue
        candidates[side] = Signal(
            side=side,
            touch_ts=touch[0],
            armed_ts=armed[0],
            armed_price=armed[1],
            confirm_ts=confirm[0],
            confirm_price=confirm[1],
        )
    if not candidates:
        return None
    return min(candidates.values(), key=lambda item: item.armed_ts)


def load_live_trade_signal(
    recovery: sqlite3.Connection,
    market_id: str,
    market_end_ts: int,
    cfg: StrategyConfig,
) -> Signal | None:
    rows_by_side: dict[str, list[tuple[float, float]]] = {"yes": [], "no": []}
    for side in ("yes", "no"):
        rows = recovery.execute(
            """
            SELECT CAST(strftime('%s', ts) AS INTEGER) AS ts, price
            FROM market_trade_history
            WHERE market_id=? AND side=?
            ORDER BY ts
            """,
            (market_id, side),
        ).fetchall()
        rows_by_side[side] = [(float(row["ts"]), float(row["price"])) for row in rows]
    return run_strategy(rows_by_side, float(market_end_ts), cfg)


def load_backtest_trade_signal(
    backtest: sqlite3.Connection,
    market_id: str,
    market_end_ts: int,
    cfg: StrategyConfig,
) -> Signal | None:
    rows_by_side: dict[str, list[tuple[float, float]]] = {"yes": [], "no": []}
    for outcome, side in (("Up", "yes"), ("Down", "no")):
        rows = backtest.execute(
            """
            SELECT ts, price
            FROM pm_trades
            WHERE market_id=? AND outcome=?
            ORDER BY ts
            """,
            (market_id, outcome),
        ).fetchall()
        rows_by_side[side] = [(float(row["ts"]), float(row["price"])) for row in rows]
    return run_strategy(rows_by_side, float(market_end_ts), cfg)


def load_market_winners(
    recovery: sqlite3.Connection,
    backtest: sqlite3.Connection,
    market_ids: list[str],
    symbol: str,
    interval_minutes: int,
) -> dict[str, str]:
    if not market_ids:
        return {}
    winners: dict[str, str] = {}
    rows = recovery.execute(
        f"""
        SELECT market_id, winning_side
        FROM positions
        WHERE market_id IN ({','.join('?' for _ in market_ids)})
          AND symbol=?
          AND interval_minutes=?
          AND winning_side IS NOT NULL
        GROUP BY market_id, winning_side
        """,
        [*market_ids, symbol, interval_minutes],
    ).fetchall()
    for row in rows:
        winners[str(row["market_id"])] = str(row["winning_side"])

    missing = [mid for mid in market_ids if mid not in winners]
    if missing:
        rows = backtest.execute(
            f"SELECT market_id, winning_side FROM markets WHERE market_id IN ({','.join('?' for _ in missing)})",
            missing,
        ).fetchall()
        for row in rows:
            if row["winning_side"]:
                winners[str(row["market_id"])] = str(row["winning_side"])
    return winners


def load_real_live_positions(
    recovery: sqlite3.Connection,
    market_ids: set[str],
    symbol: str,
    interval_minutes: int,
) -> dict[str, sqlite3.Row]:
    if not market_ids:
        return {}
    rows = recovery.execute(
        f"""
        SELECT market_id, side, winning_side, opened_at, status, filled_shares
        FROM positions
        WHERE market_id IN ({','.join('?' for _ in market_ids)})
          AND mode='real'
          AND symbol=?
          AND interval_minutes=?
          AND status='resolved'
          AND filled_shares > 0
        ORDER BY opened_at
        """,
        [*market_ids, symbol, interval_minutes],
    ).fetchall()
    first_by_market: dict[str, sqlite3.Row] = {}
    for row in rows:
        first_by_market.setdefault(str(row["market_id"]), row)
    return first_by_market


def summarize_results(name: str, results: dict[str, tuple[str, bool]]) -> list[str]:
    fills = len(results)
    wins = sum(1 for _, won in results.values() if won)
    wr = wins / fills * 100 if fills else 0.0
    return [f"{name}: signals={fills} wins={wins} WR={wr:.2f}%"]


def summarize_breakdown(
    live_results: dict[str, tuple[str, bool]],
    backtest_results: dict[str, tuple[str, bool]],
) -> list[str]:
    groups: defaultdict[str, list[str]] = defaultdict(list)
    all_ids = sorted(set(live_results) | set(backtest_results))
    for market_id in all_ids:
        live = live_results.get(market_id)
        backtest = backtest_results.get(market_id)
        if live and backtest:
            if live[0] == backtest[0]:
                groups["same_signal"].append(market_id)
            else:
                groups["different_side"].append(market_id)
        elif live:
            groups["live_only"].append(market_id)
        else:
            groups["backtest_only"].append(market_id)

    lines = ["Breakdown:"]
    for group in ("same_signal", "different_side", "live_only", "backtest_only"):
        market_ids = groups.get(group, [])
        if group == "different_side":
            live_wins = sum(1 for mid in market_ids if live_results[mid][1])
            backtest_wins = sum(1 for mid in market_ids if backtest_results[mid][1])
            live_wr = live_wins / len(market_ids) * 100 if market_ids else 0.0
            bt_wr = backtest_wins / len(market_ids) * 100 if market_ids else 0.0
            lines.append(
                f"- {group}: markets={len(market_ids)}"
                f" | live_wins={live_wins} WR={live_wr:.2f}%"
                f" | backtest_wins={backtest_wins} WR={bt_wr:.2f}%"
            )
            continue
        source = live_results if group != "backtest_only" else backtest_results
        wins = sum(1 for mid in market_ids if source[mid][1])
        wr = wins / len(market_ids) * 100 if market_ids else 0.0
        lines.append(f"- {group}: markets={len(market_ids)} wins={wins} WR={wr:.2f}%")
    return lines


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare recovery live-trades, backtest-trades and real-live WR on the same BTC 5m markets."
    )
    parser.add_argument("--recovery-db", default=RECOVERY_DB)
    parser.add_argument("--backtest-db", default=BACKTEST_DB)
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--interval", type=int, default=5)
    parser.add_argument("--bottom", type=float, default=0.38)
    parser.add_argument("--entry", type=float, default=0.65)
    parser.add_argument("--top", type=float, default=0.68)
    parser.add_argument("--confirm-delay", type=float, default=0.2)
    parser.add_argument("--confirm-min", type=float, default=0.60)
    parser.add_argument("--min-seconds-to-expiry", type=float, default=20.0)
    parser.add_argument("--max-seconds-to-expiry", type=float, default=240.0)
    parser.add_argument("--no-sync-backtest", action="store_true")
    parser.add_argument("--hours", type=float, default=None,
                        help="ограничить окно последними N часами")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    cfg = StrategyConfig(
        bottom=args.bottom,
        entry=args.entry,
        top=args.top,
        confirm_delay=args.confirm_delay,
        confirm_min=args.confirm_min,
        min_seconds_to_expiry=args.min_seconds_to_expiry,
        max_seconds_to_expiry=args.max_seconds_to_expiry,
    )

    recovery = sqlite3.connect(args.recovery_db)
    recovery.row_factory = sqlite3.Row
    backtest = sqlite3.connect(args.backtest_db)
    backtest.row_factory = sqlite3.Row

    try:
        since_ts = int(datetime.utcnow().timestamp() - args.hours * 3600) if args.hours else None
        eligible = iter_live_markets(
            recovery,
            backtest,
            symbol=args.symbol,
            interval_minutes=args.interval,
            since_ts=since_ts,
        )
        sync_stats = ensure_backtest_markets(
            backtest,
            eligible,
            sync=not args.no_sync_backtest,
        )

        market_ids = [m.market_id for m in eligible]
        winners = load_market_winners(
            recovery,
            backtest,
            market_ids,
            symbol=args.symbol,
            interval_minutes=args.interval,
        )

        live_results: dict[str, tuple[str, bool]] = {}
        backtest_results: dict[str, tuple[str, bool]] = {}
        for market in eligible:
            winning_side = winners.get(market.market_id)
            if not winning_side:
                continue
            live_signal = load_live_trade_signal(recovery, market.market_id, market.market_end_ts, cfg)
            if live_signal is not None:
                live_results[market.market_id] = (
                    live_signal.side,
                    live_signal.side == winning_side,
                )
            backtest_signal = load_backtest_trade_signal(backtest, market.market_id, market.market_end_ts, cfg)
            if backtest_signal is not None:
                backtest_results[market.market_id] = (
                    backtest_signal.side,
                    backtest_signal.side == winning_side,
                )

        real_rows = load_real_live_positions(
            recovery,
            market_ids=set(market_ids),
            symbol=args.symbol,
            interval_minutes=args.interval,
        )
        real_results = {
            market_id: (str(row["side"]), str(row["side"]) == str(row["winning_side"]))
            for market_id, row in real_rows.items()
            if row["winning_side"]
        }

        print(
            f"Recovery live-vs-backtest analysis | symbol={args.symbol} interval={args.interval}m"
        )
        if eligible:
            print(
                f"Live-trade window: {iso_utc(min(m.market_start_ts for m in eligible))} UTC"
                f" .. {iso_utc(max(m.market_end_ts for m in eligible))} UTC"
            )
        print(
            f"Markets with live trade history: {len(eligible)}"
            f" | synced markets={sync_stats['inserted_markets']}"
            f" | synced trades={sync_stats['inserted_trades']}"
            f" | inserted pm_trades rows={sync_stats['trade_rows_inserted']}"
        )
        print(
            f"Backtest coverage before sync: missing_markets={sync_stats['missing_market']}"
            f" missing_trades={sync_stats['missing_trades']}"
        )
        print(
            "Config:"
            f" bottom={cfg.bottom:.2f}"
            f" entry={cfg.entry:.2f}"
            f" top={cfg.top:.2f}"
            f" confirm_delay={cfg.confirm_delay:.1f}s"
            f" confirm_min={cfg.confirm_min:.2f}"
            f" min_expiry={cfg.min_seconds_to_expiry:.0f}s"
            f" max_expiry={cfg.max_seconds_to_expiry:.0f}s"
        )
        for line in summarize_results("Live trade-based", live_results):
            print(line)
        for line in summarize_results("Backtest trade-based", backtest_results):
            print(line)
        for line in summarize_results("Real live", real_results):
            print(line)
        print(
            f"Overlap live/backtest signals: {len(set(live_results) & set(backtest_results))}"
            f" | live-only={len(set(live_results) - set(backtest_results))}"
            f" | backtest-only={len(set(backtest_results) - set(live_results))}"
        )
        for line in summarize_breakdown(live_results, backtest_results):
            print(line)
    finally:
        recovery.close()
        backtest.close()


if __name__ == "__main__":
    main()
