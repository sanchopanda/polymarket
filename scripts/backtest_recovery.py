"""
scripts/backtest_recovery.py

Бэктест recovery стратегии по локальной research DB.

Логика приближена к текущему recovery_bot:
  1. Ждём первое touch <= bottom_price
  2. Ждём activation_delay_seconds после touch
  3. Если первая цена после задержки:
     - > top_price: сетап считаем invalid (overshoot)
     - в диапазоне [entry_price, top_price]: считаем вход
  4. Последние CUTOFF_SECONDS рынка отсекаются (спайки резолюции).

Запуск:
    python3 scripts/backtest_recovery.py
    python3 scripts/backtest_recovery.py --interval 15 --entry-price 0.65 --detail
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import yaml

DB = "data/backtest.db"
CONFIG_PATH = "recovery_bot/config.yaml"


def ts_from_str(s: str) -> float:
    """Конвертирует 'YYYY-MM-DD HH:MM:SS' (UTC) в unix-timestamp."""
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()


def iso_week(s: str) -> str:
    """Возвращает 'YYYY-WNN' для строки даты."""
    dt = datetime.strptime(s[:10], "%Y-%m-%d")
    y, w, _ = dt.isocalendar()
    return f"{y}-W{w:02d}"


def load_recovery_config() -> dict:
    with Path(CONFIG_PATH).open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def interval_cfg(config: dict, interval: int) -> dict:
    strategy = config["strategy"]
    if interval == 5:
        raw = strategy["five_minute"]
    elif interval == 15:
        raw = strategy["fifteen_minute"]
    else:
        raise ValueError(f"unsupported interval: {interval}")
    return {
        "bottom_price": float(raw["bottom_price"]),
        "entry_price": float(raw["entry_price"]),
        "top_price": float(raw["top_price"]),
        "activation_delay_seconds": int(raw.get("activation_delay_seconds", 0)),
    }


def backtest(
    *,
    interval: int,
    stake: float,
    cutoff: int,
    bottom: float,
    entry_price: float,
    top_price: float,
    activation_delay_seconds: int,
    fee: float = 0.02,
    side_filter: str | None = None,
    min_touch_delay: float | None = None,
    max_touch_delay: float | None = None,
    min_ticks_in_zone: int = 1,
) -> None:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    markets = conn.execute(
        "SELECT market_id, symbol, market_end, winning_side "
        "FROM markets WHERE interval_minutes=? AND winning_side IS NOT NULL",
        (interval,),
    ).fetchall()

    # Counters
    total   = {"touched": 0, "filled": 0, "won": 0, "pnl": 0.0}
    by_side = {
        "no":  {"touched": 0, "filled": 0, "won": 0, "pnl": 0.0},
        "yes": {"touched": 0, "filled": 0, "won": 0, "pnl": 0.0},
    }
    by_week   = defaultdict(lambda: {"filled": 0, "won": 0, "pnl": 0.0})
    by_day    = defaultdict(lambda: {"filled": 0, "won": 0, "pnl": 0.0})
    by_sym    = defaultdict(lambda: {"touched": 0, "filled": 0, "won": 0, "pnl": 0.0})

    for mkt in markets:
        market_end_unix = ts_from_str(mkt["market_end"])
        cutoff_ts       = market_end_unix - cutoff
        winning_side    = mkt["winning_side"]
        symbol          = mkt["symbol"]
        week            = iso_week(mkt["market_end"])

        day = mkt["market_end"][:10]

        for outcome_label, side in [("Down", "no"), ("Up", "yes")]:
            if side_filter and side != side_filter:
                continue
            rows = conn.execute(
                "SELECT ts, price FROM pm_trades "
                "WHERE market_id=? AND outcome=? AND ts < ? ORDER BY ts",
                (mkt["market_id"], outcome_label, cutoff_ts),
            ).fetchall()
            if not rows:
                continue

            prices = [(r["ts"], r["price"]) for r in rows]

            # 1. First touch <= bottom
            touch = next(((ts, p) for ts, p in prices if p <= bottom), None)
            if touch is None:
                continue
            touch_ts = touch[0]

            total["touched"]      += 1
            by_side[side]["touched"] += 1
            by_sym[f"{symbol}_{side}"]["touched"] += 1

            # 2. После задержки ждём первую цену в окне входа; overshoot пропускаем.
            trigger_ts = touch_ts + activation_delay_seconds
            after_delay = [(ts, p) for ts, p in prices if ts >= trigger_ts]
            if not after_delay:
                continue

            fill_price = None
            fill_ts = None
            ticks_in_zone = 0
            for ts, p in after_delay:
                if p > top_price:
                    break
                if entry_price <= p <= top_price:
                    ticks_in_zone += 1
                    if ticks_in_zone >= min_ticks_in_zone:
                        fill_price = entry_price
                        fill_ts = ts
                        break
                else:
                    ticks_in_zone = 0
            if fill_price is None:
                continue

            # Фильтр по времени touch→fill
            touch_delay = fill_ts - touch_ts
            if min_touch_delay is not None and touch_delay < min_touch_delay:
                continue
            if max_touch_delay is not None and touch_delay > max_touch_delay:
                continue

            # Fill
            won  = (winning_side == side)
            shares = stake / fill_price
            pnl  = shares * (1 - fee) - stake if won else -stake

            total["filled"] += 1
            total["pnl"]    += pnl
            if won:
                total["won"] += 1

            by_side[side]["filled"] += 1
            by_side[side]["pnl"]    += pnl
            if won:
                by_side[side]["won"] += 1

            by_week[week]["filled"] += 1
            by_week[week]["pnl"]    += pnl
            if won:
                by_week[week]["won"] += 1

            by_day[day]["filled"] += 1
            by_day[day]["pnl"]    += pnl
            if won:
                by_day[day]["won"] += 1

            sk = f"{symbol}_{side}"
            by_sym[sk]["filled"] += 1
            by_sym[sk]["pnl"]    += pnl
            if won:
                by_sym[sk]["won"] += 1

    conn.close()

    def wr(d: dict) -> float:
        return d["won"] / d["filled"] * 100 if d["filled"] else 0.0

    def avg(d: dict) -> float:
        return d["pnl"] / d["filled"] if d["filled"] else 0.0

    fr = total["filled"] / total["touched"] * 100 if total["touched"] else 0

    print(f"\n{'='*62}")
    print(f"  {interval}m | stake=${stake:.2f} | cutoff={cutoff}s последних рынка")
    print(
        f"  touch<={bottom:.2f}  delay={activation_delay_seconds}s"
        f"  entry={entry_price:.2f}  top={top_price:.2f}"
    )
    print(f"{'='*62}")
    print(f"  Touches:  {total['touched']:>6}")
    print(f"  Fills:    {total['filled']:>6}  (fill rate {fr:.1f}%)")
    print(f"  WR:       {total['won']}/{total['filled']} = {wr(total):.1f}%")
    print(f"  PnL:      ${total['pnl']:>+.2f}")
    print(f"  Avg/fill: ${avg(total):>+.3f}")

    print(f"\n── По сайдам {'─'*40}")
    print(f"  {'Side':<6}  {'touched':>8}  {'filled':>7}  {'WR':>7}  {'PnL':>10}  {'avg':>8}")
    for side in ("no", "yes"):
        s = by_side[side]
        print(
            f"  {side.upper():<6}  {s['touched']:>8}  {s['filled']:>7}"
            f"  {wr(s):>6.1f}%  ${s['pnl']:>+9.2f}  ${avg(s):>+7.3f}"
        )
    print(f"  {'BOTH':<6}  {total['touched']:>8}  {total['filled']:>7}"
          f"  {wr(total):>6.1f}%  ${total['pnl']:>+9.2f}  ${avg(total):>+7.3f}")

    print(f"\n── По неделям {'─'*38}")
    print(f"  {'Неделя':<12}  {'fills':>6}  {'WR':>7}  {'PnL':>10}  {'avg':>8}")
    for week in sorted(by_week.keys()):
        d = by_week[week]
        print(
            f"  {week:<12}  {d['filled']:>6}  {wr(d):>6.1f}%"
            f"  ${d['pnl']:>+9.2f}  ${avg(d):>+7.3f}"
        )

    print(f"\n── По дням {'─'*42}")
    print(f"  {'День':<12}  {'fills':>6}  {'WR':>7}  {'PnL':>10}  {'avg':>8}")
    running = 0.0
    for day in sorted(by_day.keys()):
        d = by_day[day]
        running += d["pnl"]
        print(
            f"  {day:<12}  {d['filled']:>6}  {wr(d):>6.1f}%"
            f"  ${d['pnl']:>+9.2f}  ${avg(d):>+7.3f}  cumul=${running:>+8.2f}"
        )

    print(f"\n── По символам {'─'*36}")
    print(f"  {'Sym+Side':<14}  {'touched':>8}  {'fills':>6}  {'WR':>7}  {'PnL':>10}  {'avg':>8}")
    for key in sorted(by_sym.keys()):
        s = by_sym[key]
        if s["filled"] == 0:
            continue
        print(
            f"  {key:<14}  {s['touched']:>8}  {s['filled']:>6}  {wr(s):>6.1f}%"
            f"  ${s['pnl']:>+9.2f}  ${avg(s):>+7.3f}"
        )


def _compare_bottoms(
    *,
    interval: int,
    stake: float,
    cutoff: int,
    entry_price: float,
    top_price: float,
    activation_delay_seconds: int,
    fee: float = 0.02,
    bottoms: list[float] | None = None,
    side_filter: str | None = None,
    min_touch_delay: float | None = None,
    max_touch_delay: float | None = None,
) -> None:
    if bottoms is None:
        bottoms = [0.30, 0.35, 0.40, 0.45, 0.50]
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    markets = conn.execute(
        "SELECT market_id, symbol, market_end, winning_side "
        "FROM markets WHERE interval_minutes=? AND winning_side IS NOT NULL",
        (interval,),
    ).fetchall()
    conn.close()

    print(f"\n{'='*70}")
    print(f"  Сравнение bottom | {interval}m | stake=${stake:.2f} | cutoff={cutoff}s")
    print(
        f"  delay={activation_delay_seconds}s  entry={entry_price:.2f}"
        f"  top={top_price:.2f}  оба сайда (NO+YES)"
    )
    print(f"{'='*70}")
    print(f"  {'bottom':>8}  {'fills':>7}  {'WR':>7}  {'PnL':>10}  {'avg/fill':>9}  {'max DD':>8}")
    print(f"  {'-'*60}")

    for b in bottoms:
        conn = sqlite3.connect(DB)
        conn.row_factory = sqlite3.Row
        total = {"filled": 0, "won": 0, "pnl": 0.0}
        running_pnl = 0.0
        max_dd = 0.0
        peak = 0.0
        for mkt in markets:
            market_end_unix = ts_from_str(mkt["market_end"])
            cutoff_ts = market_end_unix - cutoff
            winning_side = mkt["winning_side"]
            for outcome_label, side in [("Down", "no"), ("Up", "yes")]:
                if side_filter and side != side_filter:
                    continue
                rows = conn.execute(
                    "SELECT ts, price FROM pm_trades "
                    "WHERE market_id=? AND outcome=? AND ts < ? ORDER BY ts",
                    (mkt["market_id"], outcome_label, cutoff_ts),
                ).fetchall()
                if not rows:
                    continue
                prices = [(r["ts"], r["price"]) for r in rows]
                touch = next(((ts, p) for ts, p in prices if p <= b), None)
                if touch is None:
                    continue
                trigger_ts = touch[0] + activation_delay_seconds
                after_delay = [(ts, p) for ts, p in prices if ts >= trigger_ts]
                if not after_delay:
                    continue
                fill_price = None
                fill_ts = None
                for ts, p in after_delay:
                    if p > top_price:
                        break
                    if entry_price <= p <= top_price:
                        fill_price = entry_price
                        fill_ts = ts
                        break
                if fill_price is None:
                    continue
                touch_delay = fill_ts - touch[0]
                if min_touch_delay is not None and touch_delay < min_touch_delay:
                    continue
                if max_touch_delay is not None and touch_delay > max_touch_delay:
                    continue
                won = (winning_side == side)
                pnl = (stake / fill_price) * (1 - fee) - stake if won else -stake
                total["filled"] += 1
                total["pnl"] += pnl
                if won:
                    total["won"] += 1
                running_pnl += pnl
                if running_pnl > peak:
                    peak = running_pnl
                dd = peak - running_pnl
                if dd > max_dd:
                    max_dd = dd
        conn.close()
        n = total["filled"]
        wr = total["won"] / n * 100 if n else 0
        avg = total["pnl"] / n if n else 0
        print(
            f"  ≤{b:.2f}      {n:>7}  {wr:>6.1f}%  ${total['pnl']:>+9.2f}"
            f"  ${avg:>+8.3f}  ${max_dd:>7.2f}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stake",    type=float, default=1.0)
    parser.add_argument("--cutoff",   type=int,   default=30)
    parser.add_argument("--fee",      type=float, default=0.02,
                        help="Комиссия на выигрыш (default 0.02 = 2%%)")
    parser.add_argument("--bottoms",  type=float, nargs="+", default=None,
                        help="Список bottom для сравнения (напр. 0.30 0.35 0.48)")
    parser.add_argument("--bottom",   type=float, default=None,
                        help="Фиксированное дно (если не задано — сравниваем несколько)")
    parser.add_argument("--entry-price", type=float, default=None,
                        help="Переопределить entry_price из recovery_bot/config.yaml")
    parser.add_argument("--top-price", type=float, default=None,
                        help="Переопределить top_price из recovery_bot/config.yaml")
    parser.add_argument("--interval", type=int,   default=None,
                        help="Только этот интервал (5 или 15)")
    parser.add_argument("--detail",   action="store_true",
                        help="Полный детальный вывод по неделям и символам")
    parser.add_argument("--side",           type=str,   default=None, choices=["no", "yes"],
                        help="Фильтр по стороне: no или yes")
    parser.add_argument("--min-touch-delay", type=float, default=None,
                        help="Мин. время от touch до fill (сек)")
    parser.add_argument("--max-touch-delay", type=float, default=None,
                        help="Макс. время от touch до fill (сек)")
    parser.add_argument("--min-ticks-in-zone", type=int, default=1,
                        help="Мин. последовательных тиков в [entry, top] перед входом (default 1)")
    opts = parser.parse_args()

    intervals = [opts.interval] if opts.interval else [5, 15]
    config = load_recovery_config()

    if opts.detail:
        for interval in intervals:
            cfg = interval_cfg(config, interval)
            backtest(
                interval=interval,
                stake=opts.stake,
                cutoff=opts.cutoff,
                bottom=opts.bottom if opts.bottom is not None else cfg["bottom_price"],
                entry_price=opts.entry_price if opts.entry_price is not None else cfg["entry_price"],
                top_price=opts.top_price if opts.top_price is not None else cfg["top_price"],
                activation_delay_seconds=cfg["activation_delay_seconds"],
                fee=opts.fee,
                side_filter=opts.side,
                min_touch_delay=opts.min_touch_delay,
                max_touch_delay=opts.max_touch_delay,
                min_ticks_in_zone=opts.min_ticks_in_zone,
            )
    elif opts.bottom is None:
        for interval in intervals:
            cfg = interval_cfg(config, interval)
            _compare_bottoms(
                interval=interval,
                stake=opts.stake,
                cutoff=opts.cutoff,
                entry_price=opts.entry_price if opts.entry_price is not None else cfg["entry_price"],
                top_price=opts.top_price if opts.top_price is not None else cfg["top_price"],
                activation_delay_seconds=cfg["activation_delay_seconds"],
                fee=opts.fee,
                bottoms=opts.bottoms,
                side_filter=opts.side,
                min_touch_delay=opts.min_touch_delay,
                max_touch_delay=opts.max_touch_delay,
            )
    else:
        for interval in intervals:
            cfg = interval_cfg(config, interval)
            backtest(
                interval=interval,
                stake=opts.stake,
                cutoff=opts.cutoff,
                bottom=opts.bottom,
                entry_price=opts.entry_price if opts.entry_price is not None else cfg["entry_price"],
                top_price=opts.top_price if opts.top_price is not None else cfg["top_price"],
                activation_delay_seconds=cfg["activation_delay_seconds"],
                fee=opts.fee,
                side_filter=opts.side,
                min_touch_delay=opts.min_touch_delay,
                max_touch_delay=opts.max_touch_delay,
                min_ticks_in_zone=opts.min_ticks_in_zone,
            )


if __name__ == "__main__":
    main()
