"""
scripts/backtest_scalp.py

Бэктест скальп-стратегии на Polymarket Oracle рынках:
  - Покупаем сторону в «дешёвой» зоне (например 10–25¢).
  - TP на целевой цене (например +20¢ к входу, либо абсолют 45¢).
  - Опционально SL.
  - Выход форс по TTE (не ждём резолюции).
  - Опциональный триггер: сторона должна просесть на ≥X¢ за Y сек до входа.

Комиссия Polymarket: fee = shares * price * 0.25 * (price*(1-price))^2
Списывается и на входе, и на выходе.

Данные: data/backtest.db (pm_trades + markets с winning_side).

Использование:
    python scripts/backtest_scalp.py --all              # прогнать все пресеты
    python scripts/backtest_scalp.py --variant 1        # один пресет
    python scripts/backtest_scalp.py --variant 1 --sample 200 --detail
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

DB = Path(__file__).resolve().parent.parent / "data" / "backtest.db"
STAKE_USD = 10.0
SLIPPAGE = 0.01  # ¢ slippage on entry (+) and exit (-)


# ── fee ─────────────────────────────────────────────────────────────
def pm_fee(shares: float, price: float) -> float:
    """Polymarket fee: peaks ~1.56% at 0.5, ~0.64% at 0.2, ~0% at extremes."""
    return shares * price * 0.25 * ((price * (1 - price)) ** 2)


def ts_from_str(s: str) -> int:
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
               .replace(tzinfo=timezone.utc).timestamp())


# ── strategy params ────────────────────────────────────────────────
@dataclass
class Variant:
    name: str
    symbols: tuple[str, ...] | None = None     # None = all
    intervals: tuple[int, ...] = (5,)
    side: str = "both"                          # "up" | "down" | "both"
    entry_lo: float = 0.10
    entry_hi: float = 0.25
    tp_mode: str = "delta"                      # "delta" | "abs"
    tp_value: float = 0.20
    sl_delta: float | None = None               # e.g. 0.10 → cut at entry-0.10
    exit_tte_sec: int = 30
    enter_min_tte_sec: int = 60                 # не входим если до экспирации меньше
    trigger: str = "none"                       # "none" | "drop"
    drop_cents: float = 0.10
    drop_window_sec: int = 30
    fee_on: bool = True
    slippage: float = SLIPPAGE
    stake: float = STAKE_USD


# ── simulation ──────────────────────────────────────────────────────
@dataclass
class TradeResult:
    market_id: str
    side: str
    entry_ts: int
    exit_ts: int
    entry_price: float
    exit_price: float
    exit_reason: str                            # "tp" | "sl" | "time"
    pnl_usd: float
    fees_usd: float
    shares: float


def simulate_market(
    trades: list[tuple[int, str, float]],
    market_start_ts: int,
    market_end_ts: int,
    v: Variant,
) -> TradeResult | None:
    """Один проход по трейдам рынка. Возвращает TradeResult или None."""
    # для триггера 'drop' — скользящее окно max-цены за drop_window_sec
    window: dict[str, deque[tuple[int, float]]] = {"Up": deque(), "Down": deque()}
    last_price: dict[str, float] = {}
    in_pos = False
    our_side: str | None = None
    entry_price = entry_ts = entry_shares = entry_fee = 0.0

    allowed_sides: set[str]
    if v.side == "up":
        allowed_sides = {"Up"}
    elif v.side == "down":
        allowed_sides = {"Down"}
    else:
        allowed_sides = {"Up", "Down"}

    for ts, outcome, price in trades:
        if ts < market_start_ts:
            continue
        tte = market_end_ts - ts
        if tte < 0:
            tte = 0

        last_price[outcome] = price

        # обновляем окно цены для триггера
        if v.trigger == "drop" and outcome in allowed_sides:
            w = window[outcome]
            w.append((ts, price))
            cutoff = ts - v.drop_window_sec
            while w and w[0][0] < cutoff:
                w.popleft()

        # форс-выход по TTE работает на ЛЮБОМ тике (не только нашей стороны)
        if in_pos and tte <= v.exit_tte_sec:
            exit_ref = last_price.get(our_side, entry_price)
            fill = max(0.0, exit_ref - v.slippage)
            fee_out = pm_fee(entry_shares, fill) if v.fee_on else 0.0
            pnl = entry_shares * fill - v.stake - entry_fee - fee_out
            return TradeResult(
                market_id="", side=our_side, entry_ts=entry_ts, exit_ts=ts,
                entry_price=entry_price, exit_price=fill, exit_reason="time",
                pnl_usd=pnl, fees_usd=entry_fee + fee_out, shares=entry_shares,
            )

        if not in_pos:
            if outcome not in allowed_sides:
                continue
            if tte <= v.enter_min_tte_sec:
                continue
            if not (v.entry_lo <= price <= v.entry_hi):
                continue

            # триггер
            if v.trigger == "drop":
                w = window[outcome]
                if not w:
                    continue
                peak = max(p for _, p in w)
                if peak - price < v.drop_cents:
                    continue

            # вход
            fill = min(1.0, price + v.slippage)
            shares = v.stake / fill
            fee_in = pm_fee(shares, fill) if v.fee_on else 0.0
            our_side = outcome
            entry_price = fill
            entry_ts = ts
            entry_shares = shares
            entry_fee = fee_in
            in_pos = True
            continue

        # in position — TP / SL проверяем только на тиках нашей стороны
        if outcome != our_side:
            continue

        # TP
        if v.tp_mode == "delta":
            tp_trigger = entry_price + v.tp_value
        else:
            tp_trigger = v.tp_value
        if price >= tp_trigger:
            fill = max(0.0, price - v.slippage)
            fee_out = pm_fee(entry_shares, fill) if v.fee_on else 0.0
            pnl = entry_shares * fill - v.stake - entry_fee - fee_out
            return TradeResult(
                market_id="", side=our_side, entry_ts=entry_ts, exit_ts=ts,
                entry_price=entry_price, exit_price=fill, exit_reason="tp",
                pnl_usd=pnl, fees_usd=entry_fee + fee_out, shares=entry_shares,
            )

        # SL
        if v.sl_delta is not None and price <= entry_price - v.sl_delta:
            fill = max(0.0, price - v.slippage)
            fee_out = pm_fee(entry_shares, fill) if v.fee_on else 0.0
            pnl = entry_shares * fill - v.stake - entry_fee - fee_out
            return TradeResult(
                market_id="", side=our_side, entry_ts=entry_ts, exit_ts=ts,
                entry_price=entry_price, exit_price=fill, exit_reason="sl",
                pnl_usd=pnl, fees_usd=entry_fee + fee_out, shares=entry_shares,
            )

    # если дошли сюда в позиции — TTE так и не просела до порога,
    # значит market_end прошёл без exit-тика. Используем последнюю цену нашей стороны.
    if in_pos and our_side in last_price:
        fill = max(0.0, last_price[our_side] - v.slippage)
        fee_out = pm_fee(entry_shares, fill) if v.fee_on else 0.0
        pnl = entry_shares * fill - v.stake - entry_fee - fee_out
        return TradeResult(
            market_id="", side=our_side, entry_ts=entry_ts, exit_ts=trades[-1][0],
            entry_price=entry_price, exit_price=fill, exit_reason="time",
            pnl_usd=pnl, fees_usd=entry_fee + fee_out, shares=entry_shares,
        )

    return None


# ── driver ──────────────────────────────────────────────────────────
@dataclass
class RunStats:
    n_markets: int = 0
    n_trades: int = 0
    wins: int = 0
    total_pnl: float = 0.0
    total_fees: float = 0.0
    exit_reasons: Counter = field(default_factory=Counter)
    hold_secs: list[int] = field(default_factory=list)
    pnl_samples: list[float] = field(default_factory=list)


def run_backtest(v: Variant, sample: int | None = None,
                 detail: bool = False) -> RunStats:
    return run_many([v], sample=sample, detail=detail)[v.name]


def run_many(variants: list[Variant], sample: int | None = None,
             detail: bool = False) -> dict[str, RunStats]:
    """Один проход по данным для всех вариантов с одинаковым interval-фильтром."""
    # группируем варианты по (intervals, symbols) — общая выборка рынков
    groups: dict[tuple, list[Variant]] = {}
    for v in variants:
        key = (v.intervals, v.symbols)
        groups.setdefault(key, []).append(v)

    out: dict[str, RunStats] = {v.name: RunStats() for v in variants}
    con = sqlite3.connect(str(DB))
    con.row_factory = sqlite3.Row

    for (intervals, symbols), vs in groups.items():
        q = ("SELECT market_id, symbol, interval_minutes, market_start, market_end "
             "FROM markets WHERE winning_side IS NOT NULL")
        params: list = []
        if intervals:
            q += f" AND interval_minutes IN ({','.join('?' for _ in intervals)})"
            params.extend(intervals)
        if symbols:
            q += f" AND symbol IN ({','.join('?' for _ in symbols)})"
            params.extend(symbols)
        q += " ORDER BY market_start"
        if sample:
            q += f" LIMIT {sample}"

        markets = con.execute(q, params).fetchall()
        total = len(markets)
        print(f"[group intervals={intervals} symbols={symbols}] "
              f"{total} markets, {len(vs)} variants")

        for i, m in enumerate(markets, 1):
            mid = m["market_id"]
            market_start_ts = ts_from_str(m["market_start"])
            market_end_ts = ts_from_str(m["market_end"])
            rows = con.execute(
                "SELECT ts, outcome, price FROM pm_trades "
                "WHERE market_id=? ORDER BY ts",
                (mid,),
            ).fetchall()
            if not rows:
                continue
            # clamp prices to [0, 1] — в БД встречаются выбросы (до 714!)
            trades = [(r[0], r[1], max(0.0, min(1.0, r[2]))) for r in rows
                      if r[2] is not None]

            for v in vs:
                stats = out[v.name]
                stats.n_markets += 1
                res = simulate_market(trades, market_start_ts, market_end_ts, v)
                if res is None:
                    continue
                stats.n_trades += 1
                stats.total_pnl += res.pnl_usd
                stats.total_fees += res.fees_usd
                stats.exit_reasons[res.exit_reason] += 1
                stats.hold_secs.append(res.exit_ts - res.entry_ts)
                stats.pnl_samples.append(res.pnl_usd)
                if res.pnl_usd > 0:
                    stats.wins += 1

            if i % 1000 == 0:
                print(f"  ... {i}/{total}", flush=True)

    con.close()
    return out


def format_stats(v: Variant, s: RunStats) -> str:
    if s.n_trades == 0:
        return (f"[{v.name}] scanned {s.n_markets} markets, no trades entered")
    wr = s.wins / s.n_trades * 100
    avg = s.total_pnl / s.n_trades
    avg_hold = sum(s.hold_secs) / len(s.hold_secs)
    er = " ".join(f"{k}={v}" for k, v in s.exit_reasons.most_common())
    return (
        f"[{v.name}]\n"
        f"  markets={s.n_markets}  trades={s.n_trades}  "
        f"fill_rate={s.n_trades / max(1,s.n_markets) * 100:.1f}%\n"
        f"  WR={wr:.1f}%  avg_pnl=${avg:+.4f}  total_pnl=${s.total_pnl:+.2f}  "
        f"fees=${s.total_fees:.2f}\n"
        f"  avg_hold={avg_hold:.0f}s  exits: {er}"
    )


# ── presets ────────────────────────────────────────────────────────
VARIANTS: list[Variant] = [
    # ── базовая сетка по 5m, без триггера ──
    Variant(name="v1_5m_both_1025_tp20",
            entry_lo=0.10, entry_hi=0.25, tp_mode="delta", tp_value=0.20),
    Variant(name="v2_5m_both_1530_tp15",
            entry_lo=0.15, entry_hi=0.30, tp_mode="delta", tp_value=0.15),
    Variant(name="v3_5m_both_1025_tp45abs",
            entry_lo=0.10, entry_hi=0.25, tp_mode="abs", tp_value=0.45),
    Variant(name="v4_5m_both_1025_tp20_sl10",
            entry_lo=0.10, entry_hi=0.25, tp_mode="delta", tp_value=0.20,
            sl_delta=0.10),

    # ── триггер "ловим отскок": сторона упала ≥X за Y сек ──
    Variant(name="v5_5m_trig_drop10w30",
            entry_lo=0.10, entry_hi=0.25, tp_mode="delta", tp_value=0.20,
            trigger="drop", drop_cents=0.10, drop_window_sec=30),
    Variant(name="v6_5m_trig_drop15w60",
            entry_lo=0.10, entry_hi=0.25, tp_mode="delta", tp_value=0.20,
            trigger="drop", drop_cents=0.15, drop_window_sec=60),

    # ── 15m рынки ──
    Variant(name="v7_15m_both_1025_tp20",
            intervals=(15,), entry_lo=0.10, entry_hi=0.25,
            tp_mode="delta", tp_value=0.20,
            enter_min_tte_sec=120, exit_tte_sec=60),
    Variant(name="v8_15m_trig_drop10w30",
            intervals=(15,), entry_lo=0.10, entry_hi=0.25,
            tp_mode="delta", tp_value=0.20,
            trigger="drop", drop_cents=0.10, drop_window_sec=30,
            enter_min_tte_sec=120, exit_tte_sec=60),

    # ── только UP ──
    Variant(name="v9_5m_up_1025_tp20",
            side="up",
            entry_lo=0.10, entry_hi=0.25, tp_mode="delta", tp_value=0.20),

    # ── узкая зона / широкий TP ──
    Variant(name="v10_5m_both_1020_tp25",
            entry_lo=0.10, entry_hi=0.20, tp_mode="delta", tp_value=0.25),
    Variant(name="v11_5m_both_2030_tp15",
            entry_lo=0.20, entry_hi=0.30, tp_mode="delta", tp_value=0.15),

    # ── benchmark: без TP, только time-exit (проверка, добавляет ли TP edge) ──
    Variant(name="v12_5m_bench_timeexit_only",
            entry_lo=0.10, entry_hi=0.25, tp_mode="abs", tp_value=0.99),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--variant", type=int, default=None,
                    help="run one variant by index (1-based)")
    ap.add_argument("--all", action="store_true",
                    help="run all variants")
    ap.add_argument("--sample", type=int, default=None,
                    help="limit to N markets (for smoke-test)")
    ap.add_argument("--detail", action="store_true",
                    help="print first few trades per variant")
    ap.add_argument("--no-fee", action="store_true")
    args = ap.parse_args()

    to_run: list[Variant]
    if args.variant is not None:
        to_run = [VARIANTS[args.variant - 1]]
    elif args.all:
        to_run = VARIANTS
    else:
        ap.print_help()
        return 1

    if args.no_fee:
        for v in to_run:
            v.fee_on = False

    results = run_many(to_run, sample=args.sample, detail=args.detail)
    print("\n" + "=" * 60)
    for v in to_run:
        print(format_stats(v, results[v.name]))
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
