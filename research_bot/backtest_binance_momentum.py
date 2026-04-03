"""
research_bot/backtest_binance_momentum.py

Бэктест стратегии: резкое движение Binance за 5 секунд → ставка в ту же сторону на PM.

Данные берутся из backtest.db (таблицы markets, binance_1s, pm_trades).

Шаги:
1. Загрузить рынки из DB (markets)
2. Загрузить 1s цены Binance из DB (binance_1s), дофетчить если нет
3. Агрегировать в 5s бакеты
4. Для каждого рынка: искать резкие движения (|delta| >= min_delta_pct)
5. Брать последнюю сделку PM перед сигналом как цену входа
6. Вывести статистику + PnL

Запуск:
  python3 -m research_bot.backtest_binance_momentum
  python3 -m research_bot.backtest_binance_momentum --min-delta 0.03
  python3 -m research_bot.backtest_binance_momentum --symbols BTC ETH
"""
from __future__ import annotations

import argparse
import bisect
import csv
import random
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

from research_bot.backtest_db import (
    get_connection, load_markets as db_load_markets,
    load_1s, load_trades, has_trades,
)

DATA_DIR = Path("research_bot/data")
SIGNALS_OUT = DATA_DIR / "binance_momentum_signals.csv"

BINANCE_SYMBOLS = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "XRP": "XRPUSDT"}
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

SIGNALS_FIELDS = [
    "market_id", "symbol", "interval_minutes", "winning_side",
    "bucket_ts", "minute_of_market",
    "prev_price", "curr_price", "delta_pct", "signal_side",
    "pm_entry_price", "won",
    "prev_signals", "effective_delta", "formation_sec",
]


# ── Binance 1s klines (fetch missing) ───────────────────────────────────────

def fetch_binance_1s(sym: str, start_ts: int, end_ts: int,
                     http: httpx.Client) -> dict[int, float]:
    """Fetch 1s klines for [start_ts, end_ts]. Returns {sec_ts: close_price}."""
    binance_sym = BINANCE_SYMBOLS[sym]
    result: dict[int, float] = {}
    cur_ms = start_ts * 1000
    end_ms = end_ts * 1000

    while cur_ms < end_ms:
        for attempt in range(3):
            try:
                resp = http.get(BINANCE_KLINES_URL, params={
                    "symbol": binance_sym,
                    "interval": "1s",
                    "startTime": cur_ms,
                    "endTime": end_ms,
                    "limit": 1000,
                }, timeout=15)
                resp.raise_for_status()
                batch = resp.json()
                break
            except Exception:
                if attempt < 2:
                    time.sleep(1.0)
                else:
                    return result

        if not isinstance(batch, list) or not batch:
            break

        for candle in batch:
            sec_ts = int(candle[0]) // 1000
            result[sec_ts] = float(candle[4])  # close price

        last_ms = int(batch[-1][0])
        cur_ms = last_ms + 1000
        if len(batch) < 1000:
            break
        time.sleep(0.05)

    return result


def ensure_binance_1s(conn, sym: str, start_ts: int, end_ts: int,
                      http: httpx.Client, force: bool = False) -> dict[int, float]:
    """Load 1s from DB, fetch missing ranges from Binance if needed."""
    existing = load_1s(conn, sym, start_ts, end_ts)

    if existing and not force:
        coverage = len(existing) / max(1, end_ts - start_ts)
        if coverage > 0.5:
            print(f"  [{sym}] DB: {len(existing)} свечей (coverage {coverage:.0%})")
            return existing

    print(f"  [{sym}] Загрузка с Binance 1s klines...", flush=True)
    fetched = fetch_binance_1s(sym, start_ts, end_ts, http)

    # Write fetched data to DB
    new_count = 0
    batch = []
    for sec_ts, close in fetched.items():
        if sec_ts not in existing:
            batch.append((sym, sec_ts, close))
            new_count += 1
        if len(batch) >= 5000:
            conn.executemany(
                "INSERT OR IGNORE INTO binance_1s (symbol, sec_ts, close) VALUES (?,?,?)",
                batch,
            )
            conn.commit()
            batch.clear()
    if batch:
        conn.executemany(
            "INSERT OR IGNORE INTO binance_1s (symbol, sec_ts, close) VALUES (?,?,?)",
            batch,
        )
        conn.commit()

    existing.update(fetched)
    print(f"  [{sym}] {len(existing)} свечей (новых: {new_count})")
    return existing


def to_5s_buckets(klines_1s: dict[int, float]) -> dict[int, float]:
    """Aggregate 1s candles to 5s buckets using close of last candle in bucket."""
    buckets: dict[int, float] = {}
    max_sec: dict[int, int] = {}
    for sec_ts, price in klines_1s.items():
        b = (sec_ts // 5) * 5
        if b not in max_sec or sec_ts > max_sec[b]:
            max_sec[b] = sec_ts
            buckets[b] = price
    return buckets


# ── PM trades ────────────────────────────────────────────────────────────────

def _first_trade_price_after(lookup: dict[str, list[tuple[int, float]]],
                             outcome: str, ts: int) -> Optional[float]:
    """First trade price at or after ts."""
    pts = lookup.get(outcome, [])
    if not pts:
        return None
    times = [p[0] for p in pts]
    idx = bisect.bisect_left(times, ts)
    return pts[idx][1] if idx < len(pts) else None


# ── simulation ───────────────────────────────────────────────────────────────

def count_signals_in_window(buckets: dict[int, float], window_start: int,
                            window_end: int, base_delta: float = 0.05) -> int:
    pts = sorted(
        (ts, p) for ts, p in buckets.items()
        if window_start - 10 <= ts <= window_end
    )
    count = 0
    for i in range(1, len(pts)):
        pt, pp = pts[i - 1]
        ct, cp = pts[i]
        if ct < window_start or ct - pt > 10:
            continue
        if abs((cp - pp) / pp * 100) >= base_delta:
            count += 1
    return count


def simulate_market(market: dict, buckets: dict[int, float],
                    min_delta_pct: float,
                    trades: Optional[dict],
                    min_minute: int = 0,
                    adaptive_rules: Optional[list] = None,
                    adaptive_window: int = 600,
                    entry_delay: int = 1,
                    min_price: float = 0.0,
                    max_price: float = 1.0,
                    cheap_delta: float = 0.0) -> list[dict]:
    start_ts = market["_start_ts"]
    end_ts = market["_end_ts"]
    winning_side = market["winning_side"]

    if adaptive_rules:
        prev_count = count_signals_in_window(
            buckets, start_ts - adaptive_window, start_ts,
        )
        effective_delta = min_delta_pct
        for max_n, threshold in adaptive_rules:
            if prev_count <= max_n:
                effective_delta = threshold
                break
    else:
        effective_delta = min_delta_pct
        prev_count = 0

    sorted_pts = sorted(
        (ts, p) for ts, p in buckets.items()
        if start_ts - 10 <= ts <= end_ts
    )

    for i in range(1, len(sorted_pts)):
        prev_ts, prev_price = sorted_pts[i - 1]
        curr_ts, curr_price = sorted_pts[i]

        if curr_ts < start_ts:
            continue
        if curr_ts - prev_ts > 10:
            continue

        if (curr_ts - start_ts) // 60 < min_minute:
            continue

        delta_pct = (curr_price - prev_price) / prev_price * 100
        if abs(delta_pct) < effective_delta:
            continue

        signal_side = "yes" if delta_pct > 0 else "no"
        won = signal_side == winning_side

        pm_entry_price = None
        if trades:
            outcome = "Up" if signal_side == "yes" else "Down"
            pm_entry_price = _first_trade_price_after(trades, outcome, curr_ts + entry_delay)

        if pm_entry_price is not None:
            if pm_entry_price < min_price or pm_entry_price > max_price:
                continue
            if cheap_delta > 0 and pm_entry_price < 0.50 and abs(delta_pct) < cheap_delta:
                continue

        return [{
            "market_id": market["market_id"],
            "symbol": market["symbol"],
            "interval_minutes": market["_interval"],
            "winning_side": winning_side,
            "bucket_ts": datetime.fromtimestamp(curr_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "minute_of_market": (curr_ts - start_ts) // 60,
            "prev_price": round(prev_price, 4),
            "curr_price": round(curr_price, 4),
            "delta_pct": round(delta_pct, 6),
            "signal_side": signal_side,
            "pm_entry_price": round(pm_entry_price, 4) if pm_entry_price is not None else "",
            "won": won,
            "prev_signals": prev_count,
            "effective_delta": effective_delta,
        }]

    return []


def simulate_market_continuous(market: dict, klines_1s: dict[int, float],
                               min_delta_pct: float,
                               trades: Optional[dict],
                               min_minute: int = 0,
                               lookback: int = 5,
                               adaptive_rules: Optional[list] = None,
                               adaptive_window: int = 600,
                               buckets_5s: Optional[dict[int, float]] = None,
                               entry_delay: int = 1,
                               min_price: float = 0.0,
                               max_price: float = 1.0,
                               cheap_delta: float = 0.0) -> list[dict]:
    start_ts = market["_start_ts"]
    end_ts = market["_end_ts"]
    winning_side = market["winning_side"]

    if adaptive_rules and buckets_5s is not None:
        prev_count = count_signals_in_window(
            buckets_5s, start_ts - adaptive_window, start_ts,
        )
        effective_delta = min_delta_pct
        for max_n, threshold in adaptive_rules:
            if prev_count <= max_n:
                effective_delta = threshold
                break
    else:
        effective_delta = min_delta_pct
        prev_count = 0

    min_ts = start_ts + min_minute * 60

    for t in range(min_ts, end_ts + 1):
        ref_t = t - lookback
        if t not in klines_1s or ref_t not in klines_1s:
            continue

        curr_price = klines_1s[t]
        ref_price = klines_1s[ref_t]
        delta_pct = (curr_price - ref_price) / ref_price * 100

        if abs(delta_pct) < effective_delta:
            continue

        formation_sec = lookback
        for lb in range(1, lookback):
            rt = t - lb
            if rt in klines_1s:
                d = (curr_price - klines_1s[rt]) / klines_1s[rt] * 100
                if abs(d) >= effective_delta:
                    formation_sec = lb
                    break

        signal_side = "yes" if delta_pct > 0 else "no"
        won = signal_side == winning_side

        pm_entry_price = None
        if trades:
            outcome = "Up" if signal_side == "yes" else "Down"
            pm_entry_price = _first_trade_price_after(trades, outcome, t + entry_delay)

        if pm_entry_price is not None:
            if pm_entry_price < min_price or pm_entry_price > max_price:
                continue
            if cheap_delta > 0 and pm_entry_price < 0.50 and abs(delta_pct) < cheap_delta:
                continue

        return [{
            "market_id": market["market_id"],
            "symbol": market["symbol"],
            "interval_minutes": market["_interval"],
            "winning_side": winning_side,
            "bucket_ts": datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "minute_of_market": (t - start_ts) // 60,
            "prev_price": round(ref_price, 4),
            "curr_price": round(curr_price, 4),
            "delta_pct": round(delta_pct, 6),
            "signal_side": signal_side,
            "pm_entry_price": round(pm_entry_price, 4) if pm_entry_price is not None else "",
            "won": won,
            "prev_signals": prev_count,
            "effective_delta": effective_delta,
            "formation_sec": formation_sec,
        }]

    return []


# ── stats ────────────────────────────────────────────────────────────────────

def print_stats(rows: list[dict]) -> None:
    if not rows:
        print("Нет сигналов")
        return

    def is_win(r):
        return r["won"] is True or r["won"] == "True"

    print(f"\n{'='*60}")
    print(f"  Всего сигналов: {len(rows)}")
    wins = sum(1 for r in rows if is_win(r))
    print(f"  Win rate:       {wins}/{len(rows)} = {wins/len(rows)*100:.1f}%")

    for label, key in [("символ", "symbol"), ("интервал", "interval_minutes"), ("сторона", "signal_side")]:
        print(f"\n  По {label}:")
        groups: dict = defaultdict(list)
        for r in rows:
            groups[str(r[key])].append(r)
        for k, grp in sorted(groups.items()):
            w = sum(1 for r in grp if is_win(r))
            print(f"    {k}: {w}/{len(grp)} = {w/len(grp)*100:.1f}%")

    print(f"\n  По минуте рынка:")
    by_min: dict[int, list] = defaultdict(list)
    for r in rows:
        try:
            by_min[int(r["minute_of_market"])].append(r)
        except Exception:
            pass
    for minute in sorted(by_min.keys()):
        grp = by_min[minute]
        w = sum(1 for r in grp if is_win(r))
        print(f"    min {minute:2d}: {w:4d}/{len(grp):4d} = {w/len(grp)*100:.1f}%")

    print(f"\n  По размеру движения (|delta_pct|):")
    dbuckets: dict[str, list] = defaultdict(list)
    for r in rows:
        d = abs(float(r["delta_pct"]))
        if d < 0.05:
            b = "<0.05%"
        elif d < 0.10:
            b = "0.05–0.10%"
        elif d < 0.20:
            b = "0.10–0.20%"
        else:
            b = ">0.20%"
        dbuckets[b].append(r)
    for bk in ["<0.05%", "0.05–0.10%", "0.10–0.20%", ">0.20%"]:
        grp = dbuckets.get(bk, [])
        if not grp:
            continue
        w = sum(1 for r in grp if is_win(r))
        print(f"    {bk:12s}: {w}/{len(grp)} = {w/len(grp)*100:.1f}%")

    ft_rows = [r for r in rows if r.get("formation_sec") not in (None, "", 0)]
    if ft_rows:
        fts = [int(r["formation_sec"]) for r in ft_rows]
        avg_ft = sum(fts) / len(fts)
        print(f"\n  Время формирования сигнала (continuous):")
        print(f"    Среднее: {avg_ft:.1f}с")
        by_ft: dict[int, list] = defaultdict(list)
        for r in ft_rows:
            by_ft[int(r["formation_sec"])].append(r)
        for sec in sorted(by_ft.keys()):
            grp = by_ft[sec]
            w = sum(1 for r in grp if is_win(r))
            print(f"    {sec}с: {w}/{len(grp)} = {w/len(grp)*100:.1f}%")

    # PnL
    priced = [r for r in rows if r.get("pm_entry_price") not in ("", None)]
    if not priced:
        print(f"\n  [PnL] нет данных о цене входа (запустите fetch_trades)")
        print(f"{'='*60}")
        return

    BET = 1.0
    def _f(v):
        try: return float(v)
        except: return None

    total_cost = total_payout = 0.0
    n = pw = 0
    ep_buckets: dict[str, list] = defaultdict(list)
    equity_curve: list[float] = []

    equity = 0.0
    for r in priced:
        ep = _f(r["pm_entry_price"])
        if ep is None or ep <= 0.05 or ep >= 0.95:
            continue
        won = is_win(r)
        cost = BET
        payout = BET / ep if won else 0.0
        equity += payout - cost
        equity_curve.append(equity)
        total_cost += cost
        total_payout += payout
        n += 1
        if won: pw += 1
        if ep < 0.35:
            b = "<0.35"
        elif ep < 0.50:
            b = "0.35–0.50"
        elif ep < 0.65:
            b = "0.50–0.65"
        elif ep < 0.80:
            b = "0.65–0.80"
        else:
            b = ">0.80"
        ep_buckets[b].append((ep, won))

    if n == 0:
        print(f"{'='*60}")
        return

    entry_prices = [_f(r["pm_entry_price"]) for r in priced
                    if _f(r["pm_entry_price"]) and 0.05 < _f(r["pm_entry_price"]) < 0.95]
    avg_entry = sum(entry_prices) / len(entry_prices) if entry_prices else 0

    peak = float("-inf")
    max_drawdown = 0.0
    for eq in equity_curve:
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_drawdown:
            max_drawdown = dd

    pnl = total_payout - total_cost
    print(f"\n  PnL (${BET:.2f}/сигнал, {n} с ценой):")
    print(f"    Win rate:     {pw}/{n} = {pw/n*100:.1f}%")
    print(f"    Avg entry:    {avg_entry:.3f} ({avg_entry*100:.1f}c)")
    print(f"    Вложено:      ${total_cost:.2f}")
    print(f"    Получено:     ${total_payout:.2f}")
    print(f"    PnL:          ${pnl:+.2f}  (ROI {pnl/total_cost*100:+.1f}%)")
    print(f"    Max drawdown: ${max_drawdown:.2f}  (от пика)")

    print(f"\n  PnL по цене входа:")
    for bk in ["<0.35", "0.35–0.50", "0.50–0.65", "0.65–0.80", ">0.80"]:
        pts = ep_buckets.get(bk, [])
        if not pts:
            continue
        bc = bp = 0.0
        bw = bl = 0
        for ep, won in pts:
            bc += BET
            bp += BET / ep if won else 0.0
            if won: bw += 1
            else: bl += 1
        bn = bw + bl
        bpnl = bp - bc
        print(f"    {bk:10s}: {bw:4d}/{bn:4d} = {bw/bn*100:.1f}%  "
              f"PnL ${bpnl:+.2f}  (ROI {bpnl/bc*100:+.1f}%)")

    print(f"{'='*60}")


# ── main ─────────────────────────────────────────────────────────────────────

def _load_bot_config() -> dict:
    """Load defaults from bot config if available."""
    try:
        import yaml
        with open("oracle_arb_bot/config.yaml") as f:
            return yaml.safe_load(f)
    except Exception:
        return {}


def main() -> None:
    cfg = _load_bot_config()
    strat = cfg.get("strategy", {})
    trading = cfg.get("trading", {})

    parser = argparse.ArgumentParser()
    parser.add_argument("--min-delta", type=float,
                        default=strat.get("momentum_delta_pct", 0.05),
                        help="минимальный |delta_pct| за 5с")
    parser.add_argument("--symbols", nargs="+", default=list(BINANCE_SYMBOLS.keys()))
    parser.add_argument("--force-fetch", action="store_true",
                        help="перезагрузить 1s klines даже если в DB есть")
    parser.add_argument("--limit", type=int, default=None,
                        help="максимум рынков (последних) для обработки")
    parser.add_argument("--min-minute", type=int,
                        default=strat.get("momentum_min_minute", 1),
                        help="минимальная минута рынка для сигнала")
    parser.add_argument("--shuffle", action="store_true",
                        help="перемешать рынки случайно перед --limit")
    parser.add_argument("--adaptive", action="store_true",
                        default=strat.get("momentum_adaptive", True),
                        help="адаптивный порог по сигналам за предыдущие 10 мин")
    parser.add_argument("--no-adaptive", action="store_true",
                        help="выключить адаптивный порог")
    parser.add_argument("--adaptive-window", type=int,
                        default=strat.get("momentum_adaptive_window", 600),
                        help="окно подсчёта сигналов в секундах")
    parser.add_argument("--adaptive-rules", type=str, default=None,
                        help="правила max_n:delta через запятую")
    parser.add_argument("--continuous", action="store_true",
                        help="continuous mode: проверять дельту каждую секунду (не по 5с бакетам)")
    parser.add_argument("--lookback", type=int, default=5,
                        help="окно сравнения цены в секундах для continuous mode (default 5)")
    parser.add_argument("--entry-delay", type=int, default=1,
                        help="задержка входа: PM цена берётся через N секунд после сигнала (default 1)")
    parser.add_argument("--min-price", type=float, default=0.0,
                        help="минимальная цена входа PM (default 0)")
    parser.add_argument("--max-price", type=float,
                        default=trading.get("max_price", 0.48),
                        help="максимальная цена входа PM")
    parser.add_argument("--cheap-delta", type=float,
                        default=strat.get("momentum_cheap_delta_pct", 0.10),
                        help="мин. дельта для ставок с ценой < 0.50")
    args = parser.parse_args()

    if args.no_adaptive:
        args.adaptive = False

    adaptive_rules = None
    if args.adaptive:
        if args.adaptive_rules:
            adaptive_rules = []
            for part in args.adaptive_rules.split(","):
                n, d = part.split(":")
                adaptive_rules.append((int(n), float(d)))
        else:
            # из конфига бота: [[2, 0.05], [5, 0.08], [999, 0.12]]
            raw = strat.get("momentum_adaptive_rules", [[2, 0.05], [5, 0.08], [999, 0.12]])
            adaptive_rules = [(int(r[0]), float(r[1])) for r in raw]
        print(f"Adaptive: window={args.adaptive_window}s rules={adaptive_rules}")

    if args.max_price < 1.0 or args.min_price > 0.0:
        print(f"Price filter: {args.min_price:.2f} – {args.max_price:.2f}")
    if args.cheap_delta > 0:
        print(f"Cheap delta: price < 0.50 requires |delta| >= {args.cheap_delta}%")

    conn = get_connection()

    # Load markets from DB
    symbols_filter = [s for s in args.symbols if s in BINANCE_SYMBOLS]
    all_markets = db_load_markets(conn, symbols=symbols_filter)

    # Add computed fields
    markets = []
    for m in all_markets:
        try:
            m["_start_ts"] = int(datetime.strptime(m["market_start"], "%Y-%m-%d %H:%M:%S")
                                 .replace(tzinfo=timezone.utc).timestamp())
            m["_end_ts"] = int(datetime.strptime(m["market_end"], "%Y-%m-%d %H:%M:%S")
                               .replace(tzinfo=timezone.utc).timestamp())
            m["_interval"] = m["interval_minutes"]
        except Exception:
            continue
        markets.append(m)

    # Сортируем по времени старта (новые первые) для --limit
    markets.sort(key=lambda m: m["_start_ts"], reverse=True)
    if args.shuffle:
        random.shuffle(markets)
    if args.limit:
        markets = markets[:args.limit]
    print(f"Рынков: {len(markets)}")
    if not markets:
        conn.close()
        return

    range_start = min(m["_start_ts"] for m in markets) - (args.adaptive_window + 30 if adaptive_rules else 30)
    range_end = max(m["_end_ts"] for m in markets) + 30
    print(f"Диапазон: {datetime.fromtimestamp(range_start, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')} – "
          f"{datetime.fromtimestamp(range_end, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}")

    symbols = list({m["symbol"] for m in markets})
    http = httpx.Client(timeout=20.0)

    binance_1s_all: dict[str, dict[int, float]] = {}
    binance_5s: dict[str, dict[int, float]] = {}
    for sym in symbols:
        klines_1s = ensure_binance_1s(conn, sym, range_start, range_end, http, args.force_fetch)
        binance_1s_all[sym] = klines_1s
        binance_5s[sym] = to_5s_buckets(klines_1s)
        print(f"  [{sym}] 5s бакетов: {len(binance_5s[sym])}")

    http.close()

    # Filter markets that have trades in DB
    before = len(markets)
    markets = [m for m in markets if has_trades(conn, m["market_id"])]
    print(f"Рынков с загруженными ценами: {len(markets)} (пропущено {before - len(markets)})")

    mode_label = "continuous" if args.continuous else "5s-bucket"
    print(f"\nСимуляция {mode_label} (min_delta={args.min_delta}%)...")
    all_rows: list[dict] = []

    markets_by_sym: dict[str, list] = defaultdict(list)
    for m in markets:
        markets_by_sym[m["symbol"]].append(m)

    out_f = open(SIGNALS_OUT, "w", newline="")
    writer = csv.DictWriter(out_f, fieldnames=SIGNALS_FIELDS)
    writer.writeheader()

    for sym, sym_markets in markets_by_sym.items():
        if args.continuous:
            klines_1s = binance_1s_all.get(sym, {})
            buckets = binance_5s.get(sym, {})
            if not klines_1s:
                print(f"  [{sym}] Нет данных, пропускаем")
                continue
            for market in sym_markets:
                trades_data = load_trades(conn, market["market_id"])
                rows = simulate_market_continuous(
                    market, klines_1s, args.min_delta, trades_data, args.min_minute,
                    lookback=args.lookback,
                    adaptive_rules=adaptive_rules,
                    adaptive_window=args.adaptive_window,
                    buckets_5s=buckets,
                    entry_delay=args.entry_delay,
                    min_price=args.min_price,
                    max_price=args.max_price,
                    cheap_delta=args.cheap_delta,
                )
                writer.writerows(rows)
                all_rows.extend(rows)
        else:
            buckets = binance_5s.get(sym, {})
            if not buckets:
                print(f"  [{sym}] Нет данных, пропускаем")
                continue
            for market in sym_markets:
                trades_data = load_trades(conn, market["market_id"])
                rows = simulate_market(market, buckets, args.min_delta, trades_data, args.min_minute,
                                       adaptive_rules=adaptive_rules,
                                       adaptive_window=args.adaptive_window,
                                       entry_delay=args.entry_delay,
                                       min_price=args.min_price,
                                       max_price=args.max_price,
                                       cheap_delta=args.cheap_delta)
                writer.writerows(rows)
                all_rows.extend(rows)

    out_f.close()
    conn.close()
    print(f"Файл: {SIGNALS_OUT}")
    print_stats(all_rows)


if __name__ == "__main__":
    main()
