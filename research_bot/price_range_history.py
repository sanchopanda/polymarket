"""
research_bot/price_range_history.py

Анализирует исторический диапазон цен для закрытых BTC/ETH/SOL/XRP
5min/15min рынков на Polymarket.

Использует prices-history API (midpoint/last-trade цены).
Стакан не проверяется — цифры ориентировочные.

Запуск:
    python3 -m research_bot price-range
    python3 -m research_bot price-range --limit 500 --symbols BTC,ETH
"""
from __future__ import annotations

import argparse
import csv
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from cross_arb_bot.polymarket_feed import MINUTE_WINDOW_RE, SYMBOL_MAP, UPDOWN_RE
from src.api.clob import ClobClient
from src.api.gamma import GammaClient, Market

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"

TARGET_SYMBOLS = {"BTC", "ETH", "SOL", "XRP"}
TARGET_INTERVALS = {5, 15}
OPEN_WINDOW_S = 120   # берём open price из первых 2 минут рынка
OPEN_FILTER_LO = 0.35  # фильтруем рынки с open_price вне [0.35, 0.65]
OPEN_FILTER_HI = 0.65


# ── Парсинг рынков ─────────────────────────────────────────────────────────────

def _parse_market_meta(market: Market) -> Optional[tuple[str, int]]:
    """Возвращает (symbol, interval_min) или None если рынок не подходит."""
    match = UPDOWN_RE.match(market.question)
    if not match:
        return None

    raw_sym = match.group("symbol").upper()
    symbol = SYMBOL_MAP.get(raw_sym, raw_sym)
    if symbol not in TARGET_SYMBOLS:
        return None

    if len(market.clob_token_ids) < 2:
        return None

    w = MINUTE_WINDOW_RE.search(market.question)
    if not w:
        return None
    start_dt = datetime.strptime(w.group("start").upper(), "%I:%M%p")
    end_dt = datetime.strptime(w.group("end").upper(), "%I:%M%p")
    delta = int((end_dt - start_dt).total_seconds() // 60)
    if delta <= 0:
        delta += 24 * 60
    if delta not in TARGET_INTERVALS:
        return None

    return symbol, delta


# ── Основная логика ────────────────────────────────────────────────────────────

def _fetch_updown_markets(
    gamma: GammaClient,
    target_symbols: set[str],
    limit: int,
    page_size: int = 500,
) -> list[tuple[Market, str, int]]:
    """
    Листает Gamma API постранично и собирает Up or Down рынки нужных символов.
    Останавливается как только набрали limit штук — не качает лишнего.
    """
    import httpx
    import time as _time

    filtered: list[tuple[Market, str, int]] = []
    offset = 0
    pages_without_hit = 0
    MAX_EMPTY_PAGES = 30  # Up or Down рынки разбросаны, нужен большой буфер
    MAX_OFFSET = 200_000  # защита от бесконечного цикла

    while len(filtered) < limit and offset < MAX_OFFSET:
        params = {
            "closed": "true",
            "resolved": "true",
            "order": "endDate",
            "ascending": "false",
            "limit": page_size,
            "offset": offset,
        }
        try:
            resp = gamma._http.get(f"{gamma.base_url}/markets", params=params)
            resp.raise_for_status()
            batch = resp.json()
        except Exception as e:
            print(f"  [Gamma] ошибка offset={offset}: {e}", flush=True)
            break

        if not batch:
            break

        hit_this_page = 0
        for raw in batch:
            m = gamma._parse_market(raw)
            if not m:
                continue
            parsed = _parse_market_meta(m)
            if not parsed:
                continue
            symbol, interval = parsed
            if symbol not in target_symbols:
                continue
            filtered.append((m, symbol, interval))
            hit_this_page += 1
            if len(filtered) >= limit:
                break

        if hit_this_page > 0:
            pages_without_hit = 0
            print(f"  offset={offset:>6} | +{hit_this_page} рынков | итого: {len(filtered)}", flush=True)
        else:
            pages_without_hit += 1

        if len(batch) < page_size:
            break
        if pages_without_hit >= MAX_EMPTY_PAGES and len(filtered) > 0:
            # Нашли блок рынков и вышли из него — останавливаемся
            break

        offset += page_size
        _time.sleep(gamma.delay_s)

    return filtered


def main(args: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Анализ диапазона цен историчных рынков")
    parser.add_argument("--limit", type=int, default=1000,
                        help="Макс. кол-во целевых рынков для анализа (default: 1000)")
    parser.add_argument("--symbols", default="BTC,ETH,SOL,XRP",
                        help="Символы через запятую (default: BTC,ETH,SOL,XRP)")
    opts = parser.parse_args(args or [])

    target_symbols = {s.strip().upper() for s in opts.symbols.split(",")}

    gamma = GammaClient(base_url=GAMMA_URL, page_size=500, delay_ms=150)
    clob = ClobClient(base_url=CLOB_URL, delay_ms=150)

    # ── 1. Загружаем закрытые Up or Down рынки постранично ────────────────────
    print(f"[1/3] Ищем закрытые BTC/ETH/SOL/XRP Up or Down рынки (цель: {opts.limit})...", flush=True)
    filtered = _fetch_updown_markets(gamma, target_symbols, limit=opts.limit)
    print(f"  Итого подходящих рынков: {len(filtered)}", flush=True)

    # ── 3. Собираем историю цен ───────────────────────────────────────────────
    print(f"[2/3] Получаем историю цен...", flush=True)

    data_dir = Path("research_bot/data")
    data_dir.mkdir(parents=True, exist_ok=True)
    ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = data_dir / f"price_range_{ts_str}.csv"

    HI_THRESHOLDS = [0.55, 0.60, 0.65, 0.70]
    LO_THRESHOLDS = [0.45, 0.40, 0.35, 0.30]
    HI_COLS = ["hit_055", "hit_060", "hit_065", "hit_070"]
    LO_COLS = ["dropped_045", "dropped_040", "dropped_035", "dropped_030"]

    HEADER = [
        "market_id", "symbol", "interval_min", "end_date",
        "open_price", "max_price", "min_price", "final_price", "price_range",
        "max_above_open", "min_below_open",
        *HI_COLS, *LO_COLS,
    ]

    rows: list[dict] = []

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)

        for i, (market, symbol, interval) in enumerate(filtered):
            if (i + 1) % 100 == 0:
                print(f"  {i + 1}/{len(filtered)}...", flush=True)

            # Yes/Up token всегда на индексе 0 (как в PolymarketFeed)
            token_id = market.clob_token_ids[0]

            history = clob.get_price_history(token_id, fidelity=1)
            if not history:
                continue

            # start_ts — начало рынка
            end_ts: Optional[float] = None
            if market.end_date:
                try:
                    end_ts = market.end_date.replace(tzinfo=timezone.utc).timestamp()
                except (TypeError, OSError):
                    pass
            start_ts = (end_ts - interval * 60) if end_ts else None

            # open_price = первый тик в пределах OPEN_WINDOW_S от старта
            open_price: Optional[float] = None
            if start_ts is not None:
                for ts, price in history:
                    if ts >= start_ts and (ts - start_ts) <= OPEN_WINDOW_S:
                        open_price = price
                        break
            if open_price is None:
                open_price = history[0][1]

            prices = [p for _, p in history]
            max_price = max(prices)
            min_price = min(prices)
            final_price = prices[-1]
            price_range = max_price - min_price

            row = {
                "market_id": market.id,
                "symbol": symbol,
                "interval_min": interval,
                "end_date": market.end_date.strftime("%Y-%m-%dT%H:%M:%S") if market.end_date else "",
                "open_price": round(open_price, 4),
                "max_price": round(max_price, 4),
                "min_price": round(min_price, 4),
                "final_price": round(final_price, 4),
                "price_range": round(price_range, 4),
                "max_above_open": round(max_price - open_price, 4),
                "min_below_open": round(open_price - min_price, 4),
                "hit_055": int(max_price >= 0.55),
                "hit_060": int(max_price >= 0.60),
                "hit_065": int(max_price >= 0.65),
                "hit_070": int(max_price >= 0.70),
                "dropped_045": int(min_price <= 0.45),
                "dropped_040": int(min_price <= 0.40),
                "dropped_035": int(min_price <= 0.35),
                "dropped_030": int(min_price <= 0.30),
            }
            rows.append(row)
            writer.writerow([row[k] for k in HEADER])

    # ── 4. Итоговая статистика ────────────────────────────────────────────────
    print(f"\n[3/3] CSV: {csv_path}")
    print(f"Всего рынков с историей: {len(rows)}")

    near_half = [r for r in rows if OPEN_FILTER_LO <= r["open_price"] <= OPEN_FILTER_HI]
    print(f"Рынков с open_price в [{OPEN_FILTER_LO}..{OPEN_FILTER_HI}]: {len(near_half)}\n")

    if not near_half:
        print("Нет данных для анализа.")
        return

    stats: dict[tuple, list] = defaultdict(list)
    for r in near_half:
        stats[(r["symbol"], r["interval_min"])].append(r)

    all_cols = HI_COLS + LO_COLS
    col_labels = ["≥0.55", "≥0.60", "≥0.65", "≥0.70", "≤0.45", "≤0.40", "≤0.35", "≤0.30"]

    # Заголовок таблицы
    print(f"{'Sym':<5} {'Int':>4}  {'N':>5}  " +
          "  ".join(f"{l:>6}" for l in col_labels) +
          "  med_range  avg_range")
    print("-" * 100)

    for (sym, interval), group in sorted(stats.items()):
        n = len(group)
        pcts = [f"{sum(r[c] for r in group) / n * 100:.0f}%" for c in all_cols]
        ranges = sorted(r["price_range"] for r in group)
        median_range = ranges[n // 2]
        avg_range = sum(r["price_range"] for r in group) / n
        print(
            f"{sym:<5} {interval:>4}  {n:>5}  " +
            "  ".join(f"{p:>6}" for p in pcts) +
            f"  {median_range:.3f}      {avg_range:.3f}"
        )

    print()

    # Общая статистика для "buy YES at open, sell at threshold"
    print("Потенциальная прибыль при продаже YES (только рынки с open≈0.5):")
    print(f"  Маш. cost + 2% spread предполагается при оценке")
    for threshold, col in zip([0.55, 0.60, 0.65, 0.70], HI_COLS):
        hit = sum(r[col] for r in near_half)
        pct = hit / len(near_half) * 100
        profit = threshold - 0.50
        print(f"  Продажа при ≥{threshold:.2f}: {hit}/{len(near_half)} ({pct:.1f}%) рынков | profit/share ≈ +{profit:.2f}")

    print()
    print("Покупка NO (когда YES вырос, NO подешевел):")
    for threshold, col in zip([0.45, 0.40, 0.35, 0.30], LO_COLS):
        dropped = sum(r[col] for r in near_half)
        pct = dropped / len(near_half) * 100
        print(f"  NO ≤{threshold:.2f} (YES ≥{1-threshold:.2f}): {dropped}/{len(near_half)} ({pct:.1f}%) рынков")

    # ── Consecutive resolution analysis ──────────────────────────────────────
    print()
    print("Серийность: как часто рынок решается в ту же сторону что и предыдущий:")
    print(f"{'Sym':<5} {'Int':>4}  {'Пар':>6}  {'Та же сторона':>14}  {'Смена стороны':>14}  {'% повтор':>10}")
    print("-" * 70)

    total_same = 0
    total_pairs = 0

    for (sym, iv), group in sorted(stats.items()):
        # Сортируем по дате, берём только с чёткой resolution (не ~0.5)
        resolved = [r for r in group if float(r["final_price"]) != 0.5]
        resolved.sort(key=lambda r: r["end_date"])

        same = 0
        diff = 0
        for prev, cur in zip(resolved, resolved[1:]):
            prev_yes = float(prev["final_price"]) > 0.5
            cur_yes  = float(cur["final_price"]) > 0.5
            if prev_yes == cur_yes:
                same += 1
            else:
                diff += 1

        pairs = same + diff
        if pairs == 0:
            continue
        pct = same / pairs * 100
        total_same += same
        total_pairs += pairs
        print(f"{sym:<5} {iv:>4}  {pairs:>6}  {same:>8} ({same/pairs*100:.0f}%)  {diff:>8} ({diff/pairs*100:.0f}%)  {pct:>9.1f}%")

    if total_pairs:
        print("-" * 70)
        print(f"{'ИТОГО':<5} {'':>4}  {total_pairs:>6}  {total_same:>8} ({total_same/total_pairs*100:.0f}%)  "
              f"{total_pairs-total_same:>8} ({(total_pairs-total_same)/total_pairs*100:.0f}%)  "
              f"{total_same/total_pairs*100:>9.1f}%")

    print(f"\nCSV: {csv_path}")
