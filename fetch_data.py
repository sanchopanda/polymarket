"""
Загрузка рынков + истории цен для офлайн-бэктестов.

Запуск:
  python fetch_data.py --days 180
  python fetch_data.py --days 90 --min-volume 500

Результат сохраняется в data/markets_<N>d.json.
Бэктест-скрипты потом работают с этим файлом через --data.
"""

from __future__ import annotations

import sys
import os
import json
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import time

import httpx

sys.path.insert(0, os.path.dirname(__file__))
from src.api.gamma import GammaClient, Market

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"


def _fetch_market_history(http: httpx.Client, clob_base: str, market: Market) -> dict[str, list]:
    """Загружает историю цен для всех исходов рынка."""
    history = {}
    for token_id in market.clob_token_ids:
        if not token_id:
            continue
        try:
            resp = http.get(
                f"{clob_base}/prices-history",
                params={"market": token_id, "interval": "max", "fidelity": 100},
            )
            resp.raise_for_status()
            history[token_id] = resp.json().get("history", [])
        except Exception:
            history[token_id] = []
    return history


def main():
    parser = argparse.ArgumentParser(description="Загрузка рынков для офлайн-бэктестов")
    parser.add_argument("--days", type=float, default=180, help="Период (дней назад)")
    parser.add_argument("--min-volume", type=float, default=0, help="Мин. объём рынка")
    parser.add_argument("--workers", type=int, default=50, help="Потоков для загрузки")
    parser.add_argument("--output", type=str, default="", help="Путь для сохранения")
    args = parser.parse_args()

    gamma = GammaClient(base_url=GAMMA_URL, page_size=500, delay_ms=50)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    closed_after = now - timedelta(days=args.days)

    t_start = time.time()

    print(f"=== Загрузка рынков за последние {args.days:.0f} дней ===")
    print(f"Фильтры: min_volume={args.min_volume}\n")

    # Загружаем ВСЕ рынки с логами пагинации
    print(f"[1/3] Загрузка списка рынков из Gamma API...")
    raw_markets = gamma.fetch_closed_markets(
        limit=999999,
        min_volume=args.min_volume if args.min_volume > 0 else None,
        closed_after=closed_after,
        end_date_max=now,
        order_by="closedTime",
    )
    elapsed = time.time() - t_start
    print(f"  Получено {len(raw_markets)} рынков за {elapsed:.0f}с\n")

    # Фильтр: бинарные + зарезолвленные
    candidates: list[Market] = []
    winner_map: dict[str, int] = {}
    skipped_not_binary = 0
    skipped_no_resolution = 0

    for m in raw_markets:
        if len(m.outcomes) != 2 or len(m.clob_token_ids) != 2:
            skipped_not_binary += 1
            continue
        if not m.end_date:
            skipped_no_resolution += 1
            continue
        winner_idx = next((i for i, p in enumerate(m.outcome_prices) if p >= 0.9), None)
        if winner_idx is None:
            skipped_no_resolution += 1
            continue
        candidates.append(m)
        winner_map[m.id] = winner_idx

    print(f"[2/3] Фильтрация...")
    print(f"  Бинарных зарезолвленных: {len(candidates)}")
    print(f"  Пропущено: не бинарные={skipped_not_binary}, нет резолюции={skipped_no_resolution}\n")

    # Загрузка историй цен параллельно
    print(f"[3/3] Загрузка истории цен ({args.workers} потоков, {len(candidates)} рынков)...")
    t_history = time.time()
    results = []
    processed = 0
    errors = 0

    # Разбиваем кандидатов на чанки по воркерам, каждый воркер переиспользует соединение
    import threading
    results_lock = threading.Lock()

    def _worker(chunk: list[Market]):
        nonlocal processed, errors
        with httpx.Client(timeout=15.0) as http:
            for m in chunk:
                try:
                    history = _fetch_market_history(http, CLOB_URL, m)
                except Exception:
                    history = {}
                    with results_lock:
                        errors += 1

                entry = {
                    "id": m.id,
                    "question": m.question,
                    "outcomes": m.outcomes,
                    "outcome_prices": m.outcome_prices,
                    "clob_token_ids": m.clob_token_ids,
                    "volume_num": m.volume_num,
                    "liquidity_num": m.liquidity_num,
                    "end_date": m.end_date.isoformat() if m.end_date else None,
                    "category": m.category,
                    "fee_type": m.fee_type,
                    "winner_idx": winner_map[m.id],
                    "history": history,
                }
                with results_lock:
                    results.append(entry)
                    processed += 1
                    if processed % 200 == 0:
                        elapsed_h = time.time() - t_history
                        rate = processed / elapsed_h if elapsed_h > 0 else 0
                        remaining = (len(candidates) - processed) / rate if rate > 0 else 0
                        print(f"  {processed}/{len(candidates)} ({processed*100//len(candidates)}%) | "
                              f"{rate:.0f} рынков/с | осталось ~{remaining:.0f}с")

    # Распределяем рынки по воркерам
    n_workers = args.workers
    chunks = [candidates[i::n_workers] for i in range(n_workers)]

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        list(pool.map(lambda c: _worker(c), chunks))

    elapsed_h = time.time() - t_history
    print(f"  Загрузка историй завершена за {elapsed_h:.0f}с")
    if errors > 0:
        print(f"  Ошибок: {errors}")

    # Сохранение
    output = args.output or f"data/markets_{int(args.days)}d.json"
    os.makedirs(os.path.dirname(output), exist_ok=True)

    print(f"\nСохранение в {output}...")
    data = {
        "meta": {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "days": args.days,
            "total_raw": len(raw_markets),
            "total_saved": len(results),
        },
        "markets": results,
    }

    with open(output, "w") as f:
        json.dump(data, f, ensure_ascii=False)

    size_mb = os.path.getsize(output) / 1024 / 1024
    total_elapsed = time.time() - t_start

    # Итоговая сводка
    print(f"\n{'='*60}")
    print(f"  ГОТОВО")
    print(f"{'='*60}")
    print(f"  Файл:              {output} ({size_mb:.1f} MB)")
    print(f"  Период:            {args.days:.0f} дней")
    print(f"  Рынков от API:     {len(raw_markets)}")
    print(f"  Сохранено:         {len(results)} (бинарные + зарезолвленные)")
    print(f"  Время:             {total_elapsed:.0f}с ({total_elapsed/60:.1f} мин)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
