"""Фетчер для EV-бота.

Для каждого бинарного рынка берёт цену каждого исхода в момент T-N часов
до экспирации (один снимок на исход). Обе стороны = 2 записи на рынок.

Это избегает survivorship bias: мы не идём по траектории цены,
а делаем снимок в конкретный момент времени до исхода.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import httpx

from src.api.gamma import GammaClient, Market
from src.backtest.fetcher import HistoricalMarket


def _fetch_entry_at_time(
    base_url: str,
    market: Market,
    hours_before: float,
) -> Optional[list[tuple[int, float]]]:
    """Берёт цену каждого исхода в момент T-hours_before до экспирации.

    Возвращает список (outcome_idx, price) или None если нет данных.
    Исключает цены ближе 0.05 к границам (0 или 1) — рынок уже "решён".
    """
    if not market.end_date:
        return None

    # Целевой Unix timestamp
    target_ts = (market.end_date - timedelta(hours=hours_before)).timestamp()

    results = []

    with httpx.Client(timeout=15.0) as http:
        for i, token_id in enumerate(market.clob_token_ids):
            if not token_id:
                continue
            try:
                resp = http.get(
                    f"{base_url}/prices-history",
                    params={"market": token_id, "interval": "max", "fidelity": 10},
                )
                resp.raise_for_status()
                history = resp.json().get("history", [])
            except Exception:
                continue

            if not history:
                continue

            # Ищем точку ближайшую к target_ts
            best_point = None
            best_dist = float("inf")
            for point in history:
                try:
                    t = float(point["t"])
                    p = float(point["p"])
                except (KeyError, ValueError, TypeError):
                    continue
                dist = abs(t - target_ts)
                if dist < best_dist:
                    best_dist = dist
                    best_point = p

            if best_point is None:
                continue

            # Исключаем уже решённые цены
            if not (0.05 <= best_point <= 0.95):
                continue

            results.append((i, best_point))

    return results if results else None


def fetch_ev_markets(
    gamma: GammaClient,
    clob_base_url: str,
    hours_before: float = 2.0,
    limit: int = 10000,
    min_volume: float = 0.0,
    workers: int = 20,
    closed_after_days: float = 30.0,
) -> List[HistoricalMarket]:
    """Загружает закрытые рынки для EV-анализа.

    Для каждого бинарного рынка берёт цену каждого исхода в момент
    T-hours_before до экспирации. Обе стороны бинарного рынка попадают
    в датасет как независимые записи.

    closed_after_days — брать только рынки закрытые за последние N дней
    (CLOB-история хранится ограниченное время).
    """
    closed_after = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=closed_after_days)
    print(
        f"[EV-Fetcher] Загружаем закрытые рынки "
        f"(лимит {limit}, снимок за T-{hours_before:.1f}ч до экспирации, "
        f"закрытые после {closed_after.strftime('%Y-%m-%d')})..."
    )
    raw_markets = gamma.fetch_closed_markets(
        limit=limit,
        min_volume=min_volume,
        min_liquidity=0.0,
        closed_after=closed_after,
    )
    print(f"[EV-Fetcher] Получено {len(raw_markets)} рынков с API.")

    # Только бинарные с чётким результатом
    candidates: list[Market] = []
    winner_map: dict[str, int] = {}

    for m in raw_markets:
        if len(m.outcomes) != 2 or len(m.clob_token_ids) != 2:
            continue
        if not m.end_date:
            continue
        winner_idx = next((i for i, p in enumerate(m.outcome_prices) if p >= 0.9), None)
        if winner_idx is None:
            continue
        candidates.append(m)
        winner_map[m.id] = winner_idx

    print(
        f"[EV-Fetcher] Бинарных зарезолвленных: {len(candidates)}. "
        f"Запрашиваем историю цен ({workers} потоков)..."
    )

    result: List[HistoricalMarket] = []
    skipped = 0

    futures: dict = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for m in candidates:
            f = pool.submit(_fetch_entry_at_time, clob_base_url, m, hours_before)
            futures[f] = m

        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 100 == 0:
                print(f"  {done}/{len(candidates)} обработано...")

            m = futures[future]
            samples = future.result()
            if not samples:
                skipped += 1
                continue

            winner_idx = winner_map[m.id]
            for idx, entry_price in samples:
                result.append(HistoricalMarket(
                    market_id=m.id,
                    question=m.question,
                    outcome=m.outcomes[idx],
                    token_id=m.clob_token_ids[idx],
                    entry_price=entry_price,
                    final_price=m.outcome_prices[idx],
                    won=(idx == winner_idx),
                    volume_num=m.volume_num,
                    liquidity_num=m.liquidity_num,
                    end_date=m.end_date,
                ))

    hit = len(candidates) - skipped
    print(
        f"[EV-Fetcher] Готово: {len(result)} записей из {len(candidates)} рынков. "
        f"С историей: {hit} ({hit * 100 // max(len(candidates), 1)}%). "
        f"Без истории: {skipped}."
    )
    return result
