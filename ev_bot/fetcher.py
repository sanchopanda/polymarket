"""Фетчер для EV-бота.

Отличие от src/backtest/fetcher.py:
- Берёт цену каждого исхода за N часов до экспирации (реалистичная точка входа)
- Не фильтрует по ценовому диапазону — полный спектр для EV-анализа
- Добавляет ОБА исхода бинарного рынка как отдельные записи (удваивает выборку)
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List, Optional

import httpx

from src.api.gamma import GammaClient, Market
from src.backtest.fetcher import HistoricalMarket


def _fetch_price_at_time(
    base_url: str,
    token_id: str,
    target_ts: float,
) -> Optional[float]:
    """Возвращает цену токена в момент ближайший к target_ts (unix timestamp).

    Если история пустая или разрыв > 6 часов — возвращает None.
    """
    with httpx.Client(timeout=15.0) as http:
        try:
            resp = http.get(
                f"{base_url}/prices-history",
                params={"market": token_id, "interval": "max", "fidelity": 10},
            )
            resp.raise_for_status()
            history = resp.json().get("history", [])
        except Exception:
            return None

    if not history:
        return None

    best_price: Optional[float] = None
    best_diff = float("inf")

    for point in history:
        try:
            t = float(point["t"])
            p = float(point["p"])
        except (KeyError, ValueError, TypeError):
            continue
        diff = abs(t - target_ts)
        if diff < best_diff:
            best_diff = diff
            best_price = p

    # Если ближайшая точка дальше 6 часов — данных нет
    if best_diff > 6 * 3600:
        return None

    return best_price


def _fetch_both_outcomes(
    base_url: str,
    market: Market,
    hours_before_expiry: float,
) -> Optional[list[tuple[int, float]]]:
    """Для рынка возвращает список (outcome_idx, entry_price) для обоих исходов.

    Берёт цену за hours_before_expiry часов до end_date.
    Возвращает None если не удалось получить цены.
    """
    if not market.end_date:
        return None

    target_ts = market.end_date.timestamp() - hours_before_expiry * 3600
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

            best_price: Optional[float] = None
            best_diff = float("inf")
            for point in history:
                try:
                    t = float(point["t"])
                    p = float(point["p"])
                except (KeyError, ValueError, TypeError):
                    continue
                diff = abs(t - target_ts)
                if diff < best_diff:
                    best_diff = diff
                    best_price = p

            # Ближайшая точка не дальше 6 часов
            if best_price is not None and best_diff <= 6 * 3600:
                results.append((i, best_price))

    return results if results else None


def fetch_ev_markets(
    gamma: GammaClient,
    clob_base_url: str,
    hours_before_expiry: float = 2.0,
    limit: int = 10000,
    min_volume: float = 0.0,
    workers: int = 20,
) -> List[HistoricalMarket]:
    """Загружает закрытые рынки для EV-анализа.

    Для каждого бинарного рынка берёт цену ОБОИХ исходов за hours_before_expiry
    до экспирации. Оба исхода добавляются как отдельные записи — это удваивает
    выборку и даёт полный ценовой диапазон 0.05–0.95.

    Не фильтрует по ценовому диапазону: EV-анализ сам найдёт где +EV.
    """
    print(
        f"[EV-Fetcher] Загружаем закрытые рынки "
        f"(лимит {limit}, T-{hours_before_expiry}ч до экспирации)..."
    )
    raw_markets = gamma.fetch_closed_markets(
        limit=limit,
        min_volume=min_volume,
        min_liquidity=0.0,
    )
    print(f"[EV-Fetcher] Получено {len(raw_markets)} рынков с API.")

    # Оставляем только бинарные с чётким результатом
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
            f = pool.submit(_fetch_both_outcomes, clob_base_url, m, hours_before_expiry)
            futures[f] = m

        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 100 == 0:
                print(f"  {done}/{len(candidates)} обработано...")

            m = futures[future]
            outcomes_data = future.result()
            if not outcomes_data:
                skipped += 1
                continue

            winner_idx = winner_map[m.id]
            for idx, entry_price in outcomes_data:
                # Игнорируем цены на границах (уже почти решено)
                if entry_price < 0.02 or entry_price > 0.98:
                    continue
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

    print(
        f"[EV-Fetcher] Готово: {len(result)} записей из {len(candidates)} рынков "
        f"(~{len(result)//max(len(candidates),1)*100}% → 2 исхода × рынок). "
        f"Пропущено (нет истории): {skipped}."
    )
    return result
