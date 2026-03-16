from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import httpx

from src.api.clob import ClobClient
from src.api.gamma import GammaClient, Market
from src.config import StrategyConfig


@dataclass
class HistoricalMarket:
    market_id: str
    question: str
    outcome: str          # исход на который "ставим"
    token_id: str         # clob token_id этого исхода
    entry_price: float    # цена входа (~0.50 из истории или 0.50 по умолчанию)
    final_price: float    # 0.0 или 1.0 — результат резолюции
    won: bool
    volume_num: float
    liquidity_num: float
    end_date: Optional[datetime]


def _fetch_history_for_market(
    base_url: str,
    market: Market,
    target: float,
    tolerance: float,
) -> Optional[tuple[int, float]]:
    """Для одного рынка ищет первый исход с ценой в диапазоне target±tolerance.

    Возвращает (bet_idx, entry_price) или None.
    Использует отдельный httpx.Client чтобы работать из потоков.
    """
    with httpx.Client(timeout=15.0) as http:
        for i, token_id in enumerate(market.clob_token_ids):
            try:
                resp = http.get(
                    f"{base_url}/prices-history",
                    params={"market": token_id, "interval": "max", "fidelity": 10},
                )
                resp.raise_for_status()
                history = resp.json().get("history", [])
            except Exception:
                continue

            for point in history:
                try:
                    price = float(point["p"])
                except (KeyError, ValueError, TypeError):
                    continue
                if abs(price - target) <= tolerance:
                    return (i, price)

    return None


def fetch_historical_markets(
    gamma: GammaClient,
    clob: ClobClient,
    strategy: StrategyConfig,
    limit: int = 300,
    use_price_history: bool = True,
    workers: int = 20,
) -> List[HistoricalMarket]:
    """Загружает закрытые рынки и подготавливает их для бэктеста.

    Для каждого рынка ищет момент когда любой из исходов попал в целевой
    ценовой диапазон — это симулирует реальный вход сканера.
    CLOB-запросы выполняются параллельно (workers потоков).
    """

    target = strategy.target_price
    tolerance = strategy.price_tolerance
    if strategy.price_min is not None and strategy.price_max is not None:
        target = (strategy.price_min + strategy.price_max) / 2
        tolerance = (strategy.price_max - strategy.price_min) / 2

    print(f"[Fetcher] Загружаем закрытые рынки (лимит {limit}, объём≥{strategy.min_volume_24h}, ликвидность≥{strategy.min_liquidity})...")
    raw_markets = gamma.fetch_closed_markets(
        limit=limit,
        min_volume=strategy.min_volume_24h,
        min_liquidity=strategy.min_liquidity,
    )
    print(f"[Fetcher] Получено {len(raw_markets)} рынков после серверной фильтрации.")

    # Предварительная фильтрация (без сетевых запросов)
    candidates: List[Market] = []
    winner_map: dict[str, int] = {}  # market_id -> winner_idx
    skipped_not_binary = skipped_no_resolution = 0

    for m in raw_markets:
        if len(m.outcomes) != 2 or len(m.outcome_prices) != 2 or len(m.clob_token_ids) != 2:
            skipped_not_binary += 1
            continue
        winner_idx = next((i for i, p in enumerate(m.outcome_prices) if p >= 0.9), None)
        if winner_idx is None:
            skipped_no_resolution += 1
            continue
        candidates.append(m)
        winner_map[m.id] = winner_idx

    print(f"[Fetcher] После фильтрации: {len(candidates)} кандидатов. Запрашиваем историю цен ({workers} потоков)...")

    result: List[HistoricalMarket] = []
    skipped_no_entry = 0

    if use_price_history:
        clob_base = clob.base_url
        futures = {}
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for m in candidates:
                f = pool.submit(_fetch_history_for_market, clob_base, m, target, tolerance)
                futures[f] = m

            done = 0
            for future in as_completed(futures):
                done += 1
                if done % 50 == 0:
                    print(f"  {done}/{len(candidates)} обработано...")
                m = futures[future]
                entry_result = future.result()
                if entry_result is None:
                    skipped_no_entry += 1
                    continue
                bet_idx, entry_price = entry_result
                winner_idx = winner_map[m.id]
                result.append(HistoricalMarket(
                    market_id=m.id,
                    question=m.question,
                    outcome=m.outcomes[bet_idx],
                    token_id=m.clob_token_ids[bet_idx],
                    entry_price=entry_price,
                    final_price=m.outcome_prices[bet_idx],
                    won=(bet_idx == winner_idx),
                    volume_num=m.volume_num,
                    liquidity_num=m.liquidity_num,
                    end_date=m.end_date,
                ))
    else:
        for m in candidates:
            winner_idx = winner_map[m.id]
            result.append(HistoricalMarket(
                market_id=m.id,
                question=m.question,
                outcome=m.outcomes[0],
                token_id=m.clob_token_ids[0],
                entry_price=target,
                final_price=m.outcome_prices[0],
                won=(0 == winner_idx),
                volume_num=m.volume_num,
                liquidity_num=m.liquidity_num,
                end_date=m.end_date,
            ))

    print(
        f"[Fetcher] Итого подходящих: {len(result)}. "
        f"Пропущено: не бинарные={skipped_not_binary}, "
        f"не зарезолвились={skipped_no_resolution}, "
        f"цена не входила в диапазон={skipped_no_entry}."
    )
    return result


def save_markets(markets: List[HistoricalMarket], path: str) -> None:
    """Сохраняет рынки в JSON-файл."""
    data = []
    for m in markets:
        data.append({
            "market_id": m.market_id,
            "question": m.question,
            "outcome": m.outcome,
            "token_id": m.token_id,
            "entry_price": m.entry_price,
            "final_price": m.final_price,
            "won": m.won,
            "volume_num": m.volume_num,
            "liquidity_num": m.liquidity_num,
            "end_date": m.end_date.isoformat() if m.end_date else None,
        })
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[Fetcher] Сохранено {len(markets)} рынков → {path}")


def load_markets(path: str) -> List[HistoricalMarket]:
    """Загружает рынки из JSON-файла."""
    with open(path) as f:
        data = json.load(f)
    markets = []
    for d in data:
        end_date = None
        if d.get("end_date"):
            try:
                end_date = datetime.fromisoformat(d["end_date"])
            except ValueError:
                pass
        markets.append(HistoricalMarket(
            market_id=d["market_id"],
            question=d["question"],
            outcome=d["outcome"],
            token_id=d["token_id"],
            entry_price=d["entry_price"],
            final_price=d["final_price"],
            won=d["won"],
            volume_num=d["volume_num"],
            liquidity_num=d["liquidity_num"],
            end_date=end_date,
        ))
    print(f"[Fetcher] Загружено {len(markets)} рынков из {path}")
    return markets
