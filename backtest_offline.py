"""
Бэктест Мартингейл-бота на офлайн-данных из data/markets_180d.json.

Использует ту же логику что и реальный бот (src/backtest/simulator.py),
но вместо API читает локальный файл с историей цен.

Запуск:
  python backtest_offline.py
  python backtest_offline.py --data data/markets_180d.json --balance 100 --runs 5
"""
from __future__ import annotations

import json
import argparse
from datetime import datetime, timezone
from typing import Optional

from src.config import load_config
from src.backtest.fetcher import HistoricalMarket
from src.backtest.simulator import simulate
from src.backtest.report import show_backtest_report


def load_historical_markets(
    data_path: str,
    price_min: float,
    price_max: float,
    max_days_to_expiry: float,
    min_volume: float,
    fee_type: str,
) -> list[HistoricalMarket]:
    """Конвертирует markets_180d.json → List[HistoricalMarket]."""

    print(f"Загрузка {data_path}...")
    with open(data_path) as f:
        data = json.load(f)

    markets = data["markets"]
    meta = data.get("meta", {})
    print(f"Рынков в файле: {len(markets)} (создан: {meta.get('fetched_at', '?')})")

    results: list[HistoricalMarket] = []
    skipped_fee = skipped_vol = skipped_no_entry = included = 0

    for m in markets:
        # Фильтр по fee_type
        if fee_type and m.get("fee_type") != fee_type:
            skipped_fee += 1
            continue

        # Фильтр по объёму
        if m.get("volume_num", 0) < min_volume:
            skipped_vol += 1
            continue

        end_date_str = m.get("end_date")
        if not end_date_str:
            skipped_no_entry += 1
            continue

        try:
            end_date = datetime.fromisoformat(end_date_str)
        except (ValueError, TypeError):
            skipped_no_entry += 1
            continue

        end_ts = end_date.replace(tzinfo=timezone.utc).timestamp() if end_date.tzinfo is None else end_date.timestamp()
        max_seconds_before_expiry = max_days_to_expiry * 86400

        winner_idx = m["winner_idx"]

        for i, token_id in enumerate(m["clob_token_ids"]):
            history = m.get("history", {}).get(token_id, [])
            if not history:
                continue

            won = (i == winner_idx)
            entry_price: Optional[float] = None

            for point in history:
                try:
                    p = float(point["p"])
                    t = float(point["t"])
                except (KeyError, ValueError, TypeError):
                    continue

                if p <= 0:
                    continue

                # Только точки в пределах max_days_to_expiry до экспирации
                seconds_to_expiry = end_ts - t
                if max_days_to_expiry > 0 and not (0 <= seconds_to_expiry <= max_seconds_before_expiry):
                    continue

                if price_min <= p <= price_max:
                    entry_price = p
                    break

            if entry_price is None:
                continue

            final_price = m["outcome_prices"][i] if i < len(m.get("outcome_prices", [])) else (1.0 if won else 0.0)

            results.append(HistoricalMarket(
                market_id=m["id"],
                question=m["question"],
                outcome=m["outcomes"][i],
                token_id=token_id,
                entry_price=entry_price,
                final_price=final_price,
                won=won,
                volume_num=m.get("volume_num", 0),
                liquidity_num=m.get("liquidity_num", 0),
                end_date=end_date,
            ))
            included += 1
            break  # берём первый подходящий исход рынка

    print(
        f"Подходящих возможностей: {included}"
        f" | Пропущено: fee_type={skipped_fee}, volume={skipped_vol}, нет входа={skipped_no_entry}"
    )
    return results


def main():
    parser = argparse.ArgumentParser(description="Офлайн-бэктест Мартингейл-бота")
    parser.add_argument("--data", default="data/markets_180d.json", help="Путь к кешу")
    parser.add_argument("--balance", type=float, default=0, help="Начальный баланс (0 = из config.yaml)")
    parser.add_argument("--runs", type=int, default=3, help="Количество прогонов (разный shuffle)")
    parser.add_argument("--no-fee-filter", action="store_true", help="Не фильтровать по fee_type")
    args = parser.parse_args()

    cfg = load_config()
    strategy = cfg.strategy
    martingale = cfg.martingale
    paper = cfg.paper_trading

    price_min = strategy.price_min if strategy.price_min is not None else strategy.target_price - strategy.price_tolerance
    price_max = strategy.price_max if strategy.price_max is not None else strategy.target_price + strategy.price_tolerance
    fee_type = "" if args.no_fee_filter else strategy.fee_type
    starting_balance = args.balance if args.balance > 0 else paper.starting_balance

    print(f"\n=== Офлайн-бэктест Мартингейл-бота ===")
    print(f"Цена входа:     {price_min}–{price_max}")
    print(f"До экспирации:  ≤{strategy.max_days_to_expiry:.3f}д ({strategy.max_days_to_expiry*24:.1f}ч)")
    print(f"fee_type:       {fee_type or 'все'}")
    print(f"min_volume:     ${strategy.min_volume_24h}")
    print(f"Баланс:         ${starting_balance:.2f}")
    print(f"Глубина:        {martingale.max_series_depth}")
    print(f"Ставка:         ${martingale.initial_bet_size:.2f}")
    print(f"Прогонов:       {args.runs}\n")

    markets = load_historical_markets(
        data_path=args.data,
        price_min=price_min,
        price_max=price_max,
        max_days_to_expiry=strategy.max_days_to_expiry,
        min_volume=strategy.min_volume_24h,
        fee_type=fee_type,
    )

    if not markets:
        print("\nНет данных для бэктеста.")
        return

    win_rates = []
    rois = []

    for run in range(1, args.runs + 1):
        print(f"\n--- Прогон {run}/{args.runs} ---")
        result = simulate(
            markets=markets,
            cfg=martingale,
            taker_fee=paper.taker_fee,
            starting_balance=starting_balance,
            shuffle=True,
        )
        show_backtest_report(
            result=result,
            markets_count=len(markets),
            use_price_history=True,
            starting_balance=starting_balance,
        )
        win_rates.append(result.win_rate * 100)
        rois.append(result.roi)

    if args.runs > 1:
        avg_wr = sum(win_rates) / len(win_rates)
        avg_roi = sum(rois) / len(rois)
        print(f"\n=== Среднее по {args.runs} прогонам ===")
        print(f"  Win rate: {avg_wr:.1f}%")
        print(f"  ROI:      {avg_roi:+.2f}%")


if __name__ == "__main__":
    main()
