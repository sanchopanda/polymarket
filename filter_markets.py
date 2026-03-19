"""
Фильтрует data/markets_180d.json — оставляет только рынки с историей цен.
Название выходного файла отражает реальный период данных.
"""

import json
from datetime import datetime

INPUT = "data/markets_180d.json"


def has_price_history(market: dict) -> bool:
    history = market.get("history", {})
    if not history:
        return False
    return any(len(points) > 0 for points in history.values())


def main():
    print(f"Загрузка {INPUT}...")
    with open(INPUT) as f:
        data = json.load(f)

    markets = data["markets"]
    meta = data.get("meta", {})
    total = len(markets)

    filtered = [m for m in markets if has_price_history(m)]

    # Вычислить реальный период данных
    dates = []
    for m in filtered:
        if m.get("end_date"):
            try:
                dates.append(datetime.fromisoformat(m["end_date"]))
            except (ValueError, TypeError):
                pass

    if dates:
        min_date, max_date = min(dates), max(dates)
        span_days = (max_date - min_date).days
    else:
        span_days = 0

    output = f"data/markets_{span_days}d.json"

    print(f"Всего рынков: {total}")
    print(f"С историей цен: {len(filtered)}")
    print(f"Без истории (удалено): {total - len(filtered)}")
    print(f"Период данных: {span_days} дней ({min_date.date()} — {max_date.date()})")

    data_out = {"meta": meta, "markets": filtered}

    with open(output, "w") as f:
        json.dump(data_out, f, ensure_ascii=False)

    print(f"\nСохранено в {output}")


if __name__ == "__main__":
    main()
