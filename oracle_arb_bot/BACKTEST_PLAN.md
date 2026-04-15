# Oracle Arb Bot — план анализа данных

## Что собираем

Таблица `price_ticks` в `data/oracle_arb_bot.db` — каждую секунду для каждого активного PM рынка (5m и 15m):

| Поле | Описание |
|---|---|
| `binance_price` | Текущая цена Binance spot |
| `pm_open_price` | Reference/страйк (цена на момент открытия рынка) |
| `pm_yes_ask`, `pm_no_ask` | Цены продажи YES/NO на PM |
| `pm_yes_bid`, `pm_no_bid` | Цены покупки YES/NO на PM |
| `delta_pct` | `(binance - reference) / reference * 100` |
| `seconds_to_expiry` | Секунды до закрытия рынка |
| `symbol` | BTC, ETH, SOL, XRP |
| `interval_minutes` | 5 или 15 |

Данные пишутся для ВСЕХ рынков, не только тех где была ставка.

## Текущая стратегия (paper)

- Режим: `crossing`
- Сигнал: `|delta_pct| > 0.10%` И `pm_ask < $0.40`
- Ставка: $5 paper
- Символы: BTC, ETH, SOL, XRP
- Интервалы: 5m, 15m
- Проверка ликвидности: $10 в стакане до $0.40

## Первый бэктест (на данных fast_arb_bot)

Был проведён на 256 позициях из `fast_arb_bot` (только 15m рынки). Результаты:
- dist>0.10%, ask<$0.40: 38 сделок, 53% winrate, PnL +$394 на $190 invested (ROI +207%)
- Прибыль за счёт асимметрии: проигрыш = -$5, выигрыш = +$10..+$160
- BTC — 100% winrate, но PM быстро реагирует (мало дешёвых сигналов)
- XRP — больше всего сигналов, но больше шума (88% accuracy)
- Скрипты: `scripts/backtest_binance_signal.py`, `scripts/backtest_binance_momentum.py`

## Что нужно проанализировать после сбора данных

### 1. Per-symbol пороги delta_pct

BTC более ликвидный — PM реагирует быстрее. XRP/SOL менее ликвидны — PM лагает дольше.
Вопрос: оптимальный `delta_threshold` для каждого символа отдельно.

### 2. 5m vs 15m рынки

Бэктест был только на 15m. На 5m рынках:
- Меньше времени для движения Binance → меньше сигналов?
- PM может лагать по-другому (меньше ликвидности на 5m?)
- Нужно сравнить: кол-во сигналов, winrate, avg ask при сигнале

### 3. Время внутри рынка (position_pct / market_minute)

Когда лучше входить?
- В начале рынка (5-10 мин): PM ещё не знает направление, ask дешевле
- В конце (последние 1-2 мин): Binance уже почти финальный, но PM уже дорогой
- Оптимальное окно входа по STE (seconds_to_expiry)

### 4. Скорость реакции PM (lag analysis)

Ключевой вопрос: как быстро PM догоняет Binance?
- Для каждого тика: delta_pct vs pm_ask правильной стороны
- Scatter plot: |delta| vs ask → видна ли линейная зависимость?
- Lag в секундах: после пересечения страйка, сколько секунд PM ask < 0.50?

### 5. Оптимальный max_ask

Текущий порог $0.40 — из бэктеста это лучший ROI. Но:
- При $0.50-0.60 — больше сделок, ниже ROI но выше total PnL
- Trade-off: больше сигналов vs выше цена входа
- Возможно разный max_ask для разных символов

### 6. Bid-ask spread и ликвидность

Новые данные включают bids. Это позволит:
- Посчитать spread (ask - bid) в момент сигнала
- Оценить реальную стоимость выхода (если бы хотели продать до экспирации)
- Оценить глубину стакана по времени суток

### 7. Время суток / волатильность

- Есть ли время суток когда PM лагает больше? (ночь, азиатская сессия)
- Корреляция с общей волатильностью Binance
- Влияние объёма торгов на PM на качество сигнала

## Как запускать бэктест на новых данных

```python
# Данные в data/oracle_arb_bot.db, таблица price_ticks
# Исходы рынков — в таблице bets (winning_side) или через API резолвера

import sqlite3
con = sqlite3.connect('data/oracle_arb_bot.db')

# Все тики для анализа
cur = con.execute("""
    SELECT symbol, interval_minutes, ts, seconds_to_expiry,
           binance_price, pm_open_price, pm_yes_ask, pm_no_ask,
           pm_yes_bid, pm_no_bid, delta_pct
    FROM price_ticks
    ORDER BY ts
""")
```

Для резолюции (кто победил) — нужно либо:
- Сопоставить market_id из price_ticks с bets.winning_side
- Либо добавить winning_side в отдельную таблицу market_outcomes (TODO)
