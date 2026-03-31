# momentum_bot

Paper-trading momentum бот `Polymarket ↔ Kalshi`.

## Что делает

- отслеживает лидирующую площадку
- ищет спайк или устойчивый ценовой гэп
- открывает paper-позицию на отстающей стороне
- ведёт виртуальный баланс и P&L в `data/momentum_bot.db`

## Как запустить

Из корня репозитория:

```bash
# основной live-monitoring режим
python3 -m momentum_bot watch

# статус и P&L
python3 -m momentum_bot status

# резолюция истёкших позиций
python3 -m momentum_bot resolve
```

## Конфиг

`momentum_bot/config.yaml`

Ключевые секции:

- `strategy`
- `market_filter`
- `runtime`
- `db`

Особенно важны:

- `strategy.spike_threshold_cents`
- `strategy.max_entry_price`
- `strategy.max_price_gap_cents`
- `strategy.stake_per_trade_usd`
- `strategy.starting_balance`

## Переменные окружения

Для paper-режима не нужны.
