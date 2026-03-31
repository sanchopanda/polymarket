# arb_bot

Изолированный paper-trading бот для арбитража внутри одного рынка Polymarket: покупает обе комплементарные стороны, если сумма ask-цен и комиссий даёт lock-profit.

## Что делает

- ищет short-term crypto markets на `Polymarket`
- проверяет арбитраж по обеим ногам через ask-стакан
- открывает только paper-позиции
- хранит состояние в `data/arb_bot.db`

## Как запустить

Из корня репозитория:

```bash
# разовый поиск без записи
python3 -m arb_bot scan --dry

# разовый поиск с записью paper-позиции
python3 -m arb_bot scan

# статус портфеля
python3 -m arb_bot status

# резолюция закрывшихся рынков
python3 -m arb_bot resolve

# циклический paper-режим
python3 -m arb_bot run --interval 1

# live websocket-мониторинг Polymarket
python3 -m arb_bot ws
```

## Конфиг

`arb_bot/config.yaml`

Ключевые параметры:

- `strategy.min_edge`
- `strategy.max_payout_per_trade`
- `strategy.max_open_positions`
- `trading.starting_balance`
- `trading.taker_fee`
- `db.path`

## Переменные окружения

Для обычного paper-режима не нужны.

Если используешь `arb_bot/kalshi_ws.py` отдельно, понадобятся:

```env
KALSHI_API_KEY_ID=...
KALSHI_PRIVATE_KEY_PATH=...
```
