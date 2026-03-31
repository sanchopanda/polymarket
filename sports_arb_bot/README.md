# sports_arb_bot

Paper-trading бот для спортивного арбитража `Polymarket ↔ Kalshi`.

## Что делает

- матчит спортивные события двух площадок
- ищет lock-арбитраж по двум комплементарным исходам
- открывает только paper-позиции
- ведёт баланс и позиции в `data/sports_arb_bot.db`

Сейчас основной сценарий в коде и документации заточен под теннис.

## Как запустить

Из корня репозитория:

```bash
# основной режим
python3 -m sports_arb_bot watch

# статус и позиции
python3 -m sports_arb_bot status

# ручной резолв завершённых матчей
python3 -m sports_arb_bot resolve

# разовый scan без запуска watch-loop
python3 -m sports_arb_bot scan
python3 -m sports_arb_bot scan --sports wta atp --min-edge 0.02

# посмотреть тэги PM рынков
python3 -m sports_arb_bot tags --limit 500
```

## Конфиг

`sports_arb_bot/config.yaml`

Ключевые параметры:

- `trading.stake_usd`
- `trading.min_edge`
- `trading.min_leg_price`
- `trading.max_leg_price`
- `trading.scan_interval_seconds`
- `sports`

## Как работает

- discovery: скачивает рынки PM и Kalshi и матчится без обязательного LLM
- live режим: держит websocket на ценах и пересчитывает edge на каждом апдейте
- вход: пишет paper-позицию только после проверки доступной ликвидности
- resolve: проверяет завершённые матчи и закрывает позицию в БД

## Переменные окружения

Для paper-режима не обязательны.

Опционально:

```env
TELEGRAM_TOKEN=...
OPENROUTER_API_KEY=...
```

`TELEGRAM_TOKEN` нужен для уведомлений, `OPENROUTER_API_KEY` только если используешь LLM-based matcher.
