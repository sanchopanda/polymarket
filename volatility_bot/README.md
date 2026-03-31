# volatility_bot

Направленный бот для коротких крипто-рынков на `Polymarket` и `Kalshi`.

В отличие от арб-ботов, бот открывает одну сторону рынка по ценовым бакетам и положению внутри временного окна.

## Что делает

- сканирует 5m, 15m и 60m крипто-рынки
- делит рынок на временные четверти
- открывает paper или real ставку по правилам бакета
- сохраняет историю ставок в `data/volatility_bot.db`

## Как запустить

Из корня репозитория:

```bash
# основной режим
python3 -m volatility_bot run

# разовый scan без реальных сделок
python3 -m volatility_bot --dry-run scan

# список ставок
python3 -m volatility_bot bets
python3 -m volatility_bot bets --limit 100

# ручной resolve
python3 -m volatility_bot resolve
```

Важно: live/paper определяется через `runtime.dry_run` в `volatility_bot/config.yaml`.

## Стратегия

На каждый рынок максимум 3 ставки, по одной на бакет:

- `0-0.1` — цена 0-10 центов
- `0.2-0.4` — цена 20-40 центов
- `0.85-0.95` — цена 85-95 центов, только в последней четверти

## Конфиг

`volatility_bot/config.yaml`

Ключевые секции:

- `runtime.dry_run`
- `trading.stake_usd`
- `trading.max_price`
- `market_filter`
- `db.path`

## Переменные окружения

Для live-режима:

```env
WALLET_PRIVATE_KEY=...
WALLET_PROXY=...
KALSHI_API_KEY_ID=...
KALSHI_PRIVATE_KEY_PATH=...
```

Опционально для Telegram:

```env
SIMPLE_BOT_TOKEN=...
```
