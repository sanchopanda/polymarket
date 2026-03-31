# real_momentum_bot

Momentum бот с реальными ордерами и виртуальным бюджетом.

## Что делает

- мониторит цены `Polymarket ↔ Kalshi` в live-режиме
- торгует momentum/gap сигналы реальными ордерами
- ограничивает риск через виртуальный бюджет и floor
- хранит состояние в `data/real_momentum_bot.db`

## Как запустить

Из корня репозитория:

```bash
# основной режим
python3 -m real_momentum_bot watch

# статус
python3 -m real_momentum_bot status

# резолюция и retry redeem
python3 -m real_momentum_bot resolve
```

Если хочешь сначала снизить риск, проверь в конфиге размер ставки и бюджет перед `watch`.

## Конфиг

`real_momentum_bot/config.yaml`

Ключевые секции:

- `strategy`
- `budget`
- `market_filter`
- `runtime`
- `db`

Особенно важны:

- `strategy.trades_per_budget`
- `strategy.pm_to_kalshi_test_stake_usd`
- `strategy.reverse_test_stake_usd`
- `budget.total_usd`
- `budget.floor_pct_of_total`

## Переменные окружения

Нужны для реальной торговли:

```env
WALLET_PRIVATE_KEY=...
WALLET_PROXY=...
KALSHI_API_KEY_ID=...
KALSHI_PRIVATE_KEY_PATH=...
```

Опционально для Telegram:

```env
TELEGRAM_TOKEN=...
```
