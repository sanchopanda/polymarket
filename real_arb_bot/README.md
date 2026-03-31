# real_arb_bot

Реальный кросс-маркет арбитражный бот `Polymarket ↔ Kalshi`.

## Что делает

- ищет lock-арбитраж между двумя площадками
- исполняет реальные ордера по двум ногам
- ведёт аудит API-вызовов, P&L и состояние позиций
- хранит данные в `data/real_arb_bot.db`

## Как запустить

Из корня репозитория:

```bash
# сначала безопасный dry-run
python3 -m real_arb_bot scan --dry

# статус балансов и позиций
python3 -m real_arb_bot status

# разовый реальный scan
python3 -m real_arb_bot scan

# websocket watch
python3 -m real_arb_bot watch

# непрерывный цикл
python3 -m real_arb_bot run --interval 20
python3 -m real_arb_bot run --interval 20 --dry

# резолюция
python3 -m real_arb_bot resolve

# аудит
python3 -m real_arb_bot audit --last 50

# одноногие позиции
python3 -m real_arb_bot orphans
```

## Конфиг

`real_arb_bot/config.yaml`

Перед live-запуском проверь:

- `safety.require_confirmation`
- `safety.dry_run`
- `safety.cooldown_seconds`
- `execution.first_leg`
- `trading.stake_per_pair_usd`

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
SIMPLE_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```
