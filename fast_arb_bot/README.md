# fast_arb_bot

Быстрый реальный арбитражный бот `Polymarket ↔ Kalshi` с параллельным исполнением ног.

## Что делает

- работает поверх `real_arb_bot` engine и БД
- ищет короткие lock-окна и пытается исполнить ноги параллельно
- отдельно отслеживает completion/reentry логику
- хранит данные в `data/fast_arb_bot.db`

## Как запустить

Из корня репозитория:

```bash
# безопасный старт: watch без ордеров
python3 -m fast_arb_bot watch --dry

# основной live watch
python3 -m fast_arb_bot watch

# статус, балансы, P&L и одноногие
python3 -m fast_arb_bot status

# резолюция
python3 -m fast_arb_bot resolve
```

## Конфиг

`fast_arb_bot/config.yaml`

Перед запуском проверь:

- `safety.dry_run`
- `execution.first_leg` (`parallel`)
- `trading.stake_per_pair_usd`
- `fast_arb.budget_usd`
- `fast_arb.max_realized_loss_usd`
- `safety.max_oracle_gap_pct`

## Переменные окружения

Нужны для реальной торговли:

```env
WALLET_PRIVATE_KEY=...
WALLET_PROXY=...
KALSHI_API_KEY_ID=...
KALSHI_PRIVATE_KEY_PATH=...
```
