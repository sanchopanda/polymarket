# oracle_arb_bot

Бот для коротких крипто-рынков, который сравнивает цену Polymarket с внешним референсом (`Chainlink` или `Binance`) и ищет недооценённые `YES/NO`.

## Что делает

- сканирует 5m и 15m рынки по `BTC`, `ETH`, `SOL`, `XRP`
- считает сигнал по отклонению цены рынка от oracle-цены
- умеет вести paper-учёт и реальные ставки на `Polymarket`
- сохраняет ставки и статус в `data/oracle_arb_bot.db`

## Как запустить

Из корня репозитория:

```bash
# безопасно: один проход без непрерывного цикла
python3 -m oracle_arb_bot scan

# основной режим: непрерывный запуск
python3 -m oracle_arb_bot run

# посмотреть последние ставки
python3 -m oracle_arb_bot bets
python3 -m oracle_arb_bot bets --limit 100

# вручную зарезолвить истёкшие ставки
python3 -m oracle_arb_bot resolve
```

Важно: в текущем `oracle_arb_bot/config.yaml` `real_trading.enabled: true`. Если не хочешь реальные ставки, перед запуском `run` отключи real trading в конфиге.

## Конфиг

Основной конфиг: `oracle_arb_bot/config.yaml`

Ключевые секции:

- `price_source` — `chainlink` или `binance`
- `runtime.scan_interval_seconds` — частота скана
- `trading` — paper-параметры
- `real_trading` — реальные ставки и риск-лимиты
- `market_filter` — фильтр рынков
- `strategy` — пороги входа
- `db.path` — путь к SQLite БД

## Переменные окружения

Для реальных ставок на Polymarket:

```env
WALLET_PRIVATE_KEY=...
WALLET_PROXY=...
```

Опционально:

```env
KALSHI_API_KEY_ID=...
KALSHI_PRIVATE_KEY_PATH=...
SIMPLE_BOT_TOKEN=...
```

`KALSHI_*` нужны только для дополнительных Kalshi REST/WS данных, `SIMPLE_BOT_TOKEN` для Telegram-уведомлений.
