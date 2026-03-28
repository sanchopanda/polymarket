# volatility_bot

Направленный бот для коротких крипто-рынков на Polymarket и Kalshi.

В отличие от арб-ботов, ставит **одну ногу** на основе цены и позиции внутри временного окна рынка.

---

## Запуск

```bash
# Paper mode (по умолчанию, dry_run: true в config.yaml)
python3 -m volatility_bot run

# Посмотреть активные рынки без запуска бота
python3 -m volatility_bot --dry-run scan

# Список ставок в БД
python3 -m volatility_bot bets
python3 -m volatility_bot bets --limit 100

# Заресолвить открытые ставки вручную
python3 -m volatility_bot resolve

# Live режим (dry_run: false в config.yaml)
python3 -m volatility_bot run
```

---

## Рынки

| Тип         | Venue          | Серии Kalshi                          |
|-------------|----------------|---------------------------------------|
| 5 минут     | Kalshi         | KXBTC5M, KXETH5M, KXSOL5M, …         |
| 15 минут    | Polymarket     | авто-обнаружение по заголовку         |
| 15 минут    | Kalshi         | KXBTC15M, KXETH15M, KXSOL15M, …      |
| 1 час       | Kalshi         | KXBTC1H, KXETH1H, KXSOL1H, …         |

Фильтр: `volume ≥ 500`, `fee_type = crypto_fees`, интервалы `[5, 15, 60]` минут.

---

## Стратегия

Рынок делится на 4 равные четверти. На каждый рынок — максимум 3 ставки (по одной на бакет).

| Бакет       | Диапазон цен | Когда ставим        |
|-------------|--------------|---------------------|
| `0-0.1`     | 0–10 центов  | Любая четверть      |
| `0.2-0.4`   | 20–40 центов | Любая четверть      |
| `0.85-0.95` | 85–95 центов | Последняя четверть  |

---

## База данных

`data/volatility_bot.db` — таблица `bets`.

Ключевые поля:

| Поле             | Описание                                      |
|------------------|-----------------------------------------------|
| `market_minute`  | Минута рынка, на которой сделана ставка (0-N) |
| `market_quarter` | Четверть рынка (1–4)                          |
| `position_pct`   | Доля прошедшего времени (0.0–1.0)             |
| `trigger_bucket` | Сработавший бакет                             |
| `is_legacy`      | 1 = мигрировано из старых ботов               |
| `is_paper`       | 1 = paper / dry-run ставка                    |

### Legacy миграция

554 ставки из предыдущих ботов загружены с `is_legacy=1`:

```bash
python3 scripts/migrate_legacy_bets.py
```

---

## Конфиг

`volatility_bot/config.yaml`

```yaml
runtime:
  dry_run: true          # false для live

trading:
  stake_usd: 25.0        # размер ставки в USD

market_filter:
  min_volume: 500
  interval_minutes: [5, 15, 60]
```

---

## Требования

`.env` в корне проекта (нужен только для live):

```
WALLET_PRIVATE_KEY=...
WALLET_PROXY=...
KALSHI_API_KEY_ID=...
KALSHI_PRIVATE_KEY_PATH=...
```
