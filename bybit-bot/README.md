# Bybit Martingale Bot

Бот для торговли на Bybit Futures (демо-аккаунт) по стратегии Мартингейла.

## Стратегия

Открывает Long или Short позицию на выбранном символе с тейк-профитом 1.2% и стоп-лоссом 1.0%.

- **Победа (TP):** серия завершается, прибыль > 100% от начальной маржи.
- **Проигрыш (SL):** удваивает маржу и открывает позицию на другом символе (следующая глубина серии).
- **Лимит глубины:** если достигнуто `max_series_depth` — серия считается брошенной.

### Пример серии (глубина 3)

| Depth | Символ   | Маржа    | Позиция (100×) | Результат |
|-------|----------|----------|----------------|-----------|
| 0     | XRPUSDT  | $0.10    | $10            | SL (-$0.10) |
| 1     | DOGEUSDT | $0.20    | $20            | SL (-$0.20) |
| 2     | SOLUSDT  | $0.40    | $40            | TP (+$0.48 до вычета комиссий) |

Итог серии: суммарные потери $0.30 + прибыль $0.48 → **+$0.18 чистыми**.

## Установка

```bash
cd bybit-bot
pip install -r requirements.txt
```

Или через venv родительского репозитория (если уже установлен `httpx`, `rich`, `pyyaml`):

```bash
cd bybit-bot
source ../venv/bin/activate
python3 -m src.main balance
```

## Конфигурация

Файл `config.yaml`:

```yaml
bybit:
  mode: "demo"           # demo | testnet | mainnet
  api_key: "..."
  api_secret: "..."
  symbols:               # Символы для серий (ротация при эскалации)
    - "XRPUSDT"
    - "DOGEUSDT"
    - "SOLUSDT"
    - "BNBUSDT"
    - "ETHUSDT"
  leverage: 100

martingale:
  initial_margin_usdt: 0.10  # Начальная маржа ($)
  take_profit_pct: 1.2       # Тейк-профит %
  stop_loss_pct: 1.0         # Стоп-лосс %
  max_series_depth: 10       # Макс. удвоений
  max_active_series: 3       # Параллельных серий

db:
  path: "data/bot.db"

reports:
  starting_balance: 50.0
  max_rows: 50
```

### Получение API-ключей (демо)

1. Зайти на [bybit.com](https://bybit.com) → включить **Demo Trading** (верхняя панель)
2. В демо-аккаунте: `Аккаунт → Настройки API → Создать ключ`
3. Тип: системный ключ, права: чтение + торговля
4. Вставить ключ и секрет в `config.yaml`

> Ключи от `testnet.bybit.com` и от демо-режима `bybit.com` — разные. Для демо нужны только ключи из демо-режима.

## Использование

```bash
# Запустить бота в непрерывном режиме (цикл каждые 10 сек)
python3 -m src.main run

# Изменить интервал
python3 -m src.main run --interval 30

# Разовые команды
python3 -m src.main check       # проверить открытые позиции
python3 -m src.main open        # открыть новые серии
python3 -m src.main balance     # баланс Bybit-аккаунта

# Отчёты
python3 -m src.main dashboard   # полный дашборд
python3 -m src.main series      # таблица серий
python3 -m src.main positions   # открытые позиции
```

## Расчёт прибыли

При TP = 1.2% и плече 100×:

| Параметр | Значение |
|----------|----------|
| Маржа | $0.10 |
| Размер позиции | $10.00 (100×) |
| Прибыль от TP (1.2%) | $0.12 |
| Комиссии taker (0.055% × 2) | −$0.011 |
| **Чистая прибыль** | **$0.109** |
| **ROI от маржи** | **109%** |

## Структура файлов

```
bybit-bot/
├── config.yaml
├── requirements.txt
├── data/
│   └── bot.db          # SQLite (создаётся автоматически)
└── src/
    ├── main.py          # CLI
    ├── config.py        # Конфиг
    ├── api/
    │   └── bybit.py     # Bybit V5 REST клиент
    ├── db/
    │   ├── models.py    # Series, Trade
    │   └── store.py     # SQLite хранилище
    ├── engine/
    │   └── martingale.py  # Основная логика
    └── reports/
        └── dashboard.py   # Rich дашборд
```
