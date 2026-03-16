# Polymarket Long-Tail Bot

Бот для paper trading на Polymarket по стратегии "long tail": массовая скупка дешёвых контрактов ($0.001–$0.05) на маловероятные исходы. Большинство сгорят, но несколько выигрышных перекроют убытки с большим мультипликатором.

## Установка

```bash
git clone <repo>
cd polymarket-bot
python3 -m venv venv
source venv/bin/activate
pip install httpx pyyaml rich py-clob-client
```

## Использование

```bash
source venv/bin/activate

# Скан рынков и размещение симулированных ставок
python -m src.main scan

# Только посмотреть кандидатов без сохранения
python -m src.main scan --dry

# Проверить резолюции (какие рынки закрылись)
python -m src.main resolve

# Дашборд: P&L, статистика
python -m src.main dashboard

# Открытые позиции
python -m src.main positions

# История закрытых ставок
python -m src.main history

# Непрерывный режим (скан + резолюции каждые 6 часов)
python -m src.main run --interval 6
```

## Конфиг (config.yaml)

```yaml
strategy:
  min_price: 0.001        # Мин. цена контракта
  max_price: 0.05         # Макс. цена контракта
  min_volume_24h: 100     # Мин. суточный объём ($)
  min_liquidity: 500      # Мин. ликвидность ($)
  max_days_to_expiry: 7   # Только события ближайшей недели

paper_trading:
  budget_total: 200.0     # Общий бюджет симуляции ($)
  bet_size: 0.05          # Размер одной ставки ($)
  taker_fee: 0.02         # Комиссия Polymarket 2%
```

## Как работает стратегия

1. **Скан** — загружает все активные рынки Polymarket через Gamma API
2. **Фильтр** — отбирает контракты с ценой $0.001–$0.05 и экспирацией в ближайшую неделю
3. **Скоринг** — ранжирует по ликвидности, объёму, близости экспирации
4. **Ставки** — симулирует покупку контрактов по $0.05 за штуку
5. **Резолюции** — при закрытии рынка фиксирует выигрыш ($1 за контракт) или проигрыш ($0)

**Математика:** ставка $0.05 на контракт ценой $0.005 даёт 200x при победе. Нужно чтобы хотя бы 1 из 200 ставок сработала в безубыток.

## Структура

```
src/
  api/gamma.py      — Gamma API (поиск рынков)
  api/clob.py       — CLOB API (цены, orderbook)
  strategy/
    scanner.py      — фильтрация кандидатов
    scorer.py       — скоринг и ранжирование
    sizing.py       — размер ставки
  paper/engine.py   — paper trading движок
  db/store.py       — SQLite хранилище
  reports/dashboard.py — отчёты
  main.py           — CLI
data/bot.db         — база данных (создаётся автоматически)
```

## API

Используются публичные эндпоинты Polymarket, авторизация не требуется для paper trading:
- `https://gamma-api.polymarket.com` — список рынков и цен
- `https://clob.polymarket.com` — orderbook (опционально)
