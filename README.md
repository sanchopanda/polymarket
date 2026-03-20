# Polymarket Repo

Монорепозиторий с несколькими ботами и исследовательскими скриптами:

- `src/` — основной Polymarket Martingale bot: paper trading, real trading, загрузка исторических данных и backtest.
- `simple_bot/` — упрощённый paper bot без Мартингейла.
- `ev_bot/` — отдельный paper bot с EV-фильтром по историческим бакетам.
- `bybit-bot/` — отдельный Bybit futures bot.
- корень репозитория — диагностические и backtest-скрипты.

Код и CLI в основном на русском языке. Основные пути данных по умолчанию:

- `config.yaml`
- `data/bot.db`
- `data/real.db`
- `data/simple_bot.db`
- `data/ev_bot.db`
- `data/backtest_markets.json`

## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

В текущем `pyproject.toml` описаны только зависимости основного Polymarket-бота. Для real trading и части диагностических скриптов дополнительно понадобятся пакеты вроде `web3`, `eth-account` и `py-clob-client`.

## Основной бот: `python -m src.main`

Глобальные параметры:

- `--config` — путь к YAML-конфигу, по умолчанию `config.yaml`.

Команды:

- `scan`
  - `--dry` — только показать кандидатов, не сохранять серии.
- `resolve`
  - без параметров.
- `series`
  - `--real` — показывать данные real trading БД.
- `dashboard`
  - `--real` — показывать данные real trading БД.
- `positions`
  - `--real` — показывать данные real trading БД.
- `history`
  - `--real` — показывать данные real trading БД.
- `run`
  - `--interval FLOAT` — интервал цикла в часах, по умолчанию `0.033` (около 2 минут).
- `fetch`
  - `--limit INT` — число закрытых рынков, по умолчанию `10000`.
  - `--no-price-history` — не загружать историю цен, использовать упрощённый вход.
  - `--workers INT` — число параллельных CLOB-запросов, по умолчанию `20`.
  - `--output PATH` — путь к JSON, по умолчанию `data/backtest_markets.json`.
- `backtest`
  - `--cache PATH` — читать рынки из готового JSON вместо API.
  - `--limit INT` — число закрытых рынков для загрузки, по умолчанию `300`.
  - `--no-price-history` — не загружать историю цен.
  - `--initial-bet FLOAT` — переопределить стартовую ставку.
  - `--depth INT` — переопределить максимальную глубину серии.
  - `--balance FLOAT` — переопределить стартовый баланс.
  - `--workers INT` — число параллельных CLOB-запросов, по умолчанию `20`.
- `real balance`
  - без параметров.
- `real scan`
  - `--dry` — не размещать реальные ордера.
- `real resolve`
  - без параметров.
- `real redeem`
  - без параметров.
- `real run`
  - `--interval FLOAT` — интервал в часах, по умолчанию `0.167` (около 10 минут).

Примеры:

```bash
python3 -m src.main scan --dry
python3 -m src.main run --interval 0.05
python3 -m src.main fetch --limit 5000 --workers 30
python3 -m src.main backtest --cache data/backtest_markets.json --balance 200 --depth 6
python3 -m src.main real scan --dry
```

## Конфиг `config.yaml`

Ключевые секции:

- `strategy`
  - `target_price`
  - `price_tolerance`
  - `price_min`
  - `price_max`
  - `fee_type`
  - `min_volume_24h`
  - `min_liquidity`
  - `min_days_to_expiry`
  - `max_days_to_expiry`
  - `categories`
- `martingale`
  - `initial_bet_size`
  - `max_series_depth`
  - `max_active_series`
- `real_martingale`
  - `initial_bet_size`
  - `max_series_depth`
  - `max_active_series`
  - `starting_balance`
- `paper_trading`
  - `starting_balance`
  - `check_liquidity`
  - `taker_fee`
- `api`
  - `gamma_base_url`
  - `clob_base_url`
  - `request_delay_ms`
  - `page_size`
- `db`
  - `path`
  - `real_path`
- `reports`
  - `max_rows`
- `telegram`
  - `token`
- `wallet`
  - `private_key`
  - `chain_id`
  - `proxy`

Значения из окружения:

- `BOT_CONFIG` — альтернативный путь к конфигу.
- `TELEGRAM_TOKEN` — токен Telegram для основного бота.
- `WALLET_PRIVATE_KEY` — приватный ключ для real trading.
- `WALLET_PROXY` — proxy wallet для Polymarket/Magic.

## `simple_bot`

Запуск:

```bash
python3 -m simple_bot <command>
```

Команды:

- `scan`
  - `--dry` — показать кандидатов без записи ставки.
- `resolve`
  - без параметров.
- `status`
  - без параметров.
- `bets`
  - `--status {open,won,lost}` — фильтр по статусу ставки.
- `run`
  - `--interval FLOAT` — интервал в минутах, по умолчанию `60`.

Конфиг: `simple_bot/config.yaml`.

Параметры конфига:

- `strategy.price_min`
- `strategy.price_max`
- `strategy.max_days_to_expiry`
- `strategy.min_volume`
- `trading.starting_balance`
- `trading.bet_size`
- `trading.taker_fee`
- `db.path`
- `telegram.token`

Переменная окружения:

- `SIMPLE_BOT_TOKEN` — переопределяет `telegram.token`.

## `ev_bot`

Запуск:

```bash
python3 -m ev_bot [--config ev_bot/ev_config.yaml] [--main-config config.yaml] <command>
```

Глобальные параметры:

- `--config PATH` — путь к `ev_config.yaml`.
- `--main-config PATH` — путь к основному `config.yaml`, используется для API URL.

Команды:

- `fetch`
  - `--limit INT` — по умолчанию `10000`.
  - `--hours FLOAT` — снимок за N часов до экспирации, по умолчанию `2.0`.
  - `--days FLOAT` — брать рынки, закрытые за последние N дней, по умолчанию `30.0`.
  - `--workers INT` — по умолчанию `20`.
  - `--output PATH` — путь выходного файла; если не задан, берётся из конфига.
- `analyze`
  - `--min-samples INT` — переопределить `ev_filter.min_samples`.
- `scan`
  - `--dry` — не сохранять ставки.
- `resolve`
  - без параметров.
- `run`
  - `--interval FLOAT` — интервал в часах, по умолчанию `0.033`.
- `dashboard`
  - без параметров.

Конфиг: `ev_bot/ev_config.yaml`.

Параметры конфига:

- `ev_filter.cache_path`
- `ev_filter.min_samples`
- `ev_filter.recalc_interval`
- `strategy.price_min`
- `strategy.price_max`
- `strategy.min_volume`
- `strategy.min_liquidity`
- `strategy.max_days_to_expiry`
- `strategy.fee_type`
- `martingale.initial_bet`
- `martingale.max_depth`
- `martingale.taker_fee`
- `martingale.starting_balance`
- `db.path`

## `bybit-bot`

Это отдельный подпроект со своим README в `bybit-bot/README.md`.

Запуск из каталога `bybit-bot/`:

```bash
python3 -m src.main [--config config.yaml] <command>
```

Команды:

- `run`
  - `--interval INT` — интервал проверки в секундах, по умолчанию `10`.
- `check`
  - без параметров.
- `open`
  - без параметров.
- `dashboard`
  - без параметров.
- `series`
  - без параметров.
- `positions`
  - без параметров.
- `balance`
  - без параметров.

## Backtest и исследовательские скрипты в корне

- `python3 fetch_data.py`
  - `--days FLOAT`
  - `--min-volume FLOAT`
  - `--workers INT`
  - `--output PATH`
- `python3 backtest_sim.py`
  - `--data PATH`
  - `--limit INT`
  - `--days FLOAT`
  - `--min-price FLOAT`
  - `--max-price FLOAT`
  - `--min-volume FLOAT`
  - `--deposit FLOAT`
  - `--bet FLOAT`
  - `--max-expiry FLOAT`
  - `--min-expiry FLOAT`
  - `--workers INT`
- `python3 backtest_martingale.py`
  - `--data PATH` — обязательно.
  - `--days FLOAT`
  - `--min-price FLOAT`
  - `--max-price FLOAT`
  - `--min-volume FLOAT`
  - `--deposit FLOAT`
  - `--bet FLOAT`
  - `--max-depth INT`
  - `--max-expiry FLOAT`
- `python3 backtest_offline.py`
  - `--data PATH`
  - `--balance FLOAT`
  - `--runs INT`
  - `--no-fee-filter`
- `python3 backtest_highx.py`
  - `--limit INT`
  - `--max-price FLOAT`
  - `--min-price FLOAT`
  - `--min-volume FLOAT`
  - `--min-liquidity FLOAT`
  - `--days FLOAT`
  - `--bet-size FLOAT`
  - `--workers INT`
- `python3 backtest_buckets.py`
  - `--data PATH`
  - `--limit INT`
  - `--days FLOAT`
  - `--bet-size FLOAT`
  - `--min-volume FLOAT`
  - `--workers INT`
- `python3 scripts/compare_strategies.py`
  - `--data PATH`
  - `--balance FLOAT`
  - `--bet FLOAT`
  - `--depth INT`
  - `--seed INT`

## Диагностические и сервисные скрипты

Скрипты без CLI-параметров, завязанные на `.env` и/или захардкоженные значения:

- `approve_usdc.py` — делает `approve` USDC для контрактов Polymarket.
- `check_account.py` — диагностирует адрес и CLOB account.
- `check_balance.py` — сравнивает баланс и allowance для EOA/proxy.
- `check_highx.py` — ищет дешёвые long-tail исходы на активных рынках.
- `check_key.py` — проверяет, какому адресу соответствует приватный ключ.
- `check_onchain.py` — on-chain диагностика балансов и allowances в Polygon.
- `check_order.py` — смотрит статус захардкоженного ордера.
- `filter_markets.py` — фильтрует локальный JSON по наличию price history.
- `test_order.py` — локальный тест размещения ордера через CLOB API.

Для них обычно нужны переменные окружения:

- `WALLET_PRIVATE_KEY`
- `WALLET_PROXY`

## Структура проекта

```text
src/
  api/             # Gamma и CLOB клиенты
  backtest/        # загрузка исторических рынков, симулятор, отчёты
  db/              # SQLite модели и store
  paper/           # paper trading engine
  real/            # real trading engine
  reports/         # Rich dashboard
  strategy/        # сканер, скоринг, sizing
  config.py        # загрузка config.yaml
  main.py          # основной CLI
simple_bot/        # отдельный упрощённый бот
ev_bot/            # отдельный EV-бот
bybit-bot/         # отдельный подпроект
tests/             # unit tests
scripts/           # исследовательские утилиты
```
