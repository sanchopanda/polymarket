# AGENTS.md

Этот файл — основная точка входа для кодовых агентов в этом репозитории. `CLAUDE.md` должен рассматриваться только как редирект сюда.

## Что это за репозиторий

В одном репозитории живут несколько независимых CLI-проектов:

- корневой `src/` — основной Polymarket Martingale bot;
- `simple_bot/` — простой paper trading bot без Мартингейла;
- `ev_bot/` — отдельный EV-ориентированный paper bot;
- `bybit-bot/` — отдельный Bybit futures bot;
- набор исследовательских и диагностических Python-скриптов в корне.

Рабочий язык проекта: русский. Новые README, help-тексты, комментарии и агентские заметки лучше держать в том же стиле.

## Как работать с репозиторием

- Сначала смотри на фактические CLI в коде, а не на старую документацию.
- Основной CLI находится в `src/main.py`.
- Конфиг основного бота: `config.yaml`.
- Если меняешь команды или аргументы CLI, обновляй как минимум `README.md` и этот файл.
- Для `simple_bot` используй `simple_bot/config.yaml`.
- Для `ev_bot` используй `ev_bot/ev_config.yaml` и при необходимости основной `config.yaml`.
- `bybit-bot` живёт отдельно и имеет собственный конфиг/README.
- Многие диагностические скрипты требуют `.env` с `WALLET_PRIVATE_KEY`, `WALLET_PROXY`, `TELEGRAM_TOKEN` или `SIMPLE_BOT_TOKEN`.

## Быстрые ориентиры по данным

- `data/bot.db` — paper trading основного бота.
- `data/real.db` — real trading основного бота.
- `data/simple_bot.db` — база simple bot.
- `data/ev_bot.db` — база EV-бота.
- `data/backtest_markets.json` — основной JSON-кэш для исторических рынков.

## Команды: основной бот `python3 -m src.main`

Глобально:

- `--config PATH` — путь к `config.yaml`.

Команды и параметры:

- `scan`
  - `--dry`
- `resolve`
- `series`
  - `--real`
- `dashboard`
  - `--real`
- `positions`
  - `--real`
- `history`
  - `--real`
- `run`
  - `--interval FLOAT`
- `fetch`
  - `--limit INT`
  - `--no-price-history`
  - `--workers INT`
  - `--output PATH`
- `backtest`
  - `--cache PATH`
  - `--limit INT`
  - `--no-price-history`
  - `--initial-bet FLOAT`
  - `--depth INT`
  - `--balance FLOAT`
  - `--workers INT`
- `real balance`
- `real scan`
  - `--dry`
- `real resolve`
- `real redeem`
- `real run`
  - `--interval FLOAT`

## Команды: `python3 -m simple_bot`

- `scan`
  - `--dry`
- `resolve`
- `status`
- `bets`
  - `--status {open,won,lost}`
- `run`
  - `--interval FLOAT`

## Команды: `python3 -m ev_bot`

Глобально:

- `--config PATH`
- `--main-config PATH`

Подкоманды:

- `fetch`
  - `--limit INT`
  - `--hours FLOAT`
  - `--days FLOAT`
  - `--workers INT`
  - `--output PATH`
- `analyze`
  - `--min-samples INT`
- `scan`
  - `--dry`
- `resolve`
- `run`
  - `--interval FLOAT`
- `dashboard`

## Команды: `bybit-bot`

Запускать из `bybit-bot/`:

- `python3 -m src.main run --interval INT`
- `python3 -m src.main check`
- `python3 -m src.main open`
- `python3 -m src.main dashboard`
- `python3 -m src.main series`
- `python3 -m src.main positions`
- `python3 -m src.main balance`

Глобально:

- `--config PATH`

## Корневые backtest- и utility-команды

- `python3 fetch_data.py --days FLOAT --min-volume FLOAT --workers INT --output PATH`
- `python3 backtest_sim.py --data PATH --limit INT --days FLOAT --min-price FLOAT --max-price FLOAT --min-volume FLOAT --deposit FLOAT --bet FLOAT --max-expiry FLOAT --min-expiry FLOAT --workers INT`
- `python3 backtest_martingale.py --data PATH --days FLOAT --min-price FLOAT --max-price FLOAT --min-volume FLOAT --deposit FLOAT --bet FLOAT --max-depth INT --max-expiry FLOAT`
- `python3 backtest_offline.py --data PATH --balance FLOAT --runs INT --no-fee-filter`
- `python3 backtest_highx.py --limit INT --max-price FLOAT --min-price FLOAT --min-volume FLOAT --min-liquidity FLOAT --days FLOAT --bet-size FLOAT --workers INT`
- `python3 backtest_buckets.py --data PATH --limit INT --days FLOAT --bet-size FLOAT --min-volume FLOAT --workers INT`
- `python3 scripts/compare_strategies.py --data PATH --balance FLOAT --bet FLOAT --depth INT --seed INT`

## Команды: `python3 -m cross_arb_bot`

- `scan`
  - `--dry`
- `sim`
- `status`
- `resolve`
- `watch`
- `liquidity-report`
- `rebalance`
  - `--from {polymarket,kalshi}`
  - `--to {polymarket,kalshi}`
  - `--amount FLOAT`
  - `--note TEXT`
- `run`
  - `--interval INT`
- `live`

Ключевой конфиг: `cross_arb_bot/config.yaml`.

Ключевые секции:

- `trading`
- `market_filter`
- `runtime`
- `db`
- `polymarket`
- `kalshi`

База `cross_arb_bot`:

- `data/cross_arb_bot.db`
- таблица `positions`
- таблица `transfers`

Что важно про `cross_arb_bot`:

- проект сейчас работает только с `Polymarket` и `Kalshi`; поддержку `Myriad` нужно считать удалённой
- `matches` в статусе — это число логически сопоставленных пар рынков, а не число арбитражных окон
- `opportunities` — это уже только matches с подходящим ценовым окном
- `run/live` сейчас используют HTTP discovery по рынкам; websocket применяется в `watch` для live-feed `Polymarket`
- вход в сделку проверяется по реальному ask-стакану, а не только по snapshot-ценам
- в новых позициях сохраняются:
  - snapshots рынков на `open` и `resolve`
  - liquidity-метрики по обеим ногам: requested/fill/available/best/avg/remaining
- `rebalance` — это paper-ledger перевод между площадками, который меняет свободный баланс через таблицу `transfers`

Известные ограничения и риски `cross_arb_bot`:

- `Polymarket 15m Up/Down` и `Kalshi 15m up in next 15 mins?` не являются строгими арбитражными комплементами
- `Polymarket` использует `Chainlink` start-vs-end
- `Kalshi` использует `CF Benchmarks` 60-second average-vs-average
- поэтому возможны ложные матчи с `lock_valid = 0`, в том числе случаи:
  - обе ноги выиграли
  - обе ноги проиграли
- для hourly research есть отдельный документ:
  - [docs/hourly-above-arbitrage-research.md](/Users/sasha/Documents/code/polymarket/docs/hourly-above-arbitrage-research.md)
- для false-match/positive-EV reasoning есть отдельный документ:
  - [docs/false-match-positive-ev-research.md](/Users/sasha/Documents/code/polymarket/docs/false-match-positive-ev-research.md)

Сервисные скрипты без argparse-параметров:

- `python3 approve_usdc.py`
- `python3 check_account.py`
- `python3 check_balance.py`
- `python3 check_highx.py`
- `python3 check_key.py`
- `python3 check_onchain.py`
- `python3 check_order.py`
- `python3 filter_markets.py`
- `python3 test_order.py`

## Конфиги и переменные окружения

Основной бот:

- `BOT_CONFIG`
- `TELEGRAM_TOKEN`
- `WALLET_PRIVATE_KEY`
- `WALLET_PROXY`

Simple bot:

- `SIMPLE_BOT_TOKEN`

Ключевые секции `config.yaml`:

- `strategy`
- `martingale`
- `real_martingale`
- `paper_trading`
- `api`
- `db`
- `reports`
- `telegram`
- `wallet`

Ключевые секции `ev_bot/ev_config.yaml`:

- `ev_filter`
- `strategy`
- `martingale`
- `db`

Ключевые секции `simple_bot/config.yaml`:

- `strategy`
- `trading`
- `db`
- `telegram`

## Практические замечания

- В `pyproject.toml` сейчас не перечислены все библиотеки, которые реально используются скриптами real trading и диагностики.
- Если команда не стартует из-за отсутствующих модулей, сначала проверь, к какому подпроекту она относится, и не документируй это как баг без проверки кода.
- Для изменений в CLI безопаснее сначала обновить код, потом синхронно обновить `README.md` и `AGENTS.md`.
