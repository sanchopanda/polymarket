# AGENTS.md

Этот файл — основная точка входа для кодовых агентов в этом репозитории. `CLAUDE.md` должен рассматриваться только как редирект сюда.

## Что это за репозиторий

Репозиторий содержит:

- `cross_arb_bot/` — кросс-маркет арбитражный бот (Polymarket ↔ Kalshi), **paper trading**;
- `real_arb_bot/` — реальный арбитражный бот (Polymarket ↔ Kalshi), **real trading**;
- `arb_bot/` — общий WebSocket клиент для Polymarket (зависимость `cross_arb_bot` и тестов);
- `test_real_15m.py` — тестовый скрипт реальных ставок на Polymarket (сбор аналитики);
- `test_real_kalshi.py` — тестовый скрипт реальных ставок на Kalshi (сбор аналитики);
- `check_balance.py` — проверка баланса кошелька;
- `docs/` — исследования по арбитражу.

Рабочий язык проекта: русский. Новые README, help-тексты, комментарии и агентские заметки лучше держать в том же стиле.

## Как работать с репозиторием

- Сначала смотри на фактические CLI в коде, а не на старую документацию.
- Если меняешь команды или аргументы CLI, обновляй как минимум `README.md` и этот файл.
- Многие скрипты требуют `.env` с `WALLET_PRIVATE_KEY`, `WALLET_PROXY`, `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`.

## Быстрые ориентиры по данным

- `data/cross_arb_bot.db` — база арбитражного бота (positions, transfers).
- `data/test_real_15m_results.json` — результаты тестовых ставок Polymarket.

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
  - [docs/hourly-above-arbitrage-research.md](docs/hourly-above-arbitrage-research.md)
- для false-match/positive-EV reasoning есть отдельный документ:
  - [docs/false-match-positive-ev-research.md](docs/false-match-positive-ev-research.md)

## Команды: `python3 -m real_arb_bot`

- `status` — реальные балансы с обеих бирж + позиции + дневной P&L
- `scan [--dry]` — найти арбитражные возможности, при `--dry` только показ без ордеров
- `resolve` — резолюция/redeem истёкших позиций
- `run [--interval INT]` — непрерывный цикл
- `audit [--last N]` — аудит-лог всех API вызовов и решений
- `orphans` — одноногие позиции, требующие ручного разбора

Конфиг: `real_arb_bot/config.yaml`
База: `data/real_arb_bot.db`

Важные параметры `safety`:
- `require_confirmation: true` — ручное подтверждение каждой сделки (рекомендуется на старте)
- `dry_run: false` — включить для режима без реальных ордеров
- `cooldown_seconds` — пауза между сделками

Переменные `.env` для `real_arb_bot`:
- `WALLET_PRIVATE_KEY` — Polymarket кошелёк
- `WALLET_PROXY` — прокси (опционально)
- `KALSHI_API_KEY_ID` + `KALSHI_PRIVATE_KEY_PATH` — Kalshi API

## Тестовые скрипты

- `python3 test_real_15m.py` — watcher: находит 15m крипто-рынок на Polymarket, ставит $2, собирает аналитику (WS цены, orderbook, fill, комиссии, resolution, redeem)
- `python3 test_real_kalshi.py` — аналогичный watcher для Kalshi
- `python3 check_balance.py` — проверка баланса Polymarket кошелька

## Конфиги и переменные окружения

Переменные `.env`:

- `WALLET_PRIVATE_KEY` — приватный ключ MetaMask (Polymarket)
- `WALLET_PROXY` — прокси для API
- `KALSHI_API_KEY_ID` — ID API ключа Kalshi
- `KALSHI_PRIVATE_KEY_PATH` — путь к RSA ключу Kalshi (напр. `data/kalshi.key`)

Ключевые секции `cross_arb_bot/config.yaml`:

- `trading`
- `market_filter`
- `runtime`
- `db`
- `polymarket`
- `kalshi`

## Практические замечания

- В `pyproject.toml` сейчас не перечислены все библиотеки, которые реально используются скриптами.
- После изменений в зависимостях проси пользователя повторно выполнить `pip install -e ".[dev]"` в активированном `venv`.
- Если команда не стартует из-за отсутствующих модулей, сначала проверь, к какому подпроекту она относится.
- Для изменений в CLI безопаснее сначала обновить код, потом синхронно обновить `README.md` и `AGENTS.md`.
