# Polymarket Repo

Монорепозиторий с несколькими ботами и исследовательскими скриптами:

- `src/` — основной Polymarket Martingale bot: paper trading, real trading, загрузка исторических данных и backtest.
- `simple_bot/` — упрощённый paper bot без Мартингейла.
- `ev_bot/` — отдельный paper bot с EV-фильтром по историческим бакетам.
- `momentum_bot/` — paper momentum bot по паре `Polymarket ↔ Kalshi`.
- `real_momentum_bot/` — momentum bot с реальными ордерами и виртуальным бюджетом.
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
python3.10 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -e ".[dev]"
```

`pip install -e ".[dev]"` теперь ставит и runtime-зависимости для `arb_bot`, `cross_arb_bot` и `real_arb_bot`, включая `cryptography`, `web3`, `eth-account`, `py-clob-client` и Telegram-клиент `pyTelegramBotAPI`.

Важно:

- используй Python `>= 3.9.10`; на `3.9.0-3.9.9` `py-clob-client` не установится и `real_arb_bot`/`cross_arb_bot` не стартуют
- после активации `venv` запускай ботов через `python -m ...`
- если `python3.10` у тебя не установлен, сначала проверь доступные версии: `python3.10 --version`, `python3.11 --version`
- если до этого venv уже был создан, после обновления репозитория повторно выполни `pip install -e ".[dev]"`, иначе новые зависимости не подтянутся

Для `real_arb_bot` порядок исполнения ног можно переключать через `real_arb_bot/config.yaml`:

```yaml
execution:
  first_leg: "kalshi"      # дефолт
  # first_leg: "polymarket"  # альтернативный режим для тестов
```

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
python -m src.main scan --dry
python -m src.main run --interval 0.05
python -m src.main fetch --limit 5000 --workers 30
python -m src.main backtest --cache data/backtest_markets.json --balance 200 --depth 6
python -m src.main real scan --dry
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
python -m simple_bot <command>
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

## `arb_bot`

Изолированный paper-trading бот под парный арбитраж комплементарных исходов одного рынка.
Он ищет активные short-term crypto markets, считает покупку одинакового числа акций по обеим ногам
через CLOB ask-стакан и открывает виртуальную позицию, только если:

- `gross_cost + fees < settlement payout`
- хватает ликвидности по обеим ногам
- хватает свободного paper-баланса

Запуск:

```bash
python -m arb_bot scan --dry
python -m arb_bot scan
python -m arb_bot resolve
python -m arb_bot status
python -m arb_bot run --interval 1
python -m arb_bot ws
```

Конфиг: `arb_bot/config.yaml`.

Ключевые параметры:

- `strategy.market_query` — подстрока в вопросе рынка; пусто = брать все подходящие рынки
- `strategy.category` — категория рынка; пусто = без фильтра
- `strategy.fee_type` — тип fee, по умолчанию `crypto_fees`
- `strategy.min_edge` — минимальный ожидаемый профит на одну pair-position
- `strategy.max_payout_per_trade` — максимальное число парных акций на сделку
- `strategy.max_open_positions` — лимит одновременно открытых pair-position
- `trading.starting_balance` — виртуальный депозит, по умолчанию `$200`
- `trading.taker_fee` — fee-модель для paper расчёта
- `db.path` — отдельная SQLite БД, по умолчанию `data/arb_bot.db`

Режимы работы:

- `scan` — одноразовый HTTP-скан через Gamma + CLOB REST
- `run` — polling-цикл `resolve + scan`
- `ws` — live paper-simulation: bootstrap universe через Gamma, подписка на Polymarket Market WebSocket, поиск арбитражных окон по live ask, виртуальные входы, периодический статус и отслеживание виртуального баланса

## `cross_arb_bot`

Изолированный live scanner для межплатформенного paper arbitrage между `Polymarket` и `Kalshi`.
Первая фаза не ставит реальные ордера: бот нормализует short-term crypto markets, матчает эквивалентные рынки,
ищет lock-арбитраж вида `YES на одной площадке + NO на другой < $1`, открывает виртуальные pair-позиции
и ведёт отдельный paper-учёт по двум площадкам.

Запуск:

```bash
python -m cross_arb_bot scan --dry
python -m cross_arb_bot scan
python -m cross_arb_bot sim
python -m cross_arb_bot watch
python -m cross_arb_bot status
python -m cross_arb_bot resolve
python -m cross_arb_bot liquidity-report
python -m cross_arb_bot rebalance --from polymarket --to kalshi --amount 50
python -m cross_arb_bot live
python -m cross_arb_bot run --interval 20
```

Конфиг: `cross_arb_bot/config.yaml`.

Текущие ключевые параметры `cross_arb_bot/config.yaml`:

- `trading.starting_balance_polymarket`
- `trading.starting_balance_kalshi`
- `trading.stake_per_pair_usd`
- `trading.min_lock_edge`
- `trading.max_lock_edge`
- `trading.max_open_pairs`
- `trading.max_entries_per_pair`
- `trading.rebalance_threshold_usd`
- `market_filter.symbol`
- `market_filter.fee_type`
- `market_filter.min_days_to_expiry`
- `market_filter.max_days_to_expiry`
- `market_filter.min_volume`
- `market_filter.min_liquidity`
- `market_filter.expiry_tolerance_seconds`
- `runtime.poll_interval_seconds`
- `runtime.recheck_delay_seconds`
- `runtime.watch_universe_refresh_seconds` — как часто `watch` пересканирует universe рынков; live-цены между ресканами продолжают идти по WS

Research notes:

- hourly strike-based `Polymarket <-> Kalshi` research: [docs/hourly-above-arbitrage-research.md](/Users/sasha/Documents/code/polymarket/docs/hourly-above-arbitrage-research.md)
- false-match / positive-EV research for `Polymarket 15m <-> Kalshi 15m`: [docs/false-match-positive-ev-research.md](/Users/sasha/Documents/code/polymarket/docs/false-match-positive-ev-research.md)

Важно:

- `Kalshi` API может быть геоблокирован в зависимости от страны; в этом случае бот продолжит работать, но будет писать ошибку `kalshi fetch failed`
- сейчас бот работает только с `Polymarket + Kalshi`; код и CLI, связанные с `Myriad`, убраны
- в первой фазе matcher сфокусирован на short-term crypto `Up or Down` рынках
- `matches` — это только количество пар рынков, которые совпали по `symbol`, `market_kind`, `rule_family`, `interval_minutes` и допуску по `expiry`; `matches` не означает наличие арбитража
- `watch` — гибридный режим: universe discovery по HTTP, live `Polymarket` feed через WebSocket, финальная проверка входа по executable orderbook prices
- `run`/`live` — polling-цикл `resolve + simulate_execution_cycle`; discovery по рынкам сейчас HTTP-based, а не полностью websocket-native
- резолюция в paper-режиме идёт по фактическим исходам каждой площадки; если это ложное совпадение рынков, возможен убыток вплоть до полной стоимости входа
- текущие `15m` рынки `Polymarket` и `Kalshi` не являются настоящими lock-arb эквивалентами:
  - `Polymarket`: start vs end по `Chainlink`
  - `Kalshi`: 60-second average vs 60-second average по `CF Benchmarks`
- из-за этого возможны ложные матчи двух типов:
  - обе ноги выигрывают
  - обе ноги проигрывают
- `rebalance` делает виртуальный перевод между paper-балансами и сохраняет его в отдельной ledger-таблице `transfers`
- на входе бот проверяет исполнимость через реальный ask-стакан обеих ног; если нужный объём не помещается в стакан целиком, сделка не открывается
- в live-логах бот теперь печатает человекочитаемую сводку по стакану:
  - сколько акций нужно
  - сколько есть в стакане
  - сколько реально fill-ится
  - best ask, avg fill, slippage
  - сколько осталось бы в стакане после нашего fill
- в БД по новым позициям сохраняются snapshots рынка на `open` и `resolve`, а также числовые метрики ликвидности по обеим ногам

Что лежит в `data/cross_arb_bot.db`:

- `positions`
  - paper-позиции, их исходы, `lock_valid`, snapshots на `open/resolve`
  - сохранённые liquidity-поля по обеим ногам: requested, filled, available, best ask, avg fill, remaining after fill
- `transfers`
  - история paper-ребалансировок между площадками

Команды `cross_arb_bot` по смыслу:

- `scan --dry`
  - один HTTP-срез рынков и кандидатов без открытия позиций
- `scan`
  - один HTTP-срез с попыткой открыть paper-позиции
- `sim`
  - двухшаговая симуляция исполнения: срез `t0`, recheck `t1`, оценка исчезновения edge и реального fill по стакану
- `watch`
  - HTTP discovery + `Polymarket` market websocket + финальная проверка исполнимости по стакану
- `status`
  - текущие свободные балансы, locked funds, realized P&L, transfers, last snapshot
- `resolve`
  - резолюция истёкших paper-позиций по фактическим исходам площадок
- `liquidity-report`
  - аналитика по сохранённой глубине стакана новых сделок
- `rebalance`
  - виртуальный перевод между paper-балансами
- `run` / `live`
  - непрерывный цикл со статусом и live-решениями

## `oracle_arb_bot`

Oracle-based momentum bot для short-term crypto markets на Polymarket.
Отслеживает цены через Binance aggTrade WebSocket, ищет 5-секундные импульсы
и ставит в направлении движения на 5m/15m Up or Down рынки.

Запуск:

```bash
python -m oracle_arb_bot
```

Конфиг: `oracle_arb_bot/config.yaml`.
База: `data/oracle_arb_bot.db`.

### Стратегия: Binance Momentum

1. Binance aggTrade WebSocket → агрегация в 5-секундные бакеты
2. Сравнение close[N] vs close[N-1] → delta_pct
3. Если |delta_pct| >= порога → сигнал YES (рост) или NO (падение)
4. Ставка на Polymarket в направлении сигнала

### Адаптивный порог дельты

Порог автоматически повышается при частых сигналах (choppy market):

| Сигналов за 10 мин (n) | Порог дельты | Режим рынка |
|---|---|---|
| n ≤ 2 | 0.05% | Спокойный |
| n ≤ 5 | 0.08% | Умеренный |
| n > 5 | 0.12% | Choppy |

Считаются сигналы ≥ 0.05% за скользящее окно (по умолчанию 600 секунд) по каждому символу отдельно.

Результаты бэктеста:
- Без адаптива: WR 70%, на choppy данных 50.2%
- С адаптивом: WR 74.2%, на choppy данных 60.6%

### Real Trading

Депозит с trailing floor:

```
delta = max(initial_deposit, peak * floor_pct)
floor = max(0, peak - delta)
```

При депозите $6 и пике $6: floor = $0 (можно потерять всё).
При пике $100: delta = max(6, 20) = $20, floor = $80.

Ордера: FOK market order через CLOB API, $1/ставка.

### Ключевые параметры конфига

```yaml
strategy:
  mode: "binance_momentum"
  momentum_delta_pct: 0.05        # базовый порог
  momentum_min_minute: 1          # не ставить на 1-й минуте рынка
  momentum_adaptive: true         # адаптивный порог
  momentum_adaptive_window: 600   # окно подсчёта (сек)
  momentum_adaptive_rules:        # [max_n, threshold]
    - [2, 0.05]
    - [5, 0.08]
    - [999, 0.12]
  min_orderbook_usd: 5.0          # мин. ликвидность в стакане
  depth_slippage_cents: 4.0       # глубина стакана до best_ask + Nc

real_trading:
  enabled: true
  stake_usd: 1.0
  initial_deposit: 6.0
  floor_pct: 0.20
  max_price: 0.49

trading:
  stake_usd: 5.0                  # paper bet size
  max_price: 0.48
```

### Research / Backtest

```bash
# Загрузить кэш рынков
python3 -m research_bot.fetch_markets --limit 5000

# Бэктест Binance momentum
python3 -m research_bot.backtest_binance_momentum
python3 -m research_bot.backtest_binance_momentum --adaptive
python3 -m research_bot.backtest_binance_momentum --adaptive --adaptive-rules "2:0.05,5:0.08,999:0.12"
python3 -m research_bot.backtest_binance_momentum --delta 0.10
python3 -m research_bot.backtest_binance_momentum --symbols BTC,ETH --intervals 5
```

## `momentum_bot`

Paper momentum bot для short-term crypto markets между `Polymarket` и `Kalshi`.
Бот отслеживает лидирующую площадку, ищет spike/gap сигналы и открывает виртуальную позицию
на follower-ноге, если цена ещё не ушла слишком далеко и разрыв не выглядит структурным.

Запуск:

```bash
python -m momentum_bot watch
python -m momentum_bot status
python -m momentum_bot resolve
python -m momentum_bot --config momentum_bot/config.yaml resolve
```

Команды:

- `watch` — live monitoring with spike detection
- `status` — показать позиции и P&L
- `resolve` — зарезолвить истёкшие позиции

Конфиг: `momentum_bot/config.yaml`.
База: `data/momentum_bot.db`.

Ключевые параметры:

- `strategy.spike_threshold_cents`
- `strategy.max_spike_cents`
- `strategy.max_entry_price`
- `strategy.spike_window_seconds`
- `strategy.min_leader_price`
- `strategy.max_price_gap_cents`
- `strategy.gap_signal_min_cents`
- `strategy.cooldown_seconds`
- `strategy.stake_per_trade_usd`
- `strategy.max_open_positions`
- `strategy.starting_balance`
- `market_filter.symbol`
- `market_filter.fee_type`
- `market_filter.max_days_to_expiry`
- `market_filter.min_volume`
- `market_filter.min_liquidity`
- `runtime.universe_refresh_seconds`
- `runtime.status_interval_seconds`

## `real_momentum_bot`

Momentum bot с реальными ордерами и виртуальным risk-budget. Входы происходят в live режиме,
при этом бот может остановиться по stop-loss логике относительно накопленного P&L и свободного бюджета.

Запуск:

```bash
python -m real_momentum_bot watch
python -m real_momentum_bot status
python -m real_momentum_bot resolve
python -m real_momentum_bot --config real_momentum_bot/config.yaml resolve
```

Команды:

- `watch` — live monitoring + real order execution
- `status` — показать позиции и P&L
- `resolve` — зарезолвить истёкшие позиции и повторить redeem

Конфиг: `real_momentum_bot/config.yaml`.
База: `data/real_momentum_bot.db`.

Ключевые параметры:

- `strategy.spike_threshold_cents`
- `strategy.spike_signals_enabled`
- `strategy.disable_pm_to_kalshi`
- `strategy.max_spike_cents`
- `strategy.max_entry_price`
- `strategy.min_leader_price`
- `strategy.max_leader_price`
- `strategy.max_price_gap_cents`
- `strategy.max_price_gap_cents_pm_to_kalshi`
- `strategy.gap_signal_min_cents`
- `strategy.gap_rising_lookback_seconds`
- `strategy.spike_window_seconds`
- `strategy.cooldown_seconds`
- `strategy.trades_per_budget`
- `strategy.max_open_positions`
- `budget.total_usd`
- `budget.min_floor_usd`
- `market_filter.symbol`
- `market_filter.fee_type`
- `market_filter.max_days_to_expiry`
- `market_filter.min_volume`
- `market_filter.min_liquidity`
- `runtime.universe_refresh_seconds`
- `runtime.status_interval_seconds`

Требует `.env` с:

- `WALLET_PRIVATE_KEY`
- `WALLET_PROXY`
- `KALSHI_API_KEY_ID`
- `KALSHI_PRIVATE_KEY_PATH`

## `ev_bot`

Запуск:

```bash
python -m ev_bot [--config ev_bot/ev_config.yaml] [--main-config config.yaml] <command>
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
