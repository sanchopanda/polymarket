# AGENTS.md

Этот файл — основная точка входа для кодовых агентов в этом репозитории. `CLAUDE.md` должен рассматриваться только как редирект сюда.

## Что это за репозиторий

Репозиторий содержит:

- `cross_arb_bot/` — кросс-маркет арбитражный бот (Polymarket ↔ Kalshi), **paper trading**;
- `real_arb_bot/` — реальный арбитражный бот (Polymarket ↔ Kalshi), **real trading**;
- `momentum_bot/` — momentum-following бот (Polymarket ↔ Kalshi), **paper trading**;
- `real_momentum_bot/` — momentum-following бот с реальными ордерами и виртуальным бюджетом;
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
- Все данные ботов, бэктестов, research-скриптов, локальные SQLite-базы, кеши загрузки и выгрузки нужно хранить в папке `data/` (или в подпапках внутри `data/`).
- Если агент добавляет новый data-файл по умолчанию, путь должен указывать именно в `data/`, а не в корень репозитория или в произвольную рабочую папку.

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

## Диагностика: `matches=0` при наличии Kalshi-рынков

Симптом в статусе: `pm=N kalshi=M matches=0 opps=0` + `N kalshi unmatched`.

Причины (в порядке вероятности):

1. **Неправильный `interval_minutes` у PM-рынков.** Матчер в `matcher.py` требует
   `pm.interval_minutes == kalshi.interval_minutes`. Если PM-рынок получил `None` вместо `60`,
   совпадений не будет. Проверяй `_extract_interval_minutes` в `polymarket_feed.py` —
   регулярка должна покрывать актуальный формат заголовка.

   Заголовки почасовых PM-рынков содержат год:
   `"Bitcoin Up or Down - April 9, 2026 9AM ET"`.
   Если регулярка не учитывает год — `interval_minutes` будет `None`.

   Быстрая диагностика: добавить принт после `fetch_markets_by_slugs`:
   ```python
   for m in hourly_markets:
       print(f"[debug] q={m.question!r} liq={m.liquidity_num:.0f} end={m.end_date}")
   ```

2. **Окно экспирации.** `max_days_to_expiry` в `config.yaml` (0.083 ≈ 2 ч).
   Рынки вне окна `[now, now+2h]` отфильтровываются.

3. **Неправильные slugs.** Формат: `{symbol}-up-or-down-{month}-{day}-{year}-{hour}et`
   (например `solana-up-or-down-april-9-2026-9am-et`).
   При смене летнего/зимнего времени обновить `_ET_OFFSET` в `polymarket_feed.py`:
   EDT = `timedelta(hours=-4)` (апрель–октябрь), EST = `timedelta(hours=-5)` (ноябрь–март).

4. **Низкая ликвидность.** Фильтр `min_liquidity: 1000` в `config.yaml`.
   Новые рынки в начале окна могут не проходить.

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

Практика для `real_arb_bot watch`:
- universe refresh настраивается через `runtime.watch_universe_refresh_seconds`; частые price-update всё равно приходят через WS, так что discovery можно держать заметно реже
- порядок исполнения ног настраивается через `execution.first_leg`:
  - `kalshi` — текущий дефолт
  - `polymarket` — альтернативный режим для теста `Polymarket first`

Переменные `.env` для `real_arb_bot`:
- `WALLET_PRIVATE_KEY` — Polymarket кошелёк
- `WALLET_PROXY` — прокси (опционально)
- `KALSHI_API_KEY_ID` + `KALSHI_PRIVATE_KEY_PATH` — Kalshi API

## Команды: `python3 -m momentum_bot`

- `watch` — live monitoring with spike detection
- `status` — позиции и P&L
- `resolve` — резолюция истёкших позиций

Конфиг: `momentum_bot/config.yaml`
База: `data/momentum_bot.db`

Ключевые секции:

- `strategy`
- `market_filter`
- `runtime`
- `db`
- `polymarket`
- `kalshi`

## Команды: `python3 -m real_momentum_bot`

- `watch` — live monitoring + real order execution
- `status` — позиции и P&L
- `resolve` — резолюция истёкших позиций + retry redeem

Конфиг: `real_momentum_bot/config.yaml`
База: `data/real_momentum_bot.db`

Переменные `.env` для `real_momentum_bot`:
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

## sports_arb_bot — добавление нового спорта

**ОБЯЗАТЕЛЬНО:** при добавлении нового спорта/лиги всегда следуй флоу из `docs/adding_new_sport.md`. Никогда не прописывай новый спорт в код без прохождения всех шагов.

**Краткий обязательный флоу** (подробности в документации):

1. **API запрос**: получи сырые данные по URL матча, который прислал пользователь:
   ```bash
   curl "https://gamma-api.polymarket.com/markets?slug={slug}"
   curl "https://api.elections.kalshi.com/trade-api/v2/markets?event_ticker={event_ticker}&status=open"
   ```
   Запиши: `seriesSlug` (PM), `series_ticker` (Kalshi), `gameStartTime`, `expected_expiration_time`.

2. **Временно́е окно**: вычисли `delta = expected_expiration - gameStartTime`. Убедись, что стандартное окно бота покрывает матч (PM: ±5h от gameStart, Kalshi: +15m…+7h до expiry). Если нет — флаг для изменения feed фильтров.

3. **Имена команд**: сравни PM `outcomes` с Kalshi `yes_sub_title`. Проверь, что токены ≥4 символов из обоих списков пересекаются → TennisMatcher подходит.

4. **Dump скрипт**: создай `scripts/dump_{sport}_markets.py` (копия `dump_r6_markets.py`). Запусти с `--date <gameStartTime>` — окно `[date-2h, date+10h]`. Проверь что нужный матч виден в обоих title-файлах.

5. **Прописать в 4 файла**:
   - `feed_polymarket.py`: `SLUG_PREFIX_TO_SPORT` или `SERIES_SLUG_TO_SPORT`
   - `feed_kalshi.py`: `SERIES_TO_SPORT`
   - `watch_runner.py`: `KALSHI_SERIES_BY_SPORT`
   - `config.yaml`: добавить в `sports: []` с комментарием (PM seriesSlug + Kalshi series_ticker)

6. **Тест матчера**: вызови `TennisMatcher().match(pm_events, ka_events)` с данными из dump. Убедись что `outcome_map` правильный.

Добавление без dump скрипта и проверки матчера — **недопустимо**.

## sports_arb_bot — команды

```bash
python3 -m sports_arb_bot watch    # запуск бота
python3 -m sports_arb_bot status   # позиции и баланс
python3 -m sports_arb_bot resolve  # резолюция истёкших позиций
```

Конфиг: `sports_arb_bot/config.yaml`
База: `data/sports_arb_bot.db`

Dump скрипты: `scripts/dump_r6_markets.py`, `scripts/dump_cwbb_markets.py`

## Практические замечания

- В `pyproject.toml` сейчас не перечислены все библиотеки, которые реально используются скриптами.
- После изменений в зависимостях проси пользователя повторно выполнить `pip install -e ".[dev]"` в активированном `venv`.
- Если команда не стартует из-за отсутствующих модулей, сначала проверь, к какому подпроекту она относится.
- Для изменений в CLI безопаснее сначала обновить код, потом синхронно обновить `README.md` и `AGENTS.md`.
