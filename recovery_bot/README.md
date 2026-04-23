# recovery_bot

Стратегия «отскока» на коротких крипто-рынках Polymarket: ловим глубокое касание `bottom_price`, потом покупаем на восстановлении в `[entry_price, top_price]` и держим до экспирации.

## Идея

1. Подписываемся на `best_ask` YES/NO для всех активных 5m/15m рынков BTC/ETH/SOL/XRP/DOGE/BNB.
2. `touch`: фиксируем момент когда `ask ≤ bottom_price` (например, ≤ 0.38).
3. После `activation_delay_seconds` начинаем искать восстановление.
4. `signal`: первое пересечение `entry_price` после конкретного `touch`.
5. После `signal` текущий `touch`-цикл сбрасывается; следующий сигнал возможен только после нового `touch`.
6. На первый сигнал рынка/конфига ставим лимитный BUY по `top_price` (0.68) на соответствующую сторону.
7. Повторные сигналы по новым `touch` пишутся в аналитику, но новый ордер на них не ставится.
8. Ждём экспирации → payoff по `winning_side`.

## Как запустить

```bash
python3 -m recovery_bot run
python3 -m recovery_bot resolve   # добить истёкшие в фоне
```

## Конфиг (`recovery_bot/config.yaml`)

### Основной real

- `paper_enabled: false` — paper выключен (включая все `paper_extras`)
- `real_enabled: true`
- `real_stake_usd: 1.0` — floor ставки
- `stake_scale_n: 20` — ставка = `balance / 20` (sim: медиана M2 ~$10k vs $7.7k у N=30, cap reached ~29d vs 41d, bust-rate тот же 14%)
- `five_minute.{bottom=0.38, entry=0.65, top=0.68}`
- `fifteen_minute.{bottom=0.38, entry=0.65, top=0.68, activation_delay=30s}`
- `real_disabled_intervals: [15]` — 15m отключён в real (убыточен при этом entry/top)
- `signal_source: "trade"` — источник сигнала для `touch/armed/confirm`:
  - `best_ask` — старая логика
  - `trade` — полностью trade-based signal pipeline
- `repeat_bet.enabled: false` — repeat/opposite repeat-беты в рабочем режиме отключены; бот ставит только first-signal

Важно:

- `signal_source` переключает именно signal-логику (`touch`, `overshoot`, `armed`, `confirm`)
- исполнение ордера и контроль working-ордера по-прежнему идут через стакан / `best_ask`

### Фильтр фейк-сигналов (post-armed, pre-order)

Активирован после того как замечали «ложные» армы — цена на 1с касается 0.65, потом обратно уходит в 0.40.

- `real_confirm_delay_seconds: 1.0` — default sleep после armed
- `real_confirm_delay_seconds_by_symbol` — **per-symbol delay**: `SOL: 2.0, DOGE: 2.0, BNB: 2.0`. У альтов цена через 2с НЕ улетает выше 0.68 (только 0-5% fills с ask@2s >0.68 — slow fill), зато 24-39% падают ниже 0.65 — точно токсичные дипы. Сим: baseline −$11.54 → +$7.47 по альтам.
- `real_confirm_min_price: 0.63` — если `ask < min` после delay → **skip fake_drop**
- `real_confirm_min_price_by_symbol` — per-symbol override: `DOGE: 0.65, BNB: 0.65` (в зоне [0.63, 0.65] у них WR 25-47% — toxic). SOL там же имеет WR=73% → остаётся default 0.63. XRP использует `real_hold_test_by_symbol` вместо этого (жёстче).
- `real_hold_test_by_symbol` — **оконный** фильтр вместо точечной проверки. Сейчас для `XRP`: окно 2000ms, семпл каждые 200ms, `min(samples) ≥ 0.65`. Причина: у XRP bucket «min в окне 0.60-0.63» имеет WR=30% — точечная проверка на 1с пропускает дипы на 1.5с/1.7с, hold-test ловит их. В коде hold-test имеет приоритет над `real_confirm_min_price` для этого символа.
- `real_ran_away_price` — **удалён 2026-04-19.** Симуляция на 504 сделках показала: лимит 0.68 сам обрабатывает «улетевшие» сигналы (ордер висит пока цена не вернётся ≤0.68 или пока не отменится по `real_order_cancel_after_seconds`). Убирание фильтра: fills 302→320, WR 69.5%→70.6%, PnL +$11.77→+$16.83. Повышение `top_price` выше 0.68 делало хуже (рос средний entry, break-even WR уходил за 71%).
- `real_order_cancel_after_seconds: 15` — если лимит-ордер не заполнился за 15с после постановки, отменяем. Защищает от fills в момент когда цена наконец спустилась до 0.68, но уже после того как сигнал «устарел» (цена ушла в проигрыш).
- `real_order_cancel_if_ask_below: 0.60` — price-floor: если пока ордер висит `best_ask` падает ниже порога, отменяем. Закрывает дыру между разовой проверкой на 1с и 15-секундным таймаутом (видели кейс entry=0.52 на SOL).
- `max_seconds_to_expiry: 240` (в `five_minute`/`fifteen_minute`) — не арм-им сигналы если до экспирации >240с. Анализ показал: у XRP bucket [240,300]с имеет WR=29% (−$3.87), у SOL [180,240]с WR=58%. Баланс по всей выборке: cut n=49 WR=63% $−2.06, kept n=252 WR=69% $+7.94.

Пропущенные сигналы пишутся в `positions` со `status='skipped_filter'`. Логика `has_market_record` игнорирует их, чтобы второй сигнал на тот же рынок/сторону мог сработать.

В `engine.resolve()` для `skipped_filter` считается гипотетический PnL (чтоб видеть сколько фильтр спас / упустил), через `resolve_skipped_position()` (статус сохраняется).

### Market filter

```yaml
symbols: ["BTC","ETH","SOL","XRP","DOGE","BNB"]
interval_minutes: [5,15]
fee_type: "crypto_fees"
max_days_to_expiry: 0.02
min_volume: 0
min_liquidity: 0
```

## Таблица `positions`

Колонки, которые отличаются от дефолта / важны для аналитики:

| Колонка              | Что           |
|----------------------|---------------|
| `status`             | `open`, `working`, `resolved`, `error`, `skipped_filter` |
| `pnl`                | реализованный PnL (для `skipped_filter` — гипотетический) |
| `signal_volume`      | `volumeNum` из Gamma на момент сигнала (фоновый fetch) |
| `signal_liquidity`   | `liquidityNum` (на закрытых рынках обычно 0) |
| `signal_asks`        | JSON ask-ladder из CLOB `/book`, только levels с `price ≤ top_price`, формат `[[price,size],...]` |

### Асинхронный сбор meta/depth

После размещения ордера (в самом конце `_place_orders`) стартуют **два daemon-thread**:

1. `_fetch_market_meta_async(market_id)` — GET `{gamma}/markets/{id}`, сохраняет `volumeNum`+`liquidityNum`. Дедуп через `self._meta_fetched: set[str]`.
2. `_fetch_depth_async(market_id, token_id, side, top_price)` — CLOB `/book`, фильтрует asks ≤ `top_price`, сохраняет как JSON. Дедуп через `self._depth_fetched: set[(market_id, side)]`.

Ни один не блокирует hot path — ордер ставится первым.

### Price probes (`price_probes`)

В момент `signal` и сразу после `order` стартует probe: 8 выборок `best_ask` на offsets [200, 500, 700, 1000, 1200, 1500, 1700, 2000] ms. Один `signal` соответствует одному `touch`-циклу; после сигнала цикл сбрасывается и для следующего сигнала нужен новый `touch`.

### Live trade logging (`market_trade_history`)

Чтобы сравнить live-логику с историческим backtest по `pm_trades`, в live добавлено
параллельное логирование `last_trade_price` из PM WebSocket.

- торговая логика **не изменилась**: `touch/signal` по-прежнему считаются по `best_ask`
- `last_trade_price` сейчас пишется только для аналитики
- таблица: `market_trade_history(market_id, symbol, side, ts, price)`
- для экономии объёма пишем только последние `300s` жизни рынка

Зачем это нужно:

- backtest сейчас считается по историческим трейдам (`pm_trades`)
- live bot сейчас принимает решение по `best_ask`
- без параллельного лога нельзя честно ответить, откуда берётся расхождение:
  - из-за book-dynamics (`best_ask` двигается без сделок)
  - или из-за самих prints (`last_trade_price`)

Практическое правило:

- если хотим сохранить текущую стратегию, `best_ask` остаётся основным сигналом
- если хотим приблизить live к backtest, сначала собираем `market_trade_history`,
  а уже потом сравниваем signal-level статистику `best_ask vs trades`

Что теперь сохраняется:

- в `market_trade_history` для новых live prints пишутся `price` и `size`
- в `backtest.db.pm_trades` `size` сохраняется для новых догрузок historical trades

Отдельный backfill historical `size` по уже существующим рынкам:

```bash
PYTHONPATH=. venv/bin/python scripts/backfill_pm_trade_sizes.py --symbol BTC --interval 5 --only-missing-size
```

## Телеграм-статус

Рендерит engine.get_status_text:
- блоки PAPER / REAL (по всем mode)
- CLOB balance
- real-extras (только активные, т.е. определённые в `_real_only_names` — исторические типа `real_v2` скрыты)
- paper-extras блок (пустой если `paper_extras` не определён)
- счётчики рынков и touched/armed

`real_stats_exclude` применяется к REAL main, чтобы исключить исторические имена стратегий из агрегата.

## Известные особенности

- `entry_price` в DB ≠ `cfg.entry_price`: при удачном fill пишем `cfg.top_price` (лимит). После `_maybe_patch_real_from_paper` можем скорректировать из paper-матчинга по стакану.
- `real_v2` исторически существовал, сейчас не определён в yaml → не запускается. Старые 22 resolved строки остаются в БД.

## План анализа после накопления данных

После того как накопится несколько полных дней `market_trade_history`, делать анализ в таком порядке:

1. Сверка покрытия.
   Для каждого `market_id + side` сравнить:
   - сколько `best_ask` апдейтов попало в `market_price_history`
   - сколько trade prints попало в `market_trade_history`
   - где live-рынки были неполными из-за рестартов / дыр WS

2. Signal parity: `best_ask` vs `trade`.
   На одном и том же live-окне посчитать для `BTC 5m`:
   - `touch<=0.38 -> signal>=0.65` по `best_ask`
   - тот же цикл по `last_trade_price`
   - overlap / only-book / only-trade сигналы

3. WR по первым сигналам.
   Для первых сигналов на рынок сравнить:
   - WR у `best_ask`-сигналов
   - WR у `trade`-сигналов
   - отдельно по `YES/NO`
   - отдельно по хорошему и плохому live-периодам

4. Delay analysis.
   Проверить, что происходит между:
   - моментом `best_ask`-signal
   - первым `trade >= 0.65`
   - первым `trade <= 0.68` после signal

   Это ответит на вопрос, не даёт ли book «ранний» сигнал, который prints подтверждают слишком поздно или вообще не подтверждают.

5. Entry drift.
   Для сигналов, которые реально дошли до сделки, сравнить:
   - сигнал по `best_ask`
   - ближайшие `last_trade_price`
   - фактический live entry

   Здесь станет видно, где расхождение создаёт именно исполнение, а где уже сам сигнал.

6. Решение по следующему шагу.
   После накопления данных выбирать одно из трёх:
   - оставить signal только по `best_ask`
   - перейти на signal по `last_trade_price`
   - сделать гибрид: `touch` по `best_ask`, а `signal` подтверждать trade-print'ом

## Повторяемый анализ live vs backtest

Для регулярного сравнения:

- `real live WR`
- `live trade-based WR`
- `backtest trade-based WR`

на одном и том же наборе `BTC 5m` рынков добавлен отдельный скрипт:

```bash
venv/bin/python scripts/recovery_live_vs_backtest_analysis.py
```

Что он делает:

1. Находит в `data/recovery_bot.db` рынки с полной `market_trade_history`
2. При необходимости догружает недостающие рынки и `pm_trades` в `data/backtest.db`
3. Считает на одной и той же выборке рынков:
- `live trade-based WR`
- `backtest trade-based WR`
- `real live WR`
4. Печатает breakdown:
- `same_signal`
- `different_side`
- `live_only`
- `backtest_only`

Сейчас скрипт считает по всем `BTC 5m` рынкам, где вообще есть `market_trade_history`, без strict-отсечения по "полностью записанному" рынку.

Полное описание pipeline и критериев eligible-рынка:

- [docs/recovery-live-vs-backtest-repeatable-analysis.md](/Users/sasha/Documents/code/polymarket/docs/recovery-live-vs-backtest-repeatable-analysis.md)

Полезные флаги:

```bash
venv/bin/python scripts/recovery_live_vs_backtest_analysis.py --no-sync-backtest
venv/bin/python scripts/recovery_live_vs_backtest_analysis.py --start-buffer-seconds 60 --end-buffer-seconds 10
```
