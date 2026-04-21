# recovery_bot

Стратегия «отскока» на коротких крипто-рынках Polymarket: ловим глубокое касание `bottom_price`, потом покупаем на восстановлении в `[entry_price, top_price]` и держим до экспирации.

## Идея

1. Подписываемся на `best_ask` YES/NO для всех активных 5m/15m рынков BTC/ETH/SOL/XRP/DOGE/BNB.
2. `touch`: фиксируем момент когда `ask ≤ bottom_price` (например, ≤ 0.38).
3. После `activation_delay_seconds` начинаем искать восстановление.
4. `armed`: первое `ask ∈ [entry_price, top_price]` после touch.
5. Ставим лимитный BUY по `top_price` (0.68) на соответствующую сторону.
6. Ждём экспирации → payoff по `winning_side`.

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

В момент `armed` и сразу после `order` стартует probe: 8 выборок `best_ask` на offsets [200, 500, 700, 1000, 1200, 1500, 1700, 2000] ms. Позволяет анализировать поведение цены на микроуровне (см. фильтр выше — его эвристика строилась по этой таблице).

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
