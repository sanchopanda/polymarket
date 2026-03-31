# Oracle Arb Bot — Signal Strategies

## Strategy A: Simple Delta (current)

Сигнал срабатывает в последнюю минуту рынка, если текущая цена Binance
отличается от `pm_open_price` на нужную сторону и PM ещё не переоценил.

```python
delta_pct = (current_binance - pm_open_price) / pm_open_price * 100

if delta_pct > threshold and yes_ask < max_entry_price:  → YES
if delta_pct < -threshold and no_ask < max_entry_price:  → NO
```

**Плюсы:** просто, не требует данных предыдущей минуты.
**Минусы:** не отличает "цена давно выше" от "только что пересекла".

---

## Strategy B: Crossing Detection (experimental)

На предпоследней минуте (N-2) фиксируем сторону цены относительно `pm_open_price`.
Сигнал только если в последнюю минуту (N-1) цена перешла на **противоположную** сторону.

```python
# Минута N-2: захват референса
binance_ref_side = "above" if binance > pm_open_price else "below"

# Минута N-1: сигнал только при пересечении
crossed_up   = ref == "below" and delta_pct > threshold
crossed_down = ref == "above" and delta_pct < -threshold

if crossed_up   and yes_ask < max_entry_price:  → YES
if crossed_down and no_ask  < max_entry_price:  → NO
```

**Плюсы:** ловит именно момент движения, меньше ложных срабатываний.
**Минусы:** пропускает рынки если бот стартовал после предпоследней минуты;
 требует `pm_open_price` загруженным к N-2.

Реализация: `binance_ref_side` хранится в `OracleMarket.binance_ref_side`,
захватывается в `OracleArbBot._check_signal` при `market_minute == interval_minutes - 2`.
