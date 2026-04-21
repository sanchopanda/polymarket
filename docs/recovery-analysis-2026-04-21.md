# recovery_bot — повторные сигналы и новый touch-цикл (2026-04-21)

Документ фиксирует изменение семантики `signal` в `recovery_bot`, cleanup старых `signal`-проб в live DB и quick backtest по historical базе `data/backtest.db`.

## Что поменяли в логике

Раньше `signal`-события могли писаться повторно в рамках одного и того же `touch`: рынок касался `bottom_price`, потом доходил до `entry_price`, потом снова уходил вниз и ещё раз пересекал `entry_price`, а аналитика продолжала считать это повторным сигналом внутри старого цикла.

Теперь цикл такой:

1. ловим `touch` (`ask <= bottom_price`)
2. ждём `signal` (`ask >= entry_price` после `activation_delay_seconds`)
3. сразу после `signal` сбрасываем `touch_ts/touch_price/armed_ts`
4. следующий `signal` на той же стороне возможен только после нового собственного `touch`

Практический смысл:

- `repeat_same` теперь означает именно **новый повторный touch → новый signal** на той же стороне
- на первый сигнал рынка/конфига ордер по-прежнему ставим
- последующие сигналы логируются для аналитики, но **новый ордер не ставится**

Отдельно: при `overshoot` (`ask > top_price` после touch и после activation delay) цикл тоже сбрасывается. Иначе новый touch на этой стороне не начинался бы.

## Cleanup live DB

Перед запуском новой логики старые `signal`-пробы из `data/recovery_bot.db` были очищены:

- backup: `data/recovery_bot.db.bak-20260421-legacy-signals-cleanup`
- удалено legacy `price_probes.kind='signal'`: `31444`
- `order`-пробы оставлены как есть

Цель cleanup: не смешивать старую семантику (`repeat` внутри одного touch) с новой (`каждый signal имеет свой touch`).

## Backtest: что считали

Источник: `data/backtest.db`

Параметры из текущего `recovery_bot/config.yaml`:

- `5m`: `bottom=0.38 entry=0.65 top=0.68 delay=0`
- cutoff перед экспирацией: `30s`

Для каждого `market_id + side`:

1. строим последовательность сигналов по новой логике `touch -> signal -> reset`
2. `first` = первый сигнал на стороне
3. `second_same` = второй сигнал на той же стороне
4. `all_repeat_same` = все сигналы начиная со второго

## Результаты: BTC / ETH, только 5m

### BTC 5m

- `first`: `n=3708`, `WR=69.58%`
- `second_same`: `n=346`, `WR=63.87%`
- `all_repeat_same`: `n=459`, `WR=62.09%`

Средний `touch -> signal`:

- `first`: `53.41s`
- `second_same`: `18.34s`
- `all_repeat_same`: `14.55s`

### ETH 5m

- `first`: `n=4405`, `WR=65.47%`
- `second_same`: `n=558`, `WR=62.72%`
- `all_repeat_same`: `n=677`, `WR=62.92%`

Средний `touch -> signal`:

- `first`: `57.99s`
- `second_same`: `28.61s`
- `all_repeat_same`: `25.01s`

## BTC 5m: где ломается second_same

### По `touch -> signal` delay

- `0-9s`: `n=146`, `WR=55.48%`
- `10-19s`: `n=77`, `WR=70.13%`
- `20-29s`: `n=43`, `WR=62.79%`
- `30-59s`: `n=60`, `WR=70.00%`
- `60s+`: `n=20`, `WR=85.00%`

Главная слабая зона — сверхбыстрый повторный сигнал в первые `0-9s` после нового `touch`.

### По времени до экспирации в момент second_same

- `30-59s left`: `n=201`, `WR=61.69%`
- `60-89s left`: `n=83`, `WR=61.45%`
- `90-119s left`: `n=22`, `WR=77.27%`
- `120-149s left`: `n=29`, `WR=72.41%`
- `150-179s left`: `n=7`, `WR=85.71%`

Вывод: у `BTC 5m second_same` слабая зона — поздние сигналы, когда до экспирации осталось меньше `90s`.

## Сверка с live DB

Важно не путать разные выборки:

- `data/recovery_bot.db` отражает **real fills** с live-фильтрами
- backtest выше — это **signal-level** анализ по новой семантике repeat-cycle

Актуальные числа в live DB по resolved real:

- `BTC`, все стратегии: `352`, `WR=69.32%`
- `BTC`, только `5m_base`: `294`, `WR=71.09%`

Поэтому расхождение вида «в live BTC около 70%, а repeat_same в backtest около 62-64%» нормально:

- `~70%` относится к first/live fills
- `~62-64%` относится к повторным same-side сигналам со своим новым touch

## Практический вывод

Новая трактовка `repeat_same` более корректна для анализа:

- повторный сигнал теперь не паразитирует на старом touch
- `BTC 5m` показывает, что сами по себе repeat_same сигналы заметно слабее first signal
- особенно токсичны быстрые повторные циклы (`touch -> signal < 10s`) и поздние сигналы (`< 90s left`)

Следующий кандидат для проверки:

- фильтр для `repeat_same` вида `touch_to_signal >= 10s`
- и/или `seconds_left >= 90`
