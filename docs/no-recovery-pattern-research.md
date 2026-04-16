# No Recovery Pattern Research

**Дата анализа:** 2026-04-16  
**Период данных:** 2026-03-15 — 2026-04-15  
**Рынки:** BTC, ETH, SOL, XRP — up/down, интервалы 5m / 15m / 60m

## Суть паттерна

Если сторона **no (Down)** падала до уровня ≤ X, а затем восстанавливалась до ≥ Y — win rate no значимо выше справедливого.

Стратегия: при касании no уровня ≤ 0.30 выставить лимитный ордер на покупку no по 0.70. Ордер заполняется автоматически если восстановление произойдёт.

## Базы данных

| База | Путь | Содержимое |
|------|------|-----------|
| Основная бэктест-база | `research_bot/data/backtest.db` | таблицы `markets`, `pm_trades`, `binance_1s` |

Таблица `markets` — закрытые рынки с `winning_side`.  
Таблица `pm_trades` — все сделки (outcome=`Up`/`Down`, ts, price).

## Команды сбора данных

### 5-мин и 15-мин рынки (основная команда)

```bash
python3 -m research_bot.fetch_backtest_data --days 7
# опции:
#   --symbols BTC ETH SOL XRP   (default: все)
#   --force                     перескачать имеющиеся
#   --skip-binance              без Binance 1s klines
#   --skip-trades               без PM трейдов
```

### 60-мин (hourly) рынки

```bash
python3 -m research_bot.fetch_hourly --days 7
# опции:
#   --symbols BTC ETH SOL XRP
#   --force
```

Hourly рынки не попадают в стандартный /markets листинг — скрипт фетчит их через `/events?slug=` по сгенерированным slug-ам.

## Что в базе на момент анализа

| Интервал | Рынков | Период |
|----------|--------|--------|
| 5m  | 35 203 | 2026-03-15 — 2026-04-15 |
| 15m | 11 760 | 2026-03-15 — 2026-04-15 |
| 60m |  2 176 | 2026-03-15 — 2026-04-07 |

## Матрица win rate: дно → восстановление

Ставка на no при первом восстановлении до уровня ≥ Y, после касания ≤ X.  
Win rate одинаков для всех трёх интервалов (5m / 15m / 60m).

```
Дно \ Рост  >= 0.45  >= 0.50  >= 0.55  >= 0.60  >= 0.65  >= 0.70
<= 0.10      43-49%   48-55%   53-61%   55-65%   62-69%   66-74%
<= 0.15      44-49%   49-56%   54-62%   58-66%   64-71%   68-75%
<= 0.20      47-50%   52-56%   56-61%   61-66%   66-71%   69-75%
<= 0.25      48-49%   53-55%   57-61%   61-66%   66-71%   71-75%
<= 0.30      48-49%   54%      59-60%   63-65%   68-70%   73-74%
<= 0.35      48-50%   53-55%   59-60%   64%      69-70%   74%
```

Диапазон — разброс между 5m/15m/60m (очень малый, паттерн стабилен).

## Ключевой результат: дно ≤ 0.30 → ордер на 0.70

| | 5m | 15m | 60m |
|--|----|----|-----|
| Рынков где no касалась ≤ 0.30 | 17 012 | 5 799 | 1 474 |
| Ордер заполнился (восстановление до ≥ 0.70) | 6 083 (36%) | 2 198 (38%) | 580 (39%) |
| Win rate при заполнении | **74%** | **74%** | **73%** |
| Edge (wr − price) | +0.04 | +0.04 | +0.03 |

Maker fee на лимитных ордерах PM = 0%, поэтому edge не съедается комиссиями.

## PnL симуляция: $1 на ордер, все рынки

Период: 24 дня, апрель 2026.

| | 5m | 15m | 60m | Итого |
|--|----|----|-----|-------|
| Ставок/день | 253 | 92 | 24 | ~369 |
| Вложено/день | ~$177 | ~$64 | ~$17 | ~$258 |
| PnL/день | **+$15.23** | **+$5.32** | **+$0.95** | **+$21.50** |
| ROI | ~8.6% | ~8.3% | ~5.6% | ~8.3% |
| Прибыльных дней | 23/24 (96%) | 17/24 (71%) | 16/24 (67%) | |

## Как воспроизвести анализ

Все расчёты — inline Python, данные из `research_bot/data/backtest.db`.

### Матрица win rate

```python
import sqlite3
from collections import defaultdict

DB = 'research_bot/data/backtest.db'
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

BOTTOMS    = [0.10, 0.15, 0.20, 0.25, 0.30, 0.35]
RECOVERIES = [0.45, 0.50, 0.55, 0.60, 0.65, 0.70]
INTERVAL   = 5  # или 15, 60

markets = conn.execute(
    'SELECT market_id, winning_side FROM markets WHERE interval_minutes=? AND winning_side IS NOT NULL',
    (INTERVAL,)
).fetchall()

grid = {(b, r): [0, 0] for b in BOTTOMS for r in RECOVERIES}

for mkt in markets:
    mid = mkt['market_id']
    winning_side = mkt['winning_side']

    trades = conn.execute(
        "SELECT ts, price FROM pm_trades WHERE market_id=? AND outcome='Down' ORDER BY ts",
        (mid,)
    ).fetchall()
    if not trades:
        continue

    prices = [(t['ts'], t['price']) for t in trades]

    for bottom in BOTTOMS:
        touch_ts = next((ts for ts, p in prices if p <= bottom), None)
        if touch_ts is None:
            continue
        after = [p for ts, p in prices if ts > touch_ts]
        for recovery in RECOVERIES:
            if recovery > bottom and any(p >= recovery for p in after):
                if winning_side == 'no':
                    grid[(bottom, recovery)][0] += 1
                else:
                    grid[(bottom, recovery)][1] += 1

for b in BOTTOMS:
    for r in RECOVERIES:
        if r <= b:
            continue
        w, l = grid[(b, r)]
        n = w + l
        if n:
            print(f'Дно<={b:.2f} Рост>={r:.2f}: {w/n:.0%} n={n}')
```

### PnL по дням

```python
import sqlite3
from collections import defaultdict

DB = 'research_bot/data/backtest.db'
STAKE = 1.0; BOTTOM = 0.30; ENTRY = 0.70
INTERVAL = 5  # или 15, 60

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

markets = conn.execute(
    'SELECT market_id, winning_side, market_end FROM markets WHERE interval_minutes=? AND winning_side IS NOT NULL',
    (INTERVAL,)
).fetchall()

by_day = defaultdict(list)

for mkt in markets:
    mid = mkt['market_id']
    winning_side = mkt['winning_side']
    day = mkt['market_end'][:10]

    trades = conn.execute(
        "SELECT ts, price FROM pm_trades WHERE market_id=? AND outcome='Down' ORDER BY ts",
        (mid,)
    ).fetchall()
    if not trades:
        continue

    touch_ts = next((t['ts'] for t in trades if t['price'] <= BOTTOM), None)
    if touch_ts is None:
        continue
    if not any(t['price'] >= ENTRY for t in trades if t['ts'] > touch_ts):
        continue

    pnl = STAKE / ENTRY - STAKE if winning_side == 'no' else -STAKE
    by_day[day].append(pnl)

total = sum(p for ps in by_day.values() for p in ps)
n_days = len(by_day)
print(f'PnL/день: ${total/n_days:+.2f}  |  всего ${total:+.2f} за {n_days} дней')
```

## Выводы

1. Паттерн стабильно воспроизводится на 5m / 15m / 60m рынках.
2. Лимитный ордер на 0.70 при касании 0.30 — самый простой способ реализации: не нужно мониторить восстановление в реальном времени.
3. При $1/ордер: ~$21.50/день прибыли при ~$258 вложенных (ROI ~8.3%).
4. Maker fee = 0% — edge сохраняется полностью.
5. Основной объём — 5-минутные рынки (96% прибыльных дней).
6. Главный инфраструктурный вопрос: мониторинг ~всех активных рынков и выставление ордеров при касании 0.30.
