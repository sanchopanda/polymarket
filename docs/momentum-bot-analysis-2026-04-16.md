# Momentum Bot — анализ повторных входов и методика

Дата среза: `2026-04-16`

Источник данных:
- база `data/momentum_bot.db`
- таблица `positions`

Важно:
- сейчас это **не финальные фильтры**, а рабочая аналитическая заметка
- цель: сначала накопить ещё данных, потом повторить тот же анализ и проверить, устойчивы ли выводы
- в основном статусе бота считаются только **первые ставки рынка** (`первая запись по pair_key`)
- все остальные позиции сохраняются именно для аналитики повторных входов

## Что анализировали

Есть два слоя:

1. **Боевой слой**
   - только первая ставка по каждому `pair_key`
   - нужен для честного WR/PnL по рынкам

2. **Аналитический слой**
   - все повторные ставки `rn > 1`
   - нужен, чтобы понять:
     - когда повторный вход улучшает PnL
     - когда повторный вход уже превращается в шум
     - какие параметры сигнала в момент входа полезны

Ключевая идея:
- смотреть не на сам факт `repeat`
- а на параметры входа:
  - `entry_price`
  - `gap_cents = (leader_price_at_entry - entry_price) * 100`
  - `spike_magnitude`
  - `minute_in_cycle = minute(opened_at) % 15`

## Текущий срез

### Все позиции

- всего позиций: `227`
- `resolved`: `227`
- суммарный `PnL all`: `+41.21`

### Только первые ставки рынка

- рынков: `31`
- wins: `24`
- `WR = 77.4%`
- `PnL first = +13.01`

### Только аналитические повторные входы

- повторных позиций: `196`
- wins: `142`
- `WR = 72.4%`
- `PnL repeat = +28.20`

## Главные выводы на текущей выборке

### 1. Для repeat-входов важнее не номер ставки, а параметры сигнала

Номер входа сам по себе не является целевым признаком.
Он нужен только как способ отделить первую ставку рынка от аналитических повторов.

Факторы, которые реально влияют на PnL:
- `gap_cents`
- `minute_in_cycle`
- `entry_price`
- `spike` vs `gap-only`

### 2. Самый сильный признак сейчас — `gap_cents`

По repeat-входам:

| Gap bucket | n | WR | PnL |
|---|---:|---:|---:|
| `<8c` | 45 | 97.8% | `+78.75` |
| `8-12c` | 37 | 78.4% | `+9.20` |
| `12-16c` | 57 | 61.4% | `-39.58` |
| `16c+` | 57 | 59.6% | `-20.17` |

Промежуточный вывод:
- **умеренный gap** выглядит намного лучше огромного
- большой gap часто похож не на edge, а на плохую структуру рынка

### 3. Второй по силе признак — время внутри 15m окна

По repeat-входам:

| Time bucket | n | WR | PnL |
|---|---:|---:|---:|
| `m9-10` | 146 | 63.0% | `-76.26` |
| `m11-12` | 33 | 100.0% | `+66.96` |
| `m13-14` | 17 | 100.0% | `+37.50` |

Промежуточный вывод:
- ранние повторы в `m9-10` пока выглядят слабо
- поздние повторы в `m11-14` выглядят очень сильно

### 4. `entry_price` сам по себе не лучший признак, но даёт полезный сигнал

По repeat-входам:

| Entry bucket | n | WR | avg PnL | total PnL |
|---|---:|---:|---:|---:|
| `<0.55` | 18 | 11.1% | `-4.01` | `-72.09` |
| `0.55-0.60` | 29 | 44.8% | `-1.15` | `-33.48` |
| `0.60-0.65` | 26 | 65.4% | `+0.17` | `+4.38` |
| `0.65-0.70` | 31 | 58.1% | `-0.70` | `-21.74` |
| `0.70-0.75` | 46 | 100.0% | `+1.89` | `+86.76` |
| `0.75-0.80` | 46 | 100.0% | `+1.40` | `+64.38` |

Интерпретация:
- теоретически низкий вход даёт больший payout при победе
- но на текущей выборке низкий `entry` слишком часто сопровождается плохим качеством сигнала
- поэтому низкий `entry` **не компенсирует** низкий WR

### 5. Важен не просто `entry_price`, а его сочетание с `gap` и временем

Совместный разбор repeat-входов:

#### Раннее окно `m9-10`

| Entry | Gap | n | WR | PnL |
|---|---|---:|---:|---:|
| `<0.60` | `12c+` | 41 | 22.0% | `-129.15` |
| `0.60-0.70` | `8-12c` | 9 | 11.1% | `-37.42` |
| `0.60-0.70` | `12c+` | 24 | 45.8% | `-36.04` |
| `0.60-0.70` | `<8c` | 16 | 93.8% | `+33.86` |
| `0.70-0.80` | `любой gap` | стабильно сильный плюс |  |  |

Главный вывод:
- **ранний вход сам по себе не плох**
- плох **ранний вход с широким gap**
- ранний вход с `gap < 8c` наоборот выглядит сильным

#### Поздние окна `m11-14`

На текущем срезе поздние входы почти везде положительны, даже при более широком gap.
Это нужно перепроверить на новой выборке, потому что сейчас тут выборка заметно меньше.

### 6. `spike` пока выглядит лучше, чем `gap-only`, но выборка маленькая

| Signal bucket | n | WR | PnL |
|---|---:|---:|---:|
| `gap_only` | 184 | 70.7% | `-0.13` |
| `spike10-20c` | 5 | 100.0% | `+12.02` |
| `spike20c+` | 7 | 100.0% | `+16.30` |

Пока это только гипотеза:
- spike-сигналы выглядят сильнее
- но выборка слишком мала, чтобы вводить фильтр

## Что НЕ надо делать по этому срезу

Пока не стоит:
- жёстко фильтровать по номеру входа
- фильтровать только по `entry_price`
- формировать production-правила по spike

Причина:
- часть хороших и плохих эффектов на самом деле может быть следствием связки `time + gap + symbol`
- нужна ещё одна выборка после прогона бота

## Что именно проверять на следующей выборке

После нового прогона повторить анализ и проверить, подтверждаются ли такие гипотезы:

1. `gap < 8c` действительно лучше, чем `gap >= 8c`
2. ранний repeat-вход в `m9-10` плох только при широком gap
3. поздние repeat-входы `m11-14` действительно устойчиво лучше
4. низкий `entry < 0.60` плох сам по себе или только как proxy широкого gap
5. `spike` действительно лучше `gap-only`
6. этот эффект устойчив по отдельным символам (`BTC`, `ETH`, `SOL`)

## Как повторять анализ

Ниже — базовые SQL-паттерны, которыми повторяли анализ.

### 1. Разделение на first vs repeat

```sql
WITH ranked AS (
  SELECT *,
         row_number() OVER (PARTITION BY pair_key ORDER BY opened_at, id) rn
  FROM positions
)
SELECT *
FROM ranked;
```

Смысл:
- `rn = 1` → первая ставка рынка
- `rn > 1` → аналитические повторные входы

### 2. Общий результат по first-ставкам

```sql
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY pair_key ORDER BY opened_at, id) rn
  FROM positions
)
SELECT
  count(*) as first_markets,
  sum(case when status='resolved' then 1 else 0 end) as resolved,
  sum(case when status='resolved' and pnl > 0 then 1 else 0 end) as wins,
  round(100.0 * sum(case when status='resolved' and pnl > 0 then 1 else 0 end)
        / nullif(sum(case when status='resolved' then 1 else 0 end),0), 1) as wr_pct,
  round(sum(case when status='resolved' then coalesce(pnl,0) else 0 end), 2) as pnl_first
FROM ranked
WHERE rn = 1;
```

### 3. Анализ repeat-входов по gap

```sql
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY pair_key ORDER BY opened_at, id) rn
  FROM positions
),
enriched AS (
  SELECT *,
         (leader_price_at_entry - entry_price) * 100.0 as gap_cents
  FROM ranked
  WHERE rn > 1
)
SELECT
  CASE
    WHEN gap_cents < 8 THEN '<8c'
    WHEN gap_cents < 12 THEN '8-12c'
    WHEN gap_cents < 16 THEN '12-16c'
    ELSE '16c+'
  END as gap_bucket,
  count(*) as n,
  round(100.0 * sum(case when pnl > 0 then 1 else 0 end) / count(*), 1) as wr_pct,
  round(sum(pnl), 2) as pnl
FROM enriched
GROUP BY gap_bucket;
```

### 4. Анализ repeat-входов по времени внутри 15m

```sql
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY pair_key ORDER BY opened_at, id) rn
  FROM positions
),
enriched AS (
  SELECT *,
         CAST(strftime('%M', opened_at) AS INTEGER) % 15 AS minute_in_cycle
  FROM ranked
  WHERE rn > 1
)
SELECT
  CASE
    WHEN minute_in_cycle IN (9,10) THEN 'm9-10'
    WHEN minute_in_cycle IN (11,12) THEN 'm11-12'
    ELSE 'm13-14'
  END as time_bucket,
  count(*) as n,
  round(100.0 * sum(case when pnl > 0 then 1 else 0 end) / count(*), 1) as wr_pct,
  round(sum(pnl), 2) as pnl
FROM enriched
GROUP BY time_bucket;
```

### 5. Анализ repeat-входов по entry bucket

```sql
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY pair_key ORDER BY opened_at, id) rn
  FROM positions
),
base AS (
  SELECT entry_price, pnl
  FROM ranked
  WHERE rn > 1
)
SELECT
  CASE
    WHEN entry_price < 0.55 THEN '<0.55'
    WHEN entry_price < 0.60 THEN '0.55-0.60'
    WHEN entry_price < 0.65 THEN '0.60-0.65'
    WHEN entry_price < 0.70 THEN '0.65-0.70'
    WHEN entry_price < 0.75 THEN '0.70-0.75'
    ELSE '0.75-0.80'
  END as entry_bucket,
  count(*) as n,
  round(100.0 * sum(case when pnl > 0 then 1 else 0 end) / count(*), 1) as wr_pct,
  round(avg(pnl), 2) as avg_pnl,
  round(sum(pnl), 2) as total_pnl,
  round(avg(case when pnl > 0 then pnl end), 2) as avg_win,
  round(avg(case when pnl <= 0 then pnl end), 2) as avg_loss
FROM base
GROUP BY entry_bucket;
```

### 6. Совместный анализ `entry + gap + time`

```sql
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY pair_key ORDER BY opened_at, id) rn
  FROM positions
),
base AS (
  SELECT
    entry_price,
    (leader_price_at_entry - entry_price) * 100.0 AS gap_cents,
    CAST(strftime('%M', opened_at) AS INTEGER) % 15 AS minute_in_cycle,
    pnl
  FROM ranked
  WHERE rn > 1
)
SELECT
  CASE
    WHEN entry_price < 0.60 THEN '<0.60'
    WHEN entry_price < 0.70 THEN '0.60-0.70'
    ELSE '0.70-0.80'
  END AS entry_bucket,
  CASE
    WHEN gap_cents < 8 THEN '<8c'
    WHEN gap_cents < 12 THEN '8-12c'
    ELSE '12c+'
  END AS gap_bucket,
  CASE
    WHEN minute_in_cycle IN (9,10) THEN 'm9-10'
    WHEN minute_in_cycle IN (11,12) THEN 'm11-12'
    ELSE 'm13-14'
  END AS time_bucket,
  count(*) AS n,
  round(100.0 * sum(case when pnl > 0 then 1 else 0 end) / count(*), 1) AS wr_pct,
  round(avg(pnl), 2) AS avg_pnl,
  round(sum(pnl), 2) AS total_pnl
FROM base
GROUP BY entry_bucket, gap_bucket, time_bucket
HAVING count(*) >= 3
ORDER BY entry_bucket, gap_bucket, time_bucket;
```

## Что делать дальше

План:

1. Дать `momentum_bot` ещё поработать на текущей логике
2. Не вводить фильтры заранее
3. После накопления новой выборки повторить:
   - `first vs repeat`
   - `gap`
   - `time_in_cycle`
   - `entry`
   - `entry + gap + time`
   - `symbol slices`
4. Только если картина повторится ещё раз — превращать выводы в боевые фильтры

