# Edge Ticks: план анализа

После 1–2 дней сбора данных через `edge_ticks` нужно проверить несколько гипотез и построить сигнал выхода из позиции до истечения.

## Что собираем

Каждые ~0.5 секунды в последние 5 минут до expiry:

| Поле | Описание |
|------|----------|
| `pm_yes_ask / pm_no_ask` | PM best ask для Up/Down токена |
| `kalshi_yes_ask / kalshi_no_ask` | Kalshi best ask для YES/NO |
| `pm_yes_bid / pm_no_bid` | PM best bid (из WS) |
| `kalshi_yes_bid / kalshi_no_bid` | Kalshi best bid (≈ 1 − other_ask) |
| `yes_ask / no_ask` | Ask на той venue где купили |
| `edge` | 1 − yes_ask − no_ask |
| `binance_price` | Текущая цена на Binance |
| `binance_distance_pct` | `|price − strike| / strike` |

Дополнительно в `positions`:
- `edge_first_10pct / edge_first_15pct / edge_first_20pct` — когда впервые пробили уровень

## Типы исходов

- **Match** — PM=YES и Kalshi=YES (или оба NO): арбитраж сработал, один выиграл, другой проиграл — PnL ≈ edge × stake
- **Double-loss** — оба проиграли: обе ноги ошиблись в одну сторону
- **Double-win** — оба выиграли: обе ноги угадали (тоже убыток — переплатили за обе)

Мисматч = double-loss ИЛИ double-win.

## Гипотезы для проверки

### 1. Binance distance от страйка ⚠️ слабый сигнал

**Логика**: все 38 исторических мисматчей произошли когда цена Binance была в пределах 0.20% от страйка.

**Бэктест (447 позиций, 38 мисматчей, базовая ставка 8.5%)**:

| Порог | Выловлено позиций | Точность | Recall | Lift | Нормальных выброшено |
|-------|-------------------|----------|--------|------|----------------------|
| < 0.03% | 83 | 19.3% | 42% | 2.3x | 67 |
| < 0.05% | 132 | 17.4% | 61% | 2.0x | 109 |
| < 0.10% | 267 | 12.0% | 84% | 1.4x | 235 |
| < 0.20% | 370 | 10.3% | 100% | 1.2x | 332 |

**Вывод**: сигнал слабый — максимум 2.3x lift. 63% нормальных матчей тоже имеют distance < 0.20% (медиана всех рынков = 0.14%). Один только этот признак — не actionable.

**Польза**: может работать как один из компонентов composite сигнала. Записываем `binance_distance_pct` в `edge_ticks` чтобы проверить комбинации с другими признаками.

---

### 2. PM trade price range: max − min > 0.75 за последние 90 секунд

**Логика**: если цена ставки колеблется между <0.25 и >0.75 — значит платформы «не знают» куда идёт цена → высокий риск мисматча.

**Бэктест на исторических данных (dz_trades_cache.json)**:
- 388 позиций, precision 57%, recall 65%, lift 5.2x
- Потенциал: $+62 → $+412 при фильтрации входов

**Сейчас**: этот сигнал основан только на PM торгах. После сбора `edge_ticks` можно проверить то же самое через `pm_yes_ask` траекторию (без зависимости от trade cache).

---

### 3. Форма траектории edge: «плавный рост» vs «резкий пик + откат»

**Гипотеза**: при мисматчах edge растёт равномерно и остаётся высоким (ни одна платформа не пересматривает цену). При match — edge может временно подскакивать но потом откатывается когда обе платформы сходятся.

**Как проверить**:
```python
# Для каждой позиции: slope edge за последние 60с, std(edge), max_edge
# Сравнить match vs mismatch
```

---

### 4. Асимметрия ask/bid спреда близко к expiry

**Гипотеза**: при мисматчах спред bid/ask на PM резко расширяется (маркет-мейкеры уходят) → `pm_yes_ask − pm_yes_bid` увеличивается. При нормальных матчах спред сужается когда цена уходит от 0.5.

**Что нужно**: `pm_yes_bid` и `pm_no_bid` — теперь пишем.

---

### 5. Расхождение PM и Kalshi ask в последние секунды

**Гипотеза**: перед мисматчем PM и Kalshi «смотрят в разные стороны» — `pm_yes_ask` растёт а `kalshi_yes_ask` тоже растёт (вместо того чтобы падать). Т.е. `pm_yes_ask + kalshi_yes_ask > 1.0`.

**Как проверить**:
```python
# pm_yes_ask + kalshi_yes_ask (для матчей должно быть < 1, для мисматчей ~1 или > 1)
# Аналогично: pm_no_ask + kalshi_no_ask
```

---

## Первые результаты по Binance ticks

Date: `2026-04-09`

Это промежуточный срез по уже собранным `edge_ticks`.

Важно:

- выводы пока только по `fast_arb_bot`
- в текущей базе `Binance` есть только для `paper`-позиций
- для `real` позиций в `data/fast_arb_bot.db` таких тиков пока нет
- поэтому пока это не готовый production-фильтр, а рабочая гипотеза

### Что было в базе

- resolved позиций: `597`
- позиций с `pm_close_price`: `457`
- позиций с `kalshi_close_price`: `496`
- позиций с `Binance` ticks: `83`
- из них `real`: `0`
- из них `paper`: `83`

### Кто ближе к Binance на expiry

Сравнение делалось по последнему `Binance`-тику перед expiry.

По всем `83` paper-позициям:

- `PM_close vs Kalshi_close`: avg gap `0.0219%`, median `0.0160%`, p95 `0.0518%`, max `0.1079%`
- `Binance vs PM_close`: avg gap `0.0221%`, median `0.0133%`, p95 `0.0667%`, max `0.1769%`
- `Binance vs Kalshi_close`: avg gap `0.0272%`, median `0.0221%`, p95 `0.0702%`, max `0.1501%`

Но честнее смотреть только свежие тики рядом с expiry.

Для среза `<=5s` до expiry (`46` позиций):

- `PM_close vs Kalshi_close`: avg gap `0.0249%`, median `0.0174%`, p95 `0.0731%`, max `0.1079%`
- `Binance vs PM_close`: avg gap `0.0109%`, median `0.0071%`, p95 `0.0387%`, max `0.0602%`
- `Binance vs Kalshi_close`: avg gap `0.0237%`, median `0.0167%`, p95 `0.0739%`, max `0.1175%`

Промежуточный вывод:

- на expiry `Polymarket` обычно ближе к `Binance`, чем `Kalshi`
- `Kalshi` чаще оказывается дальше и от `Binance`, и от `Polymarket`
- по масштабу `Binance-PM` примерно в 2 раза ближе, чем `Binance-Kalshi`

### Проверка идеи: ловить мисматч за минуту до expiry

Отдельно был взят тик, ближайший к `60s` до expiry.

Важно:

- это не последний тик
- для чистоты дополнительно проверялись окна `45..75s`, `50..70s`, `55..65s`
- результаты почти не менялись

Для окна `55..65s`:

- всего позиций: `59`
- мисматчей: `2`
- базовый mismatch rate: `3.4%`

#### Гипотеза A: PM и Kalshi спорят по стороне за минуту до expiry

Критерий:

- `pm_yes_ask > pm_no_ask` и `kalshi_no_ask > kalshi_yes_ask`
- или наоборот

Результат:

- `16` позиций
- `1` мисматч
- mismatch rate `6.2%`

Это чуть выше baseline, но сигнал пока слабый.

#### Гипотеза B: PM/Binance показывают `NO`, а Kalshi показывает `YES`

Практический proxy без future data:

- `pm_no_ask > pm_yes_ask`
- `kalshi_yes_ask > kalshi_no_ask`

Результат:

- `12` позиций
- `1` мисматч
- mismatch rate `8.3%`

Это уже лучше baseline (`3.4%`), но выборка маленькая.

#### Самый сильный оффлайн-сигнал

Если на срезе `~60s` до expiry `Binance` оказывался ближе к итоговому `PM_close`, чем к `Kalshi_close`, то:

- `13` позиций
- `2` мисматча
- mismatch rate `15.4%`

Если `Binance` был ближе к `Kalshi_close`, то:

- `45` позиций
- `0` мисматчей

Это выглядит сильнее, чем просто `PM side != Kalshi side`.

Но это **не online-фильтр**, потому что использует будущие close-цены:

- `pm_close_price`
- `kalshi_close_price`

Смысл этого результата такой:

- mismatch чаще возникает там, где к expiry `Kalshi` отлипает от `Binance/PM`
- сама идея использовать `Binance` как внешний якорь выглядит правдоподобной

### Что пока НЕ доказано

- что `PM/Binance=NO`, `Kalshi=YES` — устойчивый production-сигнал
- что такой фильтр будет работать на `real`
- что эффект не исчезнет после расширения выборки

### Что проверить позже, когда накопим больше матчей

1. Повторить тот же анализ отдельно по `real`.
2. Считать не только сторону (`YES/NO`), но и величину расхождения:
   - `pm_yes_ask - pm_no_ask`
   - `kalshi_yes_ask - kalshi_no_ask`
   - расстояние `Binance` до `pm_price_to_beat`
   - расстояние `Binance` до `kalshi_reference_price`
3. Построить online-score на срезе `~60s` до expiry без использования future close:
   - `PM side != Kalshi side`
   - `PM/Binance proxy = NO`, `Kalshi = YES`
   - модуль price-gap между `Binance`, `PM target`, `Kalshi reference`
4. Проверить, даёт ли такой score дополнительную пользу поверх простого фильтра по `time-to-expiry`.

---

## Что делаем после анализа

1. Выбираем 1–2 лучших предиктора из гипотез выше
2. Строим **composite exit signal** (пороги по binance_distance + edge форма)
3. Добавляем в `_monitor_edge_divergence()`: при срабатывании сигнала — выставляем лимитные ордера на продажу обеих ног по текущим bid ценам
4. Бэктестируем на собранных `edge_ticks` с реальными bid ценами

## Как запустить анализ

```python
import sqlite3
import pandas as pd

db = sqlite3.connect("data/fast_arb_bot.db")

# Все позиции с edge_ticks
ticks = pd.read_sql("""
    SELECT t.*, p.winning_side, p.pnl, p.symbol,
           p.edge_first_10pct, p.edge_first_15pct
    FROM edge_ticks t
    JOIN positions p ON t.position_id = p.id
    WHERE p.status = 'resolved'
    ORDER BY t.position_id, t.ts
""", db)

# Мисматчи
mismatches = ticks[ticks["winning_side"] == "mismatch"]
matches    = ticks[ticks["winning_side"].isin(["yes", "no"])]

# Binance distance в последние 60с
last60 = ticks[ticks["seconds_to_expiry"] < 60]
last60.groupby(["position_id", "winning_side"])["binance_distance_pct"].min()
```
