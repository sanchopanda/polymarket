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
