# Hourly Above Arbitrage Research

Date of live checks: `2026-03-21`

## Goal

Проверить, можно ли строить реальный арбитраж между hourly `above`-рынками на `Polymarket` и `Kalshi`.

Идея была такой:

- `Kalshi`: strike-based market family, например `Bitcoin price today at 3am EDT?`
- `Polymarket`: strike-based market family, например `Bitcoin above 70,000 on March 21, 3AM ET?`

Если правила совпадают, теоретически можно искать lock-окно:

- `PM YES + Kalshi NO < 1`
- или `Kalshi YES + PM NO < 1`

## What Was Checked

### 1. Matching market family on both venues

Были найдены hourly `above` рынки на обеих платформах для одного и того же settlement window:

- `Polymarket`
  - `Bitcoin above 68,800 on March 21, 3AM ET?`
  - `Bitcoin above 69,200 on March 21, 3AM ET?`
  - `Bitcoin above 69,600 on March 21, 3AM ET?`
  - `Bitcoin above 70,000 on March 21, 3AM ET?`
  - `Bitcoin above 70,400 on March 21, 3AM ET?`

- `Kalshi`
  - `KXBTCD-26MAR2103-T68799.99`
  - `KXBTCD-26MAR2103-T69199.99`
  - `KXBTCD-26MAR2103-T69599.99`
  - `KXBTCD-26MAR2103-T69999.99`
  - `KXBTCD-26MAR2103-T70399.99`

Практическое сопоставление:

- `PM 68,800` <-> `Kalshi floor_strike = 68799.99`
- `PM 69,200` <-> `Kalshi floor_strike = 69199.99`
- `PM 69,600` <-> `Kalshi floor_strike = 69599.99`
- `PM 70,000` <-> `Kalshi floor_strike = 69999.99`
- `PM 70,400` <-> `Kalshi floor_strike = 70399.99`

Это достаточно близко, чтобы считать рынки одним universe для дальнейшей проверки ликвидности.

### 2. Snapshot prices

Live API snapshot на момент проверки:

| Strike | Polymarket yes/no | Kalshi yes/no |
|---|---:|---:|
| 68,800 | `0.505 / 0.495` | `1.0000 / 0.0100` |
| 69,200 | `0.505 / 0.495` | `1.0000 / 0.0100` |
| 69,600 | `0.500 / 0.500` | `1.0000 / 0.0100` |
| 70,000 | `0.500 / 0.500` | `1.0000 / 0.0100` |
| 70,400 | `0.500 / 0.500` | `1.0000 / 0.0100` |

По одному только snapshot это могло выглядеть как сильное арбитражное окно.

Пример:

- `PM YES 0.505 + Kalshi NO 0.010 = 0.515`

Но snapshot сам по себе не доказывает исполнимость.

## Orderbook Verification

### Kalshi

Для тикеров:

- `KXBTCD-26MAR2103-T68799.99`
- `KXBTCD-26MAR2103-T69199.99`
- `KXBTCD-26MAR2103-T69999.99`
- `KXBTCD-26MAR2103-T70399.99`

live orderbook endpoint вернул пустые книги:

```json
{"no_dollars": [], "yes_dollars": []}
```

Это означает:

- top-level market snapshot не подтверждается реальным стаканом
- по этим страйкам нет доступной двусторонней ликвидности для исполнения

### Polymarket

Для страйка `70,000` live CLOB orderbook был таким:

```text
YES bids: 0.013 / 0.012 / 0.011 ...
YES asks: 0.999
NO bids: 0.001
NO asks: 0.987 / 0.988 / 0.989 ...
```

Иными словами:

- мгновенная покупка `YES` фактически стоит почти `1.00`
- мгновенная покупка `NO` тоже фактически стоит почти `0.99`
- рынок очень широкий и практически пустой в зоне “справедливой” цены

## Conclusion

### Confirmed

- Hourly `above` markets на `Polymarket` и `Kalshi` логически матчить можно.
- Это намного лучше, чем пытаться матчить `Kalshi strike-based hourly` против `Polymarket hourly up/down`.

### Not confirmed

- На момент live проверки не удалось подтвердить реальную исполнимую арбитражную возможность.

### Practical conclusion

На текущем live-срезе hourly `above` arbitrage **неисполним**:

- на `Kalshi` у совпадающих страйков стакан пустой
- на `Polymarket` исполнимые asks находятся почти у `0.99-1.00`
- значит “арбитраж”, видимый по snapshot prices, является артефактом плохой ликвидности, а не реальным lock-window

## Final Statement

Строгое формулирование результата:

- Hourly `above` arbitrage между `Polymarket` и `Kalshi` **не доказан как возможный в общем случае**
- Hourly `above` arbitrage на live данных `2026-03-21` **не был исполним**

Это не означает, что такой арбитраж невозможен всегда.
Это означает, что для него нужно:

- искать конкретные часы и страйки с реальной глубиной на обеих площадках
- проверять именно orderbook execution, а не только market snapshot
- строить отдельный scanner именно для `strike-at-time` markets
