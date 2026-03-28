# TODO: Расширить список серий и лиг для матчинга

Kalshi имеет ~1291 активных спортивных рынка в 15+ категориях.
Polymarket аналогично покрывает десятки видов спорта через `seriesSlug`.

## Текущий фильтр (активен)

Скрипт `scripts/dump_expiring_markets.py` сейчас ограничен только теннисом:

| Платформа | Фильтр | Значение |
|---|---|---|
| PM | `seriesSlug` (client-side) | `wta`, `atp` |
| Kalshi | `series_ticker` (API) | `KXWTACHALLENGERMATCH`, `KXATPCHALLENGERMATCH`, `KXATPMATCH` |

## Что добавить следующим

Расширить фильтры в `dump_expiring_markets.py`:
- `PM_SPORTS_FILTER` — добавить нужные `seriesSlug`
- `KALSHI_SPORTS_SERIES` — добавить из `KALSHI_SERIES_ACTIVE`

Также обновить `SPORT_TO_KALSHI_SERIES` в `sports_arb_bot/matcher.py`.

### Теннис (добавить серии WTA Tour)
- PM `seriesSlug`: предположительно `wta-tour` или `wta`
- Kalshi: найти тикер WTA Tour (не Challenger)

## Полный список серий (активны, проверено 2026-03-27)

| Серия | Спорт |
|---|---|
| `KXWTACHALLENGERMATCH` | WTA Challenger теннис |
| `KXATPCHALLENGERMATCH` | ATP Challenger теннис |
| `KXATPMATCH` | ATP Tour (основной тур) |
| `KXBOXING` | Бокс |
| `KXNBAGAME` | NBA |
| `KXNHLGAME` | NHL |
| `KXMLBGAME` | MLB |
| `KXDOTA2GAME` | Dota 2 |
| `KXCS2GAME` | CS2 / Counter-Strike |
| `KXLOLGAME` | League of Legends |

## Что нужно добавить

По данным сайта Kalshi (1291 рынков):

### Баскетбол (206 рынков)
- CBB Tournament (31) — NCAA колледж
- Pro Basketball M (112) — возможно `KXNBAGAME` уже покрывает
- Chinese Basketball Association (10) — `KXCBAGAME`?
- Euroleague (5) — `KXACBGAME` или `KXEUROLEAGUEGAME`?
- Pro Basketball W (5) — WNBA, `KXWNBAGAME`?

### Футбол / Soccer (273 рынков)
- Крупнейшая категория, нужно найти серии:
  - Premier League, Champions League, MLS, и др.
  - Предположительно: `KXPREMGAME`, `KXUEFAGAME`, `KXMLSGAME`, `KXAFCCLGAME`

### Американский футбол / Football (125 рынков)
- NFL game — `KXNFLGAME`?
- College Football — `KXCFBGAME`?

### Хоккей (114 рынков)
- NHL уже есть (`KXNHLGAME`)
- Другие лиги?

### Киберспорт (199 рынков)
- Dota 2, CS2, LoL уже есть
- Другие игры (Valorant, Overwatch, R6, и др.)

### MMA (22 рынков)
- `KXMMA` — был пустым 2026-03-27, но обычно активен

### Бокс (16 рынков)
- `KXBOXING` уже есть
- `KXBOXINGFIGHT` — был пустым 2026-03-27

### Гольф (33 рынка)
- `KXGOLFH2H` — Head-to-Head матчи

### Крикет (15 рынков)
- `KXCRICKETODIMATCH`, `KXCRICKETT20IMATCH`, `KXCRICKETTESTMATCH`

### Лакросс (34 рынка)
- Неизвестные серии

### Регби (24 рынка)
- Неизвестные серии

### Австралийский футбол (6 рынков)
- `KXAFLGAME`

### Моторспорт (24 рынка)
- F1, NASCAR и др.

## Как найти тикеры серий

```bash
# Полный список серий с category=Sports
python3 -c "
import httpx, json
resp = httpx.get('https://api.elections.kalshi.com/trade-api/v2/series', timeout=20)
sports = [s for s in resp.json()['series'] if s.get('category') == 'Sports']
for s in sorted(sports, key=lambda x: x['ticker']):
    print(s['ticker'], '|', s['title'])
"

# Проверить активность конкретной серии
python3 -c "
import httpx
resp = httpx.get('https://api.elections.kalshi.com/trade-api/v2/markets',
    params={'status': 'open', 'series_ticker': 'KXNFLGAME', 'limit': '5'})
print(len(resp.json().get('markets', [])), 'рынков')
"
```
