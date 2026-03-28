# Как добавить новый спорт / лигу в парсер

Этот документ описывает стандартный флоу, который Claude должен выполнять каждый раз,
когда пользователь скидывает новую пару рынков PM + Kalshi для добавления в бота.

---

## Шаг 1. Получить структуру обоих рынков по API

**Пользователь скидывает:** ссылку на PM и ссылку на Kalshi.

Из URL извлекаем идентификаторы:
- PM: `https://polymarket.com/.../{slug}` → slug
- Kalshi: `https://kalshi.com/markets/{event_ticker}/.../{market_ticker}`

Запрашиваем сырые данные:

```python
# PM
GET https://gamma-api.polymarket.com/markets?slug={slug}

# Kalshi — событие
GET https://api.elections.kalshi.com/trade-api/v2/events/{event_ticker}
# Kalshi — маркеты события
GET https://api.elections.kalshi.com/trade-api/v2/markets?event_ticker={event_ticker}&status=open
```

**Что смотрим в ответе:**

| Поле | Где искать | Зачем |
|---|---|---|
| `seriesSlug` | PM `events[0].seriesSlug` | ключ фильтрации в PM feed |
| `sportsMarketType` | PM root | должно быть `"moneyline"` |
| `gameStartTime` | PM root | время начала матча |
| `endDate` | PM root | максимальное время закрытия рынка |
| `outcomes` | PM root | имена команд/игроков |
| `series_ticker` | Kalshi event | серия для запроса |
| `expected_expiration_time` | Kalshi **market** (не event!) | время экспирации (иногда null в event) |
| `yes_sub_title` | Kalshi market | имя команды/игрока |
| `product_metadata.competition` | Kalshi event | название соревнования |

---

## Шаг 2. Определить временно́е окно

Вычисляем разницу между `gameStartTime` (PM) и `expected_expiration_time` (Kalshi):

```
delta = expected_expiration - gameStartTime
```

Типичные значения:
- Теннис: delta ≈ 2–4ч
- R6 BO1: delta ≈ 4ч
- Более длинные форматы: до 6–8ч

**Стандартное окно бота** (в `watch_runner.py` и `feed_*.py`) уже покрывает это:
- PM: `now - 1h45m … now + 5h` по `gameStartTime`
- Kalshi: `now + 15min … now + 7h` по `expected_expiration_time`

Если delta > 6ч или если игры публикуются сильно заблаговременно → ничего менять не нужно:
бот всё равно подхватит их когда они войдут в окно (скан каждые 15 мин).

Для **ручной проверки** через dump-скрипт можно расширить окно флагом `--hours N`.

---

## Шаг 3. Проверить матчинг команд

Сравниваем `outcomes` (PM) с `yes_sub_title` (Kalshi маркеты):

- **Имена совпадают** (идентично или почти): `TennisMatcher` подойдёт — он токен-матчит слова ≥4 символов, нечувствителен к порядку и регистру. Это работает для тенниса, R6 и большинства командных спортов.
- **Имена сильно расходятся** (разные аббревиатуры, транслитерации): нужен `LLMSportsMatcher` или новый кастомный матчер.

Тест в скрипте/консоли:
```python
from sports_arb_bot.sport_matcher import TennisMatcher
matches = TennisMatcher().match(pm_events, ka_events)
# Если len(matches) > 0 и outcome_map корректный — TennisMatcher подходит
```

---

## Шаг 4. Написать dump-скрипт для проверки списков

Копируем `scripts/dump_r6_markets.py` → `scripts/dump_{sport}_markets.py`, меняем:

```python
PM_SERIES_SLUG = "..."   # значение из events[0].seriesSlug (Шаг 1)
KALSHI_SERIES  = "..."   # series_ticker из Kalshi (Шаг 1)
```

**Запускаем с датой конкретного матча** (из `gameStartTime` полученного на Шаге 1):
```bash
python3 scripts/dump_{sport}_markets.py --date 2026-03-28T16:30:00+00:00
```

Окно устанавливается как `[gameStartTime - 2ч, gameStartTime + 10ч]` — точно покрывает
нужный матч без лишней загрузки. Для обзора всех предстоящих игр:
```bash
python3 scripts/dump_{sport}_markets.py --hours 96
```

Смотрим в `data/`:
- `pm_{sport}.json` / `pm_{sport}_titles.json` — что нашли на PM
- `kalshi_{sport}.json` / `kalshi_{sport}_titles.json` — что нашли на Kalshi

Сверяем: матч из URL виден с обеих сторон? Имена команд совпадают с тем, что ожидали на Шаге 3?

---

## Шаг 5. Прописать новый спорт в бот (5 файлов)

### 5.1 `feed_polymarket.py`

Если PM `seriesSlug` совпадает с нашим internal sport label (например `"wta"` → `"wta-"` prefix):
```python
SLUG_PREFIX_TO_SPORT["название-prefix-"] = "sport_label"
```

Если PM `seriesSlug` отличается (например `"rainbow-six-siege"` → `"r6"`):
```python
SERIES_SLUG_TO_SPORT["pm-series-slug"] = "sport_label"
```

### 5.2 `feed_kalshi.py`

```python
SERIES_TO_SPORT["KX..."] = "sport_label"
```

### 5.3 `watch_runner.py`

```python
KALSHI_SERIES_BY_SPORT["sport_label"] = ["KX..."]
```

### 5.4 `config.yaml`

```yaml
sports: [wta, atp, r6, sport_label]
```

### 5.5 `matcher.py` (только если используется engine.py / LLM-режим)

```python
SPORT_TO_KALSHI_SERIES["sport_label"] = ["KX..."]
```

---

## Шаг 6. Проверить матчинг live-данных

```bash
python3 -c "
from sports_arb_bot.feed_polymarket import PolymarketSportsFeed
from sports_arb_bot.feed_kalshi import KalshiSportsFeed
from sports_arb_bot.sport_matcher import TennisMatcher

pm = PolymarketSportsFeed().fetch(['sport_label'])
ka = KalshiSportsFeed().fetch(['KX...'])
matches = TennisMatcher().match(pm, ka)
for m in matches:
    print(m.pm_event.slug, '↔', m.kalshi_event.event_ticker)
    print('  outcome_map:', m.match_result.outcome_map)
    print('  edge:', m.arb_edge())
"
```

---

## Шаг 7. Добавить новый матчер (если нужен)

Если `TennisMatcher` не справляется, создаём новый класс в `sport_matcher.py`:

```python
class R6Matcher:  # пример
    def match(self, pm_events, ka_events) -> list[MatchedSportsPair]:
        ...
```

Подключаем через фабрику `get_matcher()`:

```python
def get_matcher(sport: str, use_llm: bool = False) -> SportMatcherProtocol:
    if sport == "r6":
        return R6Matcher()
    if use_llm:
        return LLMSportsMatcher()
    return TennisMatcher()
```

В `watch_runner.py` заменяем жёстко прописанный `TennisMatcher()` на:
```python
from sports_arb_bot.sport_matcher import get_matcher
matcher = get_matcher(sport)  # или get_matcher(pair.sport)
```

---

## Быстрая сводка — чеклист

```
[ ] 1. Запросить PM ?slug= и Kalshi /markets/?event_ticker=
[ ] 2. Записать: PM seriesSlug, Kalshi series_ticker, gameStartTime, expected_expiration_time, delta
[ ] 3. Сравнить имена команд PM (outcomes) vs Kalshi (yes_sub_title): TennisMatcher или нужен новый?
[ ] 4. Написать dump-скрипт (копия dump_r6_markets.py), запустить --date <gameStartTime>
[ ] 5. Проверить: нужный матч виден в pm_titles и kalshi_titles с правильными именами
[ ] 6. feed_polymarket.py  — добавить SERIES_SLUG_TO_SPORT или SLUG_PREFIX_TO_SPORT
[ ] 7. feed_kalshi.py      — добавить SERIES_TO_SPORT
[ ] 8. watch_runner.py     — добавить KALSHI_SERIES_BY_SPORT
[ ] 9. config.yaml         — добавить в sports: [] с комментарием
[ ] 10. Тест: TennisMatcher().match(pm, ka) с данными из dump — outcome_map корректен
```

---

## Пример: добавление R6 (Rainbow Six Siege)

```
PM URL:     https://polymarket.com/esports/rainbow-six-siege/europe-mena-league/r6siege-secret-tm-2026-03-30
Kalshi URL: https://kalshi.com/markets/kxr6game/r6-game/kxr6game-26mar30tmsecret

PM seriesSlug:        "rainbow-six-siege"  →  internal: "r6"
Kalshi series_ticker: "KXR6GAME"
gameStartTime:        19:00 UTC
expected_expiration:  23:00 UTC  (delta = 4h — покрывается стандартным окном)
Команды PM:           ["Team Secret", "Twisted Minds"]
Команды Kalshi:       ["Team Secret", "Twisted Minds"]  ← совпадают → TennisMatcher ✓

Dump-скрипт: scripts/dump_r6_markets.py --hours 96
  (шире стандартного, чтобы видеть предстоящие игры при ручной проверке)
```
