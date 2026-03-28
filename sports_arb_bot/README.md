# sports_arb_bot

Paper-trading арбитражный бот для спортивных рынков Polymarket ↔ Kalshi.

## Принцип

Lock-арбитраж на теннисных матчах: покупаем YES игрока A на одной платформе и YES игрока B на другой. Так как ровно один из них победит, пейаут = $1 гарантирован. Если сумма цен < $1 — есть edge.

```
Leg 1: Buy A на PM + Buy B на Kalshi → cost = pm_ask_A + ka_ask_B
Leg 2: Buy B на PM + Buy A на Kalshi → cost = pm_ask_B + ka_ask_A
Edge  = max(1 - cost1, 1 - cost2)
```

## Запуск

```bash
# Запустить бота
python3 -m sports_arb_bot watch

# Статус и позиции
python3 -m sports_arb_bot status

# Ручной резолв завершённых матчей
python3 -m sports_arb_bot resolve

# Посмотреть найденные пары без запуска бота
python3 -m sports_arb_bot scan
```

## Конфиг

`sports_arb_bot/config.yaml`:

| Параметр | По умолчанию | Описание |
|---|---|---|
| `trading.stake_usd` | 60 | Размер ставки на одну позицию |
| `trading.min_edge` | 0.02 | Минимальный edge для открытия |
| `trading.min_leg_price` | 0.05 | Нижний лимит цены ноги |
| `trading.max_leg_price` | 0.95 | Верхний лимит цены ноги |
| `trading.scan_interval_seconds` | 900 | Интервал сканирования (15 мин) |
| `sports` | [wta, atp] | Виды спорта |

## Поведение

- **Сканирование** каждые 15 минут: скачивает рынки PM и Kalshi, матчит теннисные события токен-матчингом (без LLM)
- **WebSocket** на live цены: PM WS (токены) + Kalshi WS (тикеры). Сокеты живут до конца матча — при цене ≥ 0.99 или ≤ 0.01 пара считается завершённой
- **Edge detection**: при каждом WS-обновлении проверяет два направления арбитража
- **Paper bet**: `shares = floor(60 / cost)`, записывается в БД с проверкой depth стакана
- **Резолв**: каждые 5 минут проверяет матчи, начавшиеся > 3ч назад, через REST API PM и Kalshi

## Временные окна

| Платформа | Поле | Окно |
|---|---|---|
| Polymarket | `gameStartTime` | [now-1h45m, now+5h] |
| Kalshi | `expected_expiration_time` | [now+15m, now+7h] |

PM отстаёт на ~2ч: берём начало матча, Kalshi — конец.

## Данные

БД SQLite: `data/sports_arb_bot.db`

- **positions** — paper-позиции с PnL
- **orderbook_snapshots** — depth стакана в момент сигнала (аналитика масштабируемости)
- **virtual_balance** — виртуальный баланс (старт $10 000)

## Telegram

Уведомления через `TELEGRAM_TOKEN` из `.env`:
- Новые матчи при скане
- Открытие paper-ставки (цены, edge, depth, lock_valid)
- Резолв (победитель, P&L)
- `/status` — текущий баланс и позиции

## Файлы

| Файл | Описание |
|---|---|
| `config.yaml` | Конфигурация |
| `watch_runner.py` | Основной цикл, WS, edge detection, paper bets |
| `sport_matcher.py` | DI-матчеры: TennisMatcher (токены) + LLMSportsMatcher |
| `db.py` | SQLite: positions, orderbook_snapshots, virtual_balance |
| `feed_polymarket.py` | Скачивание рынков PM с фильтрацией |
| `feed_kalshi.py` | Скачивание рынков Kalshi по сериям |
| `telegram_notify.py` | Telegram-уведомления |
| `models.py` | Датаклассы: PMSportsEvent, KalshiMatchEvent, MatchedSportsPair |

## Расширение на другие спорты

1. Добавить серии Kalshi в `watch_runner.py → KALSHI_SERIES_BY_SPORT`
2. Добавить `seriesSlug` в `feed_polymarket.py → SLUG_PREFIX_TO_SPORT`
3. При необходимости реализовать новый матчер через `SportMatcherProtocol` в `sport_matcher.py`

Список доступных серий Kalshi: `docs/todo_kalshi_series.md`
