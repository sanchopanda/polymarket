# cross_arb_bot

Paper-trading бот для кросс-маркет арбитража `Polymarket ↔ Kalshi`.

## Что делает

- ищет логически сопоставленные крипто-рынки двух площадок
- считает lock-окна вида `YES на одной площадке + NO на другой < $1`
- открывает paper-позиции и ведёт виртуальный баланс отдельно по двум биржам
- хранит данные в `data/cross_arb_bot.db`

## Как запустить

Из корня репозитория:

```bash
# поиск без открытия позиций
python3 -m cross_arb_bot scan --dry

# разовый scan с paper-входом
python3 -m cross_arb_bot scan

# симуляция execution cycle
python3 -m cross_arb_bot sim

# live watch: HTTP discovery + PM websocket
python3 -m cross_arb_bot watch

# статус
python3 -m cross_arb_bot status

# резолюция позиций
python3 -m cross_arb_bot resolve

# отчёт по ликвидности из сохранённых snapshot
python3 -m cross_arb_bot liquidity-report

# paper-перевод между площадками
python3 -m cross_arb_bot rebalance --from polymarket --to kalshi --amount 50

# непрерывный цикл
python3 -m cross_arb_bot run --interval 20
python3 -m cross_arb_bot live
```

## Конфиг

`cross_arb_bot/config.yaml`

Ключевые секции:

- `trading`
- `market_filter`
- `runtime`
- `db`
- `polymarket`
- `kalshi`

## Переменные окружения

Для paper-режима не нужны.
