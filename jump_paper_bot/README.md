# jump_paper_bot

Отдельный paper-only бот для short crypto `Polymarket` рынков.

Сигнал v1:

- только `Polymarket`
- только символы `BTC/ETH/SOL/XRP/DOGE/BNB`
- только интервалы `5m` и `15m`
- side = `yes` или `no`
- best ask должен попасть в окно `(level, level + $0.05]`
- средняя цена той же стороны за предыдущие `10s` должна быть ниже текущей минимум на `$0.05`
- buckets работают независимо: `60s`, `40s`, `30s`
- на один ключ `(market_id, side, bucket)` допускается только один сигнал

Paper fill:

- при сигнале бот тянет CLOB стакан по нужному `token_id`
- смотрит ask-уровни только до `signal_price + $0.05`
- суммарная глубина в этом диапазоне должна быть не меньше `2 x stake`
- затем симулируется fill фиксированного `$5` проходом по asks

Telegram:

- использует тот же binding, что `oracle_arb_bot`
- по умолчанию:
  - `token_env = SIMPLE_BOT_TOKEN`
  - `chat_id_file = data/.telegram_chat_id`
- команда статуса: `/jump_status`

CLI:

```bash
python3 -m jump_paper_bot watch
python3 -m jump_paper_bot status
python3 -m jump_paper_bot resolve
```

Конфиг: `jump_paper_bot/config.yaml`

База: `data/jump_paper_bot.db`

Основные таблицы:

- `positions` — paper-позиции
- `signals` — один лог на bucket-сигнал
- `price_history` — live history для диагностики

