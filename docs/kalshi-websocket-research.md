# Kalshi WebSocket API — исследование

## Текущая архитектура

- **Polymarket**: WS — триггер при изменении цены в реальном времени
- **Kalshi**: HTTP polling — цена обновляется раз в 60с при пересканировании

Execution pricing всегда свежий: прямо перед ордером `_execution_leg_info()` делает HTTP-запрос к Kalshi orderbook. Т.е. без WS мы не теряем точность при исполнении, но пропускаем окна, когда Kalshi двигается без движения Polymarket.

## Kalshi WebSocket API v2

**URL:** `wss://api.elections.kalshi.com/trade-api/ws/v2`

**Авторизация:** те же RSA-PSS заголовки что и для HTTP (уже реализованы в `real_arb_bot/clients.py` и `test_real_kalshi.py`):
- `KALSHI-ACCESS-KEY`
- `KALSHI-ACCESS-SIGNATURE`
- `KALSHI-ACCESS-TIMESTAMP`

Передаются как `additional_headers` при handshake:
```python
with connect(url, additional_headers=kalshi_trader._headers("GET", "/trade-api/ws/v2")) as ws:
    ...
```

**Подписка на orderbook:**
```json
{
  "id": 1,
  "cmd": "subscribe",
  "params": {
    "channels": ["orderbook_delta"],
    "market_ticker": "KXBTC15M-26MAR211945-45"
  }
}
```

**Формат сообщений:**

Начальный снапшот (`orderbook_snapshot`):
```json
{
  "type": "orderbook_snapshot",
  "sid": 1,
  "seq": 100,
  "msg": {
    "market_ticker": "KXBTC15M-...",
    "yes_dollars_fp": [[0.64, 904.0], [0.65, 1007.0]],
    "no_dollars_fp": [[0.38, 10.0], [0.39, 208.0]]
  }
}
```

Инкрементальное обновление (`orderbook_delta`):
```json
{
  "type": "orderbook_delta",
  "sid": 1,
  "seq": 101,
  "msg": {
    "market_ticker": "KXBTC15M-...",
    "price_dollars": 0.65,
    "delta_fp": -50.0,
    "side": "yes",
    "timestamp": "2026-03-21T23:34:23Z"
  }
}
```

**Keepalive:** стандартный ping/pong через `websockets`, никакой ручной логики не нужно.

## План реализации

### Шаг 1: `arb_bot/kalshi_ws.py`
Новый клиент по аналогии с `arb_bot/ws.py`:
```python
class KalshiWebSocketClient:
    def __init__(self, tickers: list[str], on_message: Callable, kalshi_headers_fn: Callable):
        # kalshi_headers_fn = kalshi_trader._headers (RSA-PSS signing)
        ...

    def _run(self):
        with connect(WS_URL, additional_headers=self.kalshi_headers_fn("GET", "/trade-api/ws/v2")) as ws:
            for ticker in self.tickers:
                ws.send(json.dumps({
                    "id": seq++,
                    "cmd": "subscribe",
                    "params": {"channels": ["orderbook_delta"], "market_ticker": ticker}
                }))
            # читаем snapshot + delta, обновляем TopOfBook
```

Нужно накапливать orderbook из снапшота и применять дельты, извлекать best_ask.

### Шаг 2: `cross_arb_bot/watch_runner.py`
- Добавить `KalshiWebSocketClient` рядом с Polymarket WS
- `live_books_kalshi: dict[ticker, TopOfBook]`
- Триггер `_maybe_open_pair()` теперь срабатывает и от Kalshi обновлений
- При пересканировании (60с) пересоздавать Kalshi WS с новым набором тикеров

### Файлы для изменения
- `arb_bot/kalshi_ws.py` — новый файл
- `cross_arb_bot/watch_runner.py` — добавить Kalshi WS рядом с Polymarket WS

### Зависимость авторизации
`KalshiWebSocketClient` нужен доступ к `_headers()`. Варианты:
1. Передавать `key_id` и `private_key` напрямую в клиент
2. Передавать callable `headers_fn` из `KalshiTrader`

Вариант 2 предпочтителен — не дублирует RSA-логику.

## Приоритет

Некритично для v1. Добавить когда:
- замечаем пропуск очевидных окон из-за медленного скана Kalshi
- хотим триггериться по изменениям Kalshi-цены независимо от Polymarket
