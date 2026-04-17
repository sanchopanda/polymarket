# Recovery Real DB Pattern Snapshot

Дата снимка: 2026-04-17

Это snapshot по локальной `data/recovery_bot.db`. Нужен как базовая точка сравнения: позже можно будет проверить, подтверждаются ли паттерны на новом окне данных.

## Общий итог

- resolved real: 440
- WR: 67.27%
- PnL: $+4.65
- avg entry: 0.6488
- avg touch->trigger delay: 137.8s
- последние 12h от max(opened_at=2026-04-17T09:36:10.649681): n=365, WR=65.75%, PnL=$-3.99, avg entry=0.6483

## По стратегиям

- 5m_base: n=294, WR=69.39%, PnL=$+10.60, avg entry=0.6525, avg delay=86.7s
- 15m_wait30: n=146, WR=63.01%, PnL=$-5.95, avg entry=0.6413, avg delay=240.7s

## По символам

- ETH: n=89, WR=73.03%, PnL=$+7.34, avg entry=0.6575, avg delay=151.2s
- BTC: n=77, WR=71.43%, PnL=$+4.45, avg entry=0.6583, avg delay=170.8s
- SOL: n=73, WR=58.90%, PnL=$-8.28, avg entry=0.6496, avg delay=117.5s
- DOGE: n=72, WR=73.61%, PnL=$+8.17, avg entry=0.6418, avg delay=90.5s
- XRP: n=65, WR=56.92%, PnL=$-8.32, avg entry=0.6401, avg delay=158.1s
- BNB: n=64, WR=67.19%, PnL=$+1.28, avg entry=0.6409, avg delay=135.0s

## Minute-of-hour buckets

- 00-15: n=125, WR=71.20%, PnL=$+8.21
- 15-30: n=96, WR=62.50%, PnL=$-6.29
- 30-45: n=112, WR=70.54%, PnL=$+6.66
- 45-60: n=107, WR=63.55%, PnL=$-3.94

## Touch->Trigger delay buckets

- 0-30s: n=51, WR=58.82%, PnL=$-5.22
- 30-60s: n=90, WR=68.89%, PnL=$+2.82
- 60-120s: n=133, WR=69.92%, PnL=$+5.81
- 120-300s: n=114, WR=70.18%, PnL=$+5.94
- 300-infs: n=52, WR=59.62%, PnL=$-4.70

## Entry price buckets

- 0.60-0.63: n=39, WR=66.67%, PnL=$+2.04, avg delay=129.5s
- 0.63-0.65: n=72, WR=63.89%, PnL=$-1.60, avg delay=138.6s
- 0.65-0.67: n=182, WR=68.68%, PnL=$+4.41, avg delay=126.3s
- 0.67-0.69: n=69, WR=71.01%, PnL=$+2.13, avg delay=134.3s
- 0.69-1.00: n=50, WR=64.00%, PnL=$-5.27, avg delay=149.2s

## Гипотезы для следующей проверки

- Быстрые recovery до 30s дают слабее результат, чем 30-300s.
- Для 5m стратегия окно 120-300s выглядит особенно сильным.
- Внутри часа слабее выглядит окно 15-30 минут; лучше 00-15 и 30-45.
- Диапазон entry 0.65-0.69 выглядит сильнее, чем <0.65 и >=0.69.
