# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Structure

Two independent bots in one repo:

- `/` (root) — **Polymarket Paper Trading Bot** — simulated Martingale on Polymarket prediction markets
- `bybit-bot/` — **Bybit Futures Bot** — live Martingale trading on Bybit perpetual futures (demo account)

The codebase is in Russian (comments, CLI output, README). Keep new code and messages consistent with this.

---

## Polymarket Bot

Martingale paper/real trading bot targeting **crypto outcomes priced 0.35–0.50** (multiplier 2.0x–2.86x), expiring within ~2 hours. On loss, doubles the bet on a new market. Series ends with a win (profit ≈ initial bet) or reaching the depth limit.

**Current config (config.yaml):**
- Price range: 0.35–0.50 (`price_min`/`price_max`)
- Markets: `fee_type: crypto_fees` (Bitcoin/Ethereum Up/Down only)
- Expiry window: 0–2 hours (`max_days_to_expiry: 0.083`)
- Min volume: $1000, min liquidity: $1000
- Initial bet: $1.0, max depth: 6, taker fee: 2%
- Series count: dynamic (see below)

## Commands

```bash
source venv/bin/activate

# Run a scan (find candidates and create new Martingale series)
python -m src.main scan

# Dry run (show candidates without saving)
python -m src.main scan --dry

# Check resolutions + escalate losing series
python -m src.main resolve

# Show Martingale series
python -m src.main series

# Show P&L dashboard
python -m src.main dashboard

# Continuous mode (resolve + scan every N hours)
python -m src.main run --interval 6

# Backtest на исторических данных
python -m src.main backtest --limit 300
python -m src.main backtest --limit 100 --no-price-history  # быстро, entry=0.50

# Tests
pip install -e ".[dev]"
pytest tests/
```

## Architecture

**Data flow:** Gamma API → Scanner (filter by target_price±tolerance) → Scorer (rank) → Sizer (Martingale: initial_bet * 2^depth) → PaperTradingEngine (series management) → SQLite store

Key modules:

- `src/api/gamma.py` — HTTP client for Gamma API (market discovery). Paginates through all active markets. Returns `Market` dataclass with outcomes, prices, token IDs.
- `src/api/clob.py` — HTTP client for CLOB API (orderbook). Used optionally to verify liquidity before placing a bet (`check_liquidity` config flag).
- `src/strategy/scanner.py` — Filters markets by price range (target_price±tolerance), volume, liquidity, expiry window. Produces `Candidate` per eligible outcome.
- `src/strategy/scorer.py` — Weighted scoring: price proximity to target (40%), liquidity (35%), time to expiry (25%). Returns `ScoredCandidate`.
- `src/strategy/sizing.py` — Martingale sizing: `initial_bet_size * multiplier^series_depth`. Also contains `compute_max_deep_slots()` and `compute_dynamic_active_series()` for dynamic series count.
- `src/paper/engine.py` — Orchestrates series lifecycle: `run_scan()` creates new series, `check_resolutions()` resolves bets and escalates on loss. Central class: `PaperTradingEngine`.
- `src/real/engine.py` — Real trading engine via CLOB API. Same strategies as paper but places actual orders. Includes blockchain redeem logic for won bets via CTF contract.
- `src/db/store.py` — SQLite via raw `sqlite3`. Tables: `simulated_bets`, `bet_series`, `scan_logs`. Handles bet/series CRUD, portfolio stats.
- `src/db/models.py` — Dataclasses: `BetSeries`, `SimulatedBet`, `ScanLog`, `PortfolioSnapshot`. Series have status: active/waiting → won/abandoned. Bets have status: open → won/lost.
- `src/reports/dashboard.py` — Rich-based CLI dashboard for P&L, series, positions, history.
- `src/config.py` — Typed dataclass config loaded from `config.yaml`. Key sections: `StrategyConfig` (target_price, tolerance), `MartingaleConfig` (initial_bet, max_depth, max_series), `PaperTradingConfig` (fees, liquidity check, starting_balance). Env override: `BOT_CONFIG`.
- `src/backtest/fetcher.py` — Загружает закрытые рынки через Gamma API (с серверными фильтрами по объёму/ликвидности/fee_type), ищет в истории CLOB цен момент когда исход попал в целевой диапазон — это точка входа. Возвращает `HistoricalMarket`.
- `src/backtest/simulator.py` — Чистая in-memory симуляция серий Мартингейла на `List[HistoricalMarket]`. Считает P&L с учётом taker_fee. Возвращает `BacktestResult`.
- `src/backtest/report.py` — Rich-вывод результатов: win rate, ROI, P&L, распределение по глубинам, топ-5 худших серий.

**Important design details:**
- All HTTP clients use `httpx` (sync), not `requests`.
- No authentication needed for paper — uses only public Polymarket endpoints. Real trading uses CLOB API with wallet private key.
- Database is SQLite at `data/bot.db` (paper) / `data/real.db` (real). Schema migrates idempotently.
- Bet deduplication: engine checks `store.already_bet()` before placing, keyed on (market_id, outcome) with status='open'.
- Resolution logic: exit_price >= 0.9 means "won" (contract settled to ~$1).
- Martingale series: on loss, engine calls `_escalate_series()` which scans for a new market and places 2x the previous bet. Series is "abandoned" only if max depth reached or no suitable market found.
- In continuous mode (`cmd_run`): resolve runs BEFORE scan to free budget before creating new series.
- Smart sleep: if free slots exist, sleeps until nearest market expiry (max 2 min); if all slots full, sleeps until nearest expiry.

**Dynamic series count:**
- Number of concurrent series is calculated dynamically from actual capital (not a config constant).
- `compute_max_deep_slots(total_capital, initial_bet, max_depth)`: computes how many deep series (depth≥2) the capital can support, with a buffer of one extra deep series worth of capital. Formula: `n = floor((capital - 3*cost_shallow) / cost_deep)`, `deep_slots = max(1, n-1)`.
- `compute_dynamic_active_series(deep_slots)`: total series = `deep_slots + 3` (N deep + 3 shallow with max depth 2).
- Real engine: `total_capital = wallet_balance + total_invested_in_active_series`.
- Paper engine: `total_capital = starting_balance`.

**Balance checks and series waiting:**
- Before placing any bet (new series or escalation), balance is checked against the required size.
- If balance is insufficient: series goes to `waiting` status (not abandoned), will be retried when funds become available.
- Real engine continuous mode: if `wallet_balance < initial_bet` and waiting series exist → Telegram alert + bot stops.

**Redeem logic (real engine):**
- Won bets are redeemed via `redeemPositions` on CTF contract (Polygon).
- Redeem only attempted when `market.closed=True` (oracle has finalized via `reportPayouts`).
- UMA Oracle has ~2-hour dispute window after market expiry before `closed=True`.
- If redeem fails (tx revert, RPC error), bet stays open and retries next cycle.
- Nonce management: uses `confirmed_nonce` (not `pending_nonce`) to handle stuck transactions, with 30% gas boost for replacements.

---

## Bybit Futures Bot (`bybit-bot/`)

Live Martingale trading on Bybit perpetual futures using demo account. Opens Long/Short positions with TP/SL. On SL hit, doubles margin on a different symbol. Series ends on TP hit (won) or max depth reached (abandoned).

### Commands

```bash
cd bybit-bot
pip install -r requirements.txt

# Запустить в цикле (интервал 10 сек)
python3 -m src.main run --interval 10

# Разовая проверка позиций
python3 -m src.main check

# Открыть новые серии вручную
python3 -m src.main open

# Дашборд
python3 -m src.main dashboard

# Серии
python3 -m src.main series

# Открытые позиции
python3 -m src.main positions

# Баланс аккаунта
python3 -m src.main balance
```

### Architecture

**Data flow:** BybitClient (market price) → MartingaleEngine (series mgmt) → place_order (TP/SL) → check_positions (closed_pnl) → SQLite store

Key modules:

- `src/api/bybit.py` — Bybit V5 REST API with HMAC signing. Supports demo/testnet/mainnet via URL switch. Methods: `get_ticker`, `set_leverage`, `place_order`, `get_positions`, `get_closed_pnl`, `get_wallet_balance`.
- `src/engine/martingale.py` — Core engine. `run_cycle()` calls `check_positions()` then `open_new_series()`. Detects TP/SL via `get_positions` (size=0 → closed) + `get_closed_pnl` for PnL. Escalation picks a different symbol not currently occupied by any open trade.
- `src/db/store.py` — SQLite store. Tables: `series`, `trades`. Series tracks symbol, depth, total_invested, total_pnl. Trade tracks order_id, entry/exit price, pnl, TP/SL levels.
- `src/db/models.py` — `Series`, `Trade` dataclasses.
- `src/reports/dashboard.py` — Rich dashboard: portfolio summary, depth stats, series table, open positions, trade history.
- `src/config.py` — `BybitConfig` (mode, api_key, symbols list, leverage), `MartingaleConfig` (initial_margin, take_profit_pct, stop_loss_pct, max_depth, max_active_series).

**Important design details:**
- Demo trading uses `https://api-demo.bybit.com` (different from testnet). API keys must be created on bybit.com demo trading, NOT testnet.bybit.com.
- Instrument qty step hardcoded in `INSTRUMENT_STEP` dict: XRPUSDT=0.1, DOGEUSDT=1.0, SOLUSDT=0.01, BNBUSDT=0.001, ETHUSDT=0.01.
- Qty calculation: `floor((margin * leverage / price) / step) * step`.
- TP = 1.2%, SL = 1.0% — asymmetric to cover taker fees (0.055%×2) and ensure ROI > 100% of initial margin.
- Side (Buy/Sell) chosen randomly per trade.
- Escalation symbol: picks first symbol from config list not currently occupied by any open trade, preferring a different symbol from the previous trade.
- Grace period: newly opened trades (< 5 sec) are skipped during position check to avoid false "size=0" reads.
- Leverage is set once per symbol per process run (cached in `_leverage_set`).
