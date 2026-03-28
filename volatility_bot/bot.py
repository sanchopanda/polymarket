from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime
from typing import Optional

from volatility_bot.db import VolatilityDB
from volatility_bot.executor import BetExecutor
from volatility_bot.models import Bet, VolatilityMarket
from volatility_bot.scanner import MarketScanner
from volatility_bot.strategy import (
    compute_market_minute,
    compute_market_quarter,
    compute_position_pct,
    evaluate_signal,
)


class VolatilityBot:
    def __init__(self, config: dict, db: VolatilityDB) -> None:
        self._cfg = config
        self._db = db
        self._dry_run: bool = config["runtime"]["dry_run"]
        self._stake_usd: float = config["trading"]["stake_usd"]
        self._buckets: list[dict] = config["strategy"]["buckets"]
        self._scan_interval: int = config["runtime"]["scan_interval_seconds"]

        # In-memory dedup: (venue, market_id, side, trigger_bucket)
        self._placed: set[tuple[str, str, str, str]] = set()
        self._placed_lock = threading.Lock()
        self._load_placed_from_db()

        self._scanner = MarketScanner(config)
        self._scanner.set_price_callback(self._on_price_update)

        # Traders — only instantiate when not dry_run
        pm_trader = None
        kalshi_trader = None
        if not self._dry_run:
            from real_arb_bot.clients import KalshiTrader, PolymarketTrader
            pm_trader = PolymarketTrader()
            kalshi_trader = KalshiTrader()

        self._executor = BetExecutor(
            pm_trader=pm_trader,
            kalshi_trader=kalshi_trader,
            stake_usd=self._stake_usd,
            dry_run=self._dry_run,
        )

        # Kalshi feed for resolution checks (unauthenticated)
        self._kalshi_feed = None
        self._pm_feed = None

    def run(self) -> None:
        print(f"[bot] Starting volatility_bot | dry_run={self._dry_run} | stake=${self._stake_usd}")
        while True:
            try:
                self._scanner.scan_and_subscribe()
            except Exception as exc:
                print(f"[bot] scan error: {exc}")
            try:
                self._resolve_expired()
            except Exception as exc:
                print(f"[bot] resolve error: {exc}")
            time.sleep(self._scan_interval)

    def scan_once(self) -> None:
        """Single scan cycle — for the 'scan' CLI command."""
        markets = self._scanner.scan_and_subscribe()
        print(f"\nActive markets: {len(self._scanner.all_markets())}")
        for m in self._scanner.all_markets():
            print(
                f"  [{m.venue:12s}] {m.symbol:4s} {m.interval_minutes:2d}m "
                f"yes_ask={m.yes_ask:.3f} no_ask={m.no_ask:.3f} "
                f"expiry={m.expiry.strftime('%H:%M')} vol={m.volume:.0f}"
            )

    # ── Price callback (called from WS threads) ───────────────────────────

    def _on_price_update(self, market: VolatilityMarket, side: str, best_ask: float) -> None:
        now = datetime.utcnow()

        # Skip if market window hasn't started or already expired
        if now < market.market_start or now >= market.expiry:
            return

        bucket = evaluate_signal(market, side, best_ask, now, self._buckets)
        if bucket is None:
            return

        key = (market.venue, market.market_id, side, bucket)

        with self._placed_lock:
            if key in self._placed:
                return
            # Mark immediately before placing to prevent race conditions
            self._placed.add(key)

        # Secondary DB check (handles restarts)
        if self._db.has_bet(market.venue, market.market_id, side, bucket):
            return

        result = self._executor.place_bet(market, side, best_ask)

        if not result.success and not result.is_paper:
            with self._placed_lock:
                self._placed.discard(key)
            print(
                f"[bot] bet FAILED | {market.venue} {market.symbol} {side} "
                f"{bucket} @{best_ask:.3f}: {result.error}"
            )
            return

        position_pct = compute_position_pct(now, market.market_start, market.interval_minutes)
        market_minute = compute_market_minute(now, market.market_start)
        market_quarter = compute_market_quarter(position_pct)
        entry_price = result.fill_price if result.fill_price > 0 else best_ask
        shares = result.shares
        total_cost = shares * entry_price + result.fee

        bet = Bet(
            id=str(uuid.uuid4()),
            venue=market.venue,
            market_id=market.market_id,
            symbol=market.symbol,
            interval_minutes=market.interval_minutes,
            market_start=market.market_start,
            market_end=market.expiry,
            opened_at=now,
            market_minute=market_minute,
            market_quarter=market_quarter,
            position_pct=round(position_pct, 4),
            side=side,
            entry_price=entry_price,
            trigger_bucket=bucket,
            shares=shares,
            total_cost=total_cost,
            order_id=result.order_id,
            order_status=result.order_status,
            order_fill_price=result.fill_price,
            order_fee=result.fee,
            order_latency_ms=result.latency_ms,
            status="paper" if result.is_paper else "open",
            is_paper=1 if result.is_paper else 0,
        )
        self._db.record_bet(bet)
        self._db.audit("bet_placed", bet.id, {
            "venue": bet.venue,
            "symbol": bet.symbol,
            "side": side,
            "bucket": bucket,
            "ask": best_ask,
            "entry_price": entry_price,
            "quarter": market_quarter,
            "minute": market_minute,
            "pct": round(position_pct, 3),
            "dry_run": self._dry_run,
        })
        tag = "[PAPER]" if result.is_paper else "[LIVE]"
        print(
            f"[bot]{tag} bet | {market.venue} {market.symbol} {market.interval_minutes}m "
            f"{side} {bucket} @{entry_price:.3f} | Q{market_quarter} min={market_minute} "
            f"| shares={shares:.2f} cost=${total_cost:.2f}"
        )

    # ── Resolution ────────────────────────────────────────────────────────

    def _resolve_expired(self) -> None:
        now = datetime.utcnow()
        for bet in self._db.get_open_bets():
            if bet.market_end and bet.market_end <= now:
                self._resolve_one(bet)

    def _resolve_one(self, bet: Bet) -> None:
        if bet.venue == "kalshi":
            result = self._check_kalshi(bet.market_id)
        else:
            result = self._check_polymarket(bet.market_id)

        if result is None:
            return

        won = result == bet.side
        pnl = (bet.shares - bet.total_cost) if won else -bet.total_cost

        self._db.resolve_bet(bet.id, result, round(pnl, 6))
        self._db.audit("bet_resolved", bet.id, {
            "venue": bet.venue,
            "symbol": bet.symbol,
            "side": bet.side,
            "winning_side": result,
            "won": won,
            "pnl": round(pnl, 4),
        })
        tag = "WIN" if won else "LOSE"
        print(
            f"[bot][resolve] {bet.venue} {bet.symbol} {bet.side} → {result} "
            f"| {tag} | pnl=${pnl:+.2f}"
        )

    def _check_kalshi(self, ticker: str) -> Optional[str]:
        try:
            import httpx
            resp = httpx.get(
                f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}",
                timeout=10.0,
            )
            resp.raise_for_status()
            market = resp.json().get("market", {})
            result = str(market.get("result") or "").lower()
            if result in {"yes", "no"}:
                return result
        except Exception as exc:
            print(f"[resolve] Kalshi check {ticker}: {exc}")
        return None

    def _check_polymarket(self, market_id: str) -> Optional[str]:
        try:
            import httpx
            resp = httpx.get(
                f"https://gamma-api.polymarket.com/markets/{market_id}",
                timeout=10.0,
            )
            resp.raise_for_status()
            data = resp.json()
            outcomes = data.get("outcomes") or []
            prices = data.get("outcomePrices") or []
            if not data.get("closed"):
                return None
            outcome_map = {}
            for name, price in zip(outcomes, prices):
                try:
                    outcome_map[name.lower()] = float(price)
                except (TypeError, ValueError):
                    pass
            up_p = outcome_map.get("up", 0.0)
            down_p = outcome_map.get("down", 0.0)
            if up_p >= 0.95 and down_p <= 0.05:
                return "yes"
            if down_p >= 0.95 and up_p <= 0.05:
                return "no"
        except Exception as exc:
            print(f"[resolve] PM check {market_id}: {exc}")
        return None

    # ── Helpers ───────────────────────────────────────────────────────────

    def _load_placed_from_db(self) -> None:
        for bet in self._db.get_open_bets():
            key = (bet.venue, bet.market_id, bet.side, bet.trigger_bucket)
            self._placed.add(key)
        print(f"[bot] Loaded {len(self._placed)} open bets into dedup cache")
