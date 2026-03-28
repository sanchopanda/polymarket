from __future__ import annotations

import json
import math
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional

import httpx

from arb_bot.kalshi_ws import KalshiTopOfBook, KalshiWebSocketClient
from arb_bot.ws import MarketWebSocketClient, TopOfBook
from sports_arb_bot.db import SportsArbDB
from sports_arb_bot.feed_kalshi import KalshiSportsFeed
from sports_arb_bot.feed_polymarket import PolymarketSportsFeed
from sports_arb_bot.models import MatchedSportsPair
from sports_arb_bot.sport_matcher import TennisMatcher
from src.api.clob import ClobClient

if TYPE_CHECKING:
    from sports_arb_bot.telegram_notify import SportsTelegramNotifier

PM_MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_BASE = "https://gamma-api.polymarket.com"

# Kalshi series по видам спорта
KALSHI_SERIES_BY_SPORT: dict[str, list[str]] = {
    "wta": ["KXWTACHALLENGERMATCH"],
    "atp": ["KXATPCHALLENGERMATCH", "KXATPMATCH"],
    "r6": ["KXR6GAME"],
    "ipl": ["KXIPLGAME"],
    "t20": ["KXT20MATCH"],
    "cwbb": ["KXNCAAWBGAME"],
    "cbb": ["KXNCAAMBGAME"],
}


@dataclass
class WatchedSportsPair:
    pair: MatchedSportsPair
    pair_key: str
    # PM token_id → player name
    pm_token_map: dict[str, str]
    # Kalshi ticker → player name
    ka_ticker_map: dict[str, str]
    match_over: bool = False


class SportsArbWatchRunner:
    def __init__(
        self,
        config: dict,
        db: SportsArbDB,
        tg: Optional["SportsTelegramNotifier"] = None,
    ) -> None:
        self.config = config
        self.db = db
        self.tg = tg

        trading = config.get("trading", {})
        self.stake_usd: float = float(trading.get("stake_usd", 60.0))
        self.min_edge: float = float(trading.get("min_edge", 0.02))
        self.min_leg_price: float = float(trading.get("min_leg_price", 0.05))
        self.max_leg_price: float = float(trading.get("max_leg_price", 0.95))
        self.scan_interval: float = float(trading.get("scan_interval_seconds", 900))

        runtime = config.get("runtime", {})
        self.status_interval: float = float(runtime.get("status_interval_seconds", 30))
        self.resolve_interval: float = float(runtime.get("resolve_interval_seconds", 300))

        self.sports: list[str] = config.get("sports", ["wta", "atp"])

        pm_cfg = config.get("polymarket", {})
        ka_cfg = config.get("kalshi", {})
        self.clob = ClobClient(pm_cfg.get("clob_host", "https://clob.polymarket.com"))
        self._ka_base: str = ka_cfg.get("base_url", "https://api.elections.kalshi.com/trade-api/v2")
        self._http = httpx.Client(timeout=15.0)
        self._ka_http = httpx.Client(timeout=15.0)

        # Live price caches (updated from WS)
        self.live_books: dict[str, TopOfBook] = {}
        self.live_books_kalshi: dict[str, KalshiTopOfBook] = {}

        # Watchlist
        self.watchlist: dict[str, WatchedSportsPair] = {}
        self.pairs_by_pm_token: dict[str, set[str]] = {}
        self.pairs_by_ka_ticker: dict[str, set[str]] = {}

        # WS handles
        self._pm_ws: Optional[MarketWebSocketClient] = None
        self._ka_ws: Optional[KalshiWebSocketClient] = None
        self._current_pm_tokens: list[str] = []
        self._current_ka_tickers: list[str] = []

        self._signal_lock = threading.Lock()
        self._last_scan_ts: float = 0.0
        self._last_status_ts: float = 0.0
        self._last_resolve_ts: float = 0.0
        # For change-detection in status
        self._last_status_snapshot: tuple = ()
        # Per-pair cooldown: don't bet more than once per minute per pair
        self._last_bet_ts: dict[str, float] = {}
        self._bet_cooldown: float = 60.0

    # ── Main loop ───────────────────────────────────────────────────────

    def run(self) -> None:
        print("[sports-arb] Бот запущен. Первый скан...")
        try:
            while True:
                now = time.time()
                if now - self._last_scan_ts >= self.scan_interval:
                    t0 = time.time()
                    self._refresh_watchlist()
                    elapsed = time.time() - t0
                    self._last_scan_ts = time.time()
                    next_scan = datetime.fromtimestamp(
                        self._last_scan_ts + self.scan_interval, tz=timezone.utc
                    ).strftime("%H:%M UTC")
                    print(f"[sports-arb] Скан завершён за {elapsed:.1f}с. Следующий в {next_scan}")
                if now - self._last_status_ts >= self.status_interval:
                    self._print_status()
                    self._last_status_ts = now
                if now - self._last_resolve_ts >= self.resolve_interval:
                    self._resolve_expired()
                    self._last_resolve_ts = now
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[sports-arb] Остановлен.")
        finally:
            self._stop_ws()

    def _stop_ws(self) -> None:
        if self._pm_ws:
            self._pm_ws.stop()
            self._pm_ws = None
        if self._ka_ws:
            self._ka_ws.stop()
            self._ka_ws = None

    # ── Refresh watchlist ───────────────────────────────────────────────

    def _get_ka_series(self) -> list[str]:
        series: list[str] = []
        for sport in self.sports:
            series.extend(KALSHI_SERIES_BY_SPORT.get(sport, []))
        return series

    def _refresh_watchlist(self) -> None:
        now_utc = datetime.now(tz=timezone.utc)
        pm_since = now_utc - timedelta(hours=1, minutes=45)
        pm_to = now_utc + timedelta(hours=5)
        ka_since = now_utc + timedelta(minutes=15)
        ka_to = now_utc + timedelta(hours=7)
        print(
            f"[sports-arb] Скан | "
            f"PM gameStart {pm_since.strftime('%H:%M')}–{pm_to.strftime('%H:%M')} | "
            f"Kalshi expiry {ka_since.strftime('%H:%M')}–{ka_to.strftime('%H:%M UTC')}"
        )

        pm_events = PolymarketSportsFeed().fetch(sports=self.sports)
        print(f"  PM: {len(pm_events)} событий")

        ka_events = KalshiSportsFeed().fetch(series_tickers=self._get_ka_series())
        print(f"  Kalshi: {len(ka_events)} событий")

        matches = TennisMatcher().match(pm_events, ka_events)
        print(f"  Матчей: {len(matches)}")

        current_pair_keys = {
            f"{p.pm_event.slug}:{p.kalshi_event.event_ticker}"
            for p in matches
        }

        # Add new pairs
        new_pair_descriptions: list[str] = []
        for pair in matches:
            pair_key = f"{pair.pm_event.slug}:{pair.kalshi_event.event_ticker}"
            if pair_key in self.watchlist:
                continue

            pm_token_map = {
                tid: player
                for player, tid in zip(pair.pm_event.players, pair.pm_event.token_ids)
            }
            ka_ticker_map = {
                m.ticker: m.player_name
                for m in pair.kalshi_event.markets
            }
            wp = WatchedSportsPair(
                pair=pair,
                pair_key=pair_key,
                pm_token_map=pm_token_map,
                ka_ticker_map=ka_ticker_map,
            )
            self.watchlist[pair_key] = wp
            for token_id in pm_token_map:
                self.pairs_by_pm_token.setdefault(token_id, set()).add(pair_key)
            for ticker in ka_ticker_map:
                self.pairs_by_ka_ticker.setdefault(ticker, set()).add(pair_key)

            self.db.upsert_matched_pair(
                pair_key=pair_key,
                sport=pair.sport,
                pm_slug=pair.pm_event.slug,
                pm_title=pair.pm_event.title,
                ka_event_ticker=pair.kalshi_event.event_ticker,
                ka_title=pair.kalshi_event.title,
                player_a=pair.pm_event.players[0],
                player_b=pair.pm_event.players[1],
                match_confidence=pair.match_result.confidence,
                game_date=pair.pm_event.game_date,
            )

            # Verbose log: show player names and initial prices
            players_str = " vs ".join(pair.pm_event.players)
            prices_str = " / ".join(f"{p:.2f}" for p in pair.pm_event.prices)
            ka_prices_str = " / ".join(
                f"{m.player_name}={m.yes_ask:.2f}"
                for m in pair.kalshi_event.markets
            )
            game_dt = pair.pm_event.game_date.strftime("%m-%d %H:%M")
            print(
                f"  + [{pair.sport.upper()}] {players_str} | {game_dt} UTC\n"
                f"    PM ({prices_str}) ↔ Kalshi ({ka_prices_str})\n"
                f"    {pair_key}"
            )
            new_pair_descriptions.append(
                f"[{pair.sport.upper()}] {players_str} | {game_dt}"
            )

        if self.tg and new_pair_descriptions:
            self.tg.notify_scan(new_pair_descriptions, len(self.watchlist))

        # Remove pairs that are gone or match_over
        to_remove = [
            pk for pk, wp in self.watchlist.items()
            if wp.match_over or pk not in current_pair_keys
        ]
        for pk in to_remove:
            self._remove_pair(pk)

        # Recompute subscription sets
        all_pm_tokens = sorted({
            tid for wp in self.watchlist.values() for tid in wp.pm_token_map
        })
        all_ka_tickers = sorted({
            ticker for wp in self.watchlist.values() for ticker in wp.ka_ticker_map
        })

        # Restart PM WS if set changed
        if all_pm_tokens != self._current_pm_tokens:
            if self._pm_ws:
                self._pm_ws.stop()
            self._current_pm_tokens = all_pm_tokens
            if all_pm_tokens:
                self._pm_ws = MarketWebSocketClient(
                    url=PM_MARKET_WS_URL,
                    asset_ids=all_pm_tokens,
                    on_message=self.on_pm_message,
                )
                self._pm_ws.start()
                print(f"  PM WS: {len(all_pm_tokens)} токенов")
            else:
                self._pm_ws = None
                print("  PM WS: нет токенов")

        # Restart Kalshi WS if set changed
        if all_ka_tickers != self._current_ka_tickers:
            if self._ka_ws:
                self._ka_ws.stop()
            self._current_ka_tickers = all_ka_tickers
            if all_ka_tickers:
                try:
                    self._ka_ws = KalshiWebSocketClient(
                        tickers=all_ka_tickers,
                        on_update=self.on_kalshi_update,
                    )
                    self._ka_ws.start()
                    print(f"  Kalshi WS: {len(all_ka_tickers)} тикеров")
                except RuntimeError as e:
                    print(f"  [WARN] Kalshi WS не запущен: {e}")
                    self._ka_ws = None
            else:
                self._ka_ws = None
                print("  Kalshi WS: нет тикеров")

    def _remove_pair(self, pair_key: str) -> None:
        wp = self.watchlist.pop(pair_key, None)
        if not wp:
            return
        for token_id in wp.pm_token_map:
            self.pairs_by_pm_token.get(token_id, set()).discard(pair_key)
        for ticker in wp.ka_ticker_map:
            self.pairs_by_ka_ticker.get(ticker, set()).discard(pair_key)
        print(f"  - {pair_key}")

    # ── WS callbacks ────────────────────────────────────────────────────

    def on_pm_message(self, payload: dict) -> None:
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    self.on_pm_message(item)
            return

        event_type = payload.get("event_type")
        if event_type not in {"book", "best_bid_ask"}:
            return

        asset_id = str(payload.get("asset_id", ""))
        if not asset_id:
            return

        if event_type == "book":
            asks = payload.get("asks", [])
            bids = payload.get("bids", [])
            best_ask = float(asks[0]["price"]) if asks else 0.0
            best_bid = float(bids[0]["price"]) if bids else 0.0
            timestamp = int(payload.get("timestamp", 0) or 0)
        else:
            best_ask = float(payload.get("best_ask", 0) or 0)
            best_bid = float(payload.get("best_bid", 0) or 0)
            timestamp = int(payload.get("timestamp", 0) or 0)

        self.live_books[asset_id] = TopOfBook(
            best_bid=best_bid, best_ask=best_ask, updated_at_ms=timestamp
        )

        for pair_key in list(self.pairs_by_pm_token.get(asset_id, set())):
            self._maybe_open_pair(pair_key)

    def on_kalshi_update(self, ticker: str, book: KalshiTopOfBook) -> None:
        self.live_books_kalshi[ticker] = book

        # Match-over signal: price collapsed to near 0 or 1
        if book.best_yes_ask >= 0.99 or (0 < book.best_yes_ask <= 0.01):
            for pair_key in list(self.pairs_by_ka_ticker.get(ticker, set())):
                wp = self.watchlist.get(pair_key)
                if wp and not wp.match_over:
                    wp.match_over = True
                    print(
                        f"[sports-arb] Матч завершён: {pair_key} "
                        f"(Kalshi {ticker} yes_ask={book.best_yes_ask:.3f})"
                    )
            return

        for pair_key in list(self.pairs_by_ka_ticker.get(ticker, set())):
            self._maybe_open_pair(pair_key)

    # ── Hot path ────────────────────────────────────────────────────────

    def _maybe_open_pair(self, pair_key: str) -> None:
        wp = self.watchlist.get(pair_key)
        if wp is None or wp.match_over:
            return

        pair = wp.pair
        outcome_map = pair.match_result.outcome_map  # pm_player → ka_ticker

        if len(pair.pm_event.players) != 2:
            return
        player_a, player_b = pair.pm_event.players[0], pair.pm_event.players[1]

        ka_ticker_a = outcome_map.get(player_a)
        ka_ticker_b = outcome_map.get(player_b)
        if not ka_ticker_a or not ka_ticker_b:
            return

        # Live prices from WS cache
        pm_book_a = self.live_books.get(self._token_for(wp, player_a))
        pm_book_b = self.live_books.get(self._token_for(wp, player_b))
        ka_book_a = self.live_books_kalshi.get(ka_ticker_a)
        ka_book_b = self.live_books_kalshi.get(ka_ticker_b)

        if not all((pm_book_a, pm_book_b, ka_book_a, ka_book_b)):
            return

        pm_ask_a = pm_book_a.best_ask
        pm_ask_b = pm_book_b.best_ask
        ka_ask_a = ka_book_a.best_yes_ask
        ka_ask_b = ka_book_b.best_yes_ask

        if not all(x > 0 for x in (pm_ask_a, pm_ask_b, ka_ask_a, ka_ask_b)):
            return

        # Two arb directions
        cost1 = pm_ask_a + ka_ask_b   # Buy A on PM + Buy B on Kalshi
        cost2 = pm_ask_b + ka_ask_a   # Buy B on PM + Buy A on Kalshi
        edge1 = 1.0 - cost1
        edge2 = 1.0 - cost2

        if edge1 >= edge2:
            best_edge, best_cost = edge1, cost1
            leg_pm_player, leg_pm_price = player_a, pm_ask_a
            leg_ka_player = player_b
            leg_ka_ticker = ka_ticker_b
            leg_ka_price = ka_ask_b
        else:
            best_edge, best_cost = edge2, cost2
            leg_pm_player, leg_pm_price = player_b, pm_ask_b
            leg_ka_player = player_a
            leg_ka_ticker = ka_ticker_a
            leg_ka_price = ka_ask_a

        if best_edge < self.min_edge:
            return
        if not (self.min_leg_price <= leg_pm_price <= self.max_leg_price):
            return
        if not (self.min_leg_price <= leg_ka_price <= self.max_leg_price):
            return

        with self._signal_lock:
            # Recheck after acquiring lock
            if best_edge < self.min_edge:
                return

            # Per-pair cooldown: no more than once per minute
            # Updated here (before depth check) so SKIPs also consume the cooldown
            now_ts = time.time()
            if now_ts - self._last_bet_ts.get(pair_key, 0.0) < self._bet_cooldown:
                return
            self._last_bet_ts[pair_key] = now_ts

            pm_token_id = self._token_for(wp, leg_pm_player)
            if not pm_token_id:
                return

            # Orderbook depth snapshot
            pm_depth = self._pm_depth_at_ask(pm_token_id, leg_pm_price)
            ka_depth = self._ka_depth_at_ask(leg_ka_ticker, leg_ka_price)

            self.db.save_orderbook_snapshot(
                sport=pair.sport,
                pm_slug=pair.pm_event.slug,
                pm_token_id=pm_token_id,
                pm_player=leg_pm_player,
                pm_best_ask=leg_pm_price,
                pm_ask_depth_usd=pm_depth,
                ka_ticker=leg_ka_ticker,
                ka_player=leg_ka_player,
                ka_yes_ask=leg_ka_price,
                ka_ask_depth_usd=ka_depth,
            )

            # lock_valid: depth sufficient for full stake on both legs
            min_depth = self.stake_usd * 0.8
            lock_valid = (
                pm_depth is not None and pm_depth >= min_depth and
                ka_depth is not None and ka_depth >= min_depth
            )

            # Skip trade if depth is insufficient
            if not lock_valid:
                print(
                    f"[sports-arb] SKIP {pair_key} | depth insufficient "
                    f"pm=${pm_depth or 0:.0f} ka=${ka_depth or 0:.0f}"
                )
                return

            shares = math.floor(self.stake_usd / best_cost)
            if shares < 1:
                return

            pos_id = self.db.open_position(
                sport=pair.sport,
                pm_slug=pair.pm_event.slug,
                pm_title=pair.pm_event.title,
                pm_market_id=pair.pm_event.market_id,
                ka_event_ticker=pair.kalshi_event.event_ticker,
                ka_title=pair.kalshi_event.title,
                match_confidence=pair.match_result.confidence,
                player_a=player_a,
                player_b=player_b,
                leg_pm_player=leg_pm_player,
                leg_pm_token_id=pm_token_id,
                leg_pm_price=leg_pm_price,
                leg_ka_player=leg_ka_player,
                leg_ka_ticker=leg_ka_ticker,
                leg_ka_price=leg_ka_price,
                cost=best_cost,
                edge=best_edge,
                shares=shares,
                game_date=pair.pm_event.game_date,
                lock_valid=lock_valid,
            )

            total_cost = shares * best_cost
            expected_profit = shares * best_edge
            depth_str = (
                f"pm_depth=${pm_depth:.0f} ka_depth=${ka_depth:.0f}"
                if pm_depth is not None and ka_depth is not None
                else "depth=unknown"
            )
            print(
                f"[sports-arb] PAPER BET {pos_id} | {pair.pm_event.slug}\n"
                f"  {leg_pm_player}@PM={leg_pm_price:.3f} + "
                f"{leg_ka_player}@Kalshi={leg_ka_price:.3f}\n"
                f"  edge={best_edge:.4f} cost={best_cost:.4f} "
                f"shares={shares} total=${total_cost:.2f} "
                f"profit=${expected_profit:.2f} | {depth_str} | lock_valid={lock_valid}"
            )
            if self.tg:
                self.tg.notify_bet(
                    pos_id=pos_id,
                    pm_slug=pair.pm_event.slug,
                    leg_pm_player=leg_pm_player,
                    leg_pm_price=leg_pm_price,
                    leg_ka_player=leg_ka_player,
                    leg_ka_ticker=leg_ka_ticker,
                    leg_ka_price=leg_ka_price,
                    cost=best_cost,
                    edge=best_edge,
                    shares=shares,
                    total_cost=total_cost,
                    expected_profit=expected_profit,
                    pm_depth=pm_depth,
                    ka_depth=ka_depth,
                    lock_valid=lock_valid,
                )

    def _token_for(self, wp: WatchedSportsPair, player: str) -> str:
        for token_id, name in wp.pm_token_map.items():
            if name == player:
                return token_id
        return ""

    # ── Orderbook depth ─────────────────────────────────────────────────

    def _pm_depth_at_ask(self, token_id: str, ask_price: float) -> Optional[float]:
        """Доступный объём (USD) по цене ask или лучше на PM."""
        try:
            book = self.clob.get_orderbook(token_id)
            if not book or not book.asks:
                return None
            return sum(
                level.price * level.size
                for level in book.asks
                if level.price <= ask_price + 0.005
            )
        except Exception:
            return None

    def _ka_depth_at_ask(self, ticker: str, ask_price: float) -> Optional[float]:
        """Доступный объём (USD) по цене yes_ask или лучше на Kalshi."""
        try:
            resp = self._ka_http.get(f"{self._ka_base}/markets/{ticker}/orderbook")
            resp.raise_for_status()
            data = resp.json()
            orderbook = data.get("orderbook_fp") or data.get("orderbook") or {}
            # yes_ask = 1 - no_bid; нас интересуют no_bids с price >= 1 - ask_price
            no_bids_raw = orderbook.get("no_dollars") or orderbook.get("no") or []
            min_no_bid = 1.0 - ask_price - 0.005
            depth = sum(
                (1.0 - float(item[0])) * float(item[1])
                for item in no_bids_raw
                if float(item[0]) >= min_no_bid
            )
            return depth
        except Exception:
            return None

    # ── Resolution ──────────────────────────────────────────────────────

    def resolve_expired(self) -> None:
        """Public entry point for manual --resolve command."""
        self._resolve_expired()

    def _resolve_expired(self) -> None:
        positions = self.db.get_open_positions()
        if not positions:
            return

        now = datetime.now(tz=timezone.utc)
        resolved = 0

        for pos in positions:
            game_date_raw = pos["game_date"]
            try:
                game_date = datetime.fromisoformat(game_date_raw)
                if game_date.tzinfo is None:
                    game_date = game_date.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

            if now - game_date < timedelta(hours=3):
                continue  # матч ещё может идти

            pm_winner = self._pm_winner(pos["pm_market_id"])
            ka_result = self._ka_result(pos["leg_ka_ticker"])

            if pm_winner is None or ka_result is None:
                continue  # рынок ещё не зарезолвился

            pnl = self.db.resolve_position(
                pos_id=pos["id"],
                winner=pm_winner,
                pm_result=pm_winner,
                ka_result=ka_result,
            )
            sign = "+" if pnl >= 0 else ""
            print(
                f"[sports-arb] RESOLVED {pos['id']} | "
                f"{pos['pm_slug']} | winner={pm_winner} | "
                f"pnl={sign}${pnl:.2f}"
            )
            if self.tg:
                self.tg.notify_resolve(
                    pos_id=pos["id"],
                    pm_slug=pos["pm_slug"],
                    winner=pm_winner,
                    pnl=pnl,
                )
            resolved += 1

        if resolved:
            bal = self.db.get_balance()
            print(f"[sports-arb] Баланс: ${bal['current_balance']:.2f}")

    def _pm_winner(self, market_id: str) -> Optional[str]:
        """Возвращает имя победителя или None если рынок не закрыт."""
        try:
            resp = self._http.get(f"{GAMMA_BASE}/markets/{market_id}")
            resp.raise_for_status()
            m = resp.json()
            if not m.get("closed"):
                return None
            prices_raw = m.get("outcomePrices") or []
            if isinstance(prices_raw, str):
                prices_raw = json.loads(prices_raw)
            outcomes = m.get("outcomes") or []
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            prices = [float(p) for p in prices_raw]
            if not prices or not outcomes:
                return None
            winner_idx = prices.index(max(prices))
            return outcomes[winner_idx] if winner_idx < len(outcomes) else None
        except Exception:
            return None

    def _ka_result(self, ticker: str) -> Optional[str]:
        """Возвращает 'yes' или 'no' или None если рынок не зарезолвился."""
        try:
            resp = self._ka_http.get(f"{self._ka_base}/markets/{ticker}")
            resp.raise_for_status()
            m = resp.json().get("market") or {}
            result = m.get("result")
            if result in ("yes", "no"):
                return result
            return None
        except Exception:
            return None

    # ── Status ──────────────────────────────────────────────────────────

    def _get_status_text(self) -> str:
        bal = self.db.get_balance()
        open_pos = self.db.get_open_positions()
        ts = datetime.now(tz=timezone.utc).strftime("%H:%M UTC")
        net = bal["current_balance"] - bal["initial_balance"]
        sign = "+" if net >= 0 else ""

        lines = [
            f"🎾 <b>sports_arb_bot</b> [{ts}]",
            f"Пар: {len(self.watchlist)} | Открытых: {len(open_pos)}",
            f"Баланс: <b>${bal['current_balance']:.2f}</b> ({sign}${net:.2f})",
            f"Ставок: ${bal['total_wagered']:.2f} | "
            f"Выиграно: ${bal['total_won']:.2f} | "
            f"Проиграно: ${bal['total_lost']:.2f}",
        ]
        if open_pos:
            lines.append("")
            lines.append("<b>Открытые позиции:</b>")
            for p in open_pos:
                lines.append(
                    f"  {p['id']} {p['pm_slug'][:30]}\n"
                    f"    edge={p['edge']:.3f} ${p['total_cost']:.2f} | "
                    f"{p['opened_at'][5:16]}"
                )
        return "\n".join(lines)

    def _print_status(self) -> None:
        bal = self.db.get_balance()
        open_pos = self.db.get_open_positions()
        snapshot = (
            len(self.watchlist),
            len(open_pos),
            round(float(bal["current_balance"]), 2),
            round(float(bal["total_wagered"]), 2),
        )
        if snapshot == self._last_status_snapshot:
            return  # ничего не изменилось — не спамим
        self._last_status_snapshot = snapshot

        import re
        clean = re.sub(r"<[^>]+>", "", self._get_status_text())
        print(clean)
