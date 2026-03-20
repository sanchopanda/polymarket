from __future__ import annotations

import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.api.clob import ClobClient, OrderBook
from src.api.gamma import GammaClient, Market

from arb_bot.db import ArbBotDB
from arb_bot.ws import MarketWebSocketClient, TopOfBook


@dataclass
class StrategySettings:
    market_query: str
    category: str
    fee_type: str
    min_days_to_expiry: float
    max_days_to_expiry: float
    min_volume: float
    min_liquidity: float
    max_open_positions: int
    min_edge: float
    max_payout_per_trade: float
    min_top_book_shares: float


@dataclass
class TradingSettings:
    starting_balance: float
    taker_fee: float


@dataclass
class ApiSettings:
    gamma_base_url: str
    clob_base_url: str
    market_ws_url: str
    page_size: int
    request_delay_ms: int


@dataclass
class PairQuote:
    market: Market
    outcome_a: str
    outcome_b: str
    token_a: str
    token_b: str
    shares: float
    avg_price_a: float
    avg_price_b: float
    gross_cost: float
    fee_cost: float
    total_cost: float
    expected_payout: float
    edge: float


@dataclass
class RoughOpportunity:
    market_id: str
    question: str
    outcome_a: str
    outcome_b: str
    ask_sum: float
    rough_edge_per_share: float
    updated_at_ms: int


QUERY_ALIASES: dict[str, tuple[str, ...]] = {
    "btc": ("btc", "bitcoin"),
    "eth": ("eth", "ethereum"),
    "sol": ("sol", "solana"),
    "xrp": ("xrp",),
    "bnb": ("bnb",),
    "doge": ("doge", "dogecoin"),
    "hype": ("hype", "hyperliquid"),
}


def fill_cost_for_shares(book: OrderBook | None, target_shares: float) -> tuple[float, float]:
    if book is None or target_shares <= 0:
        return 0.0, 0.0

    remaining = target_shares
    spent = 0.0
    filled = 0.0
    for level in book.asks:
        take = min(level.size, remaining)
        if take <= 0:
            continue
        spent += take * level.price
        filled += take
        remaining -= take
        if remaining <= 1e-9:
            break
    return spent, filled


class ArbBotEngine:
    SNAPSHOT_INTERVAL_SECONDS = 15
    RESOLUTION_CHECK_INTERVAL_SECONDS = 20
    UNIVERSE_REFRESH_INTERVAL_SECONDS = 60

    def __init__(self, config: dict, db: ArbBotDB) -> None:
        self.config = config
        self.db = db

        strategy = config["strategy"]
        trading = config["trading"]
        api = config["api"]

        self.strategy = StrategySettings(**strategy)
        self.trading = TradingSettings(**trading)
        self.api = ApiSettings(**api)
        self.gamma = GammaClient(
            base_url=self.api.gamma_base_url,
            page_size=self.api.page_size,
            delay_ms=self.api.request_delay_ms,
        )
        self.clob = ClobClient(
            base_url=self.api.clob_base_url,
            delay_ms=self.api.request_delay_ms,
        )
        self.live_books: dict[str, TopOfBook] = {}
        self.live_markets: dict[str, Market] = {}
        self.market_by_asset: dict[str, str] = {}
        self.outcome_by_asset: dict[str, str] = {}
        self._signal_lock = threading.Lock()
        self._last_snapshot_at = 0.0
        self._last_resolution_check_at = 0.0
        self._last_universe_refresh_at = 0.0

    def free_balance(self) -> float:
        stats = self.db.stats()
        return self.trading.starting_balance + stats["realized_pnl"] - stats["open_cost"]

    def scan_quotes(self) -> list[PairQuote]:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        min_expiry = now + timedelta(days=self.strategy.min_days_to_expiry)
        max_expiry = now + timedelta(days=self.strategy.max_days_to_expiry)

        print(
            f"[Scan] Загрузка рынков query={self.strategy.market_query!r}, "
            f"горизонт {self.strategy.min_days_to_expiry}-{self.strategy.max_days_to_expiry}д..."
        )
        markets = self.gamma.fetch_all_active_markets()
        candidates: list[PairQuote] = []

        for market in markets:
            if not self._is_market_eligible(market, now, min_expiry, max_expiry):
                continue

            book_a = self.clob.get_orderbook(market.clob_token_ids[0])
            book_b = self.clob.get_orderbook(market.clob_token_ids[1])
            if not book_a or not book_a.asks or not book_b or not book_b.asks:
                continue

            top_shares = min(book_a.asks[0].size, book_b.asks[0].size)
            if top_shares < self.strategy.min_top_book_shares:
                continue

            target_shares = min(top_shares, self.strategy.max_payout_per_trade)
            if target_shares <= 0:
                continue

            gross_cost_a, filled_a = fill_cost_for_shares(book_a, target_shares)
            gross_cost_b, filled_b = fill_cost_for_shares(book_b, target_shares)
            paired_shares = min(filled_a, filled_b)
            if paired_shares <= 0:
                continue

            if paired_shares < target_shares:
                gross_cost_a, filled_a = fill_cost_for_shares(book_a, paired_shares)
                gross_cost_b, filled_b = fill_cost_for_shares(book_b, paired_shares)
                paired_shares = min(filled_a, filled_b)

            if paired_shares <= 0:
                continue

            gross_cost = gross_cost_a + gross_cost_b
            fee_cost = gross_cost * self.trading.taker_fee
            total_cost = gross_cost + fee_cost
            payout = paired_shares
            edge = payout - total_cost
            if edge < self.strategy.min_edge:
                continue

            candidates.append(
                PairQuote(
                    market=market,
                    outcome_a=market.outcomes[0],
                    outcome_b=market.outcomes[1],
                    token_a=market.clob_token_ids[0],
                    token_b=market.clob_token_ids[1],
                    shares=paired_shares,
                    avg_price_a=(gross_cost_a / paired_shares),
                    avg_price_b=(gross_cost_b / paired_shares),
                    gross_cost=gross_cost,
                    fee_cost=fee_cost,
                    total_cost=total_cost,
                    expected_payout=payout,
                    edge=edge,
                )
            )

        candidates.sort(key=lambda item: item.edge, reverse=True)
        return candidates

    def run_scan(self, dry_run: bool = False) -> None:
        open_positions = self.db.get_open_positions()
        free_balance = self.free_balance()
        print(
            f"[Scan] Открыто позиций: {len(open_positions)}/{self.strategy.max_open_positions} "
            f"| Свободно: ${free_balance:.2f}"
        )

        candidates = self.scan_quotes()
        print(f"[Scan] Найдено арбитражных кандидатов: {len(candidates)}")
        placed = 0

        for quote in candidates:
            if len(open_positions) + placed >= self.strategy.max_open_positions:
                break
            if self.db.has_open_position(quote.market.id):
                continue
            if free_balance < quote.total_cost:
                continue

            print(
                f"  {'[DRY]' if dry_run else '[PAIR]'} {quote.market.question[:70]}\n"
                f"        {quote.outcome_a}/{quote.outcome_b} | shares={quote.shares:.2f} "
                f"| ask_sum={quote.avg_price_a + quote.avg_price_b:.4f} "
                f"| total_cost=${quote.total_cost:.2f} | edge=${quote.edge:.2f}"
            )

            if dry_run:
                placed += 1
                continue

            position = self.db.create_position(
                market_id=quote.market.id,
                question=quote.market.question,
                outcome_a=quote.outcome_a,
                outcome_b=quote.outcome_b,
                token_a=quote.token_a,
                token_b=quote.token_b,
                shares=quote.shares,
                avg_price_a=quote.avg_price_a,
                avg_price_b=quote.avg_price_b,
                gross_cost=quote.gross_cost,
                fee_cost=quote.fee_cost,
                total_cost=quote.total_cost,
                expected_payout=quote.expected_payout,
                expected_edge=quote.edge,
                placed_at=datetime.now(timezone.utc).replace(tzinfo=None),
                end_date=quote.market.end_date,
            )
            self.db.save_position(position)
            free_balance -= quote.total_cost
            placed += 1

        print(f"[Scan] Размещено позиций: {placed}")

    def run_websocket(self) -> None:
        ws: MarketWebSocketClient | None = None
        try:
            while True:
                now_ts = time.time()
                if ws is None or (now_ts - self._last_universe_refresh_at) >= self.UNIVERSE_REFRESH_INTERVAL_SECONDS:
                    if ws is not None:
                        ws.stop()
                    ws = self._start_live_feed()
                    self._last_universe_refresh_at = now_ts

                if (now_ts - self._last_resolution_check_at) >= self.RESOLUTION_CHECK_INTERVAL_SECONDS:
                    self.check_resolutions(quiet_if_empty=True)
                    self._last_resolution_check_at = now_ts

                if (now_ts - self._last_snapshot_at) >= self.SNAPSHOT_INTERVAL_SECONDS:
                    self.print_live_snapshot()
                    self._last_snapshot_at = now_ts

                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[WS] Остановлено.")
        finally:
            if ws is not None:
                ws.stop()

    def check_resolutions(self, quiet_if_empty: bool = False) -> None:
        open_positions = self.db.get_open_positions()
        if not open_positions:
            if not quiet_if_empty:
                print("[Resolve] Открытых позиций нет.")
            return

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        resolved = 0

        for position in open_positions:
            market = self.gamma.fetch_market(position.market_id)
            if market is None:
                continue
            if not market.closed and (market.end_date is None or market.end_date > now):
                continue

            winning_outcome = self._winning_outcome(market)
            if winning_outcome is None:
                continue

            self.db.resolve_position(position.id, winning_outcome)
            stored = self.db.get_position(position.id)
            print(
                f"[Resolve] {position.question[:70]}\n"
                f"          winner={winning_outcome} | pnl=${stored.pnl:+.2f}"
            )
            resolved += 1

        print(f"[Resolve] Зарезолвлено позиций: {resolved}")

    def print_status(self) -> None:
        stats = self.db.stats()
        equity = self.trading.starting_balance + stats["realized_pnl"]
        free_balance = self.free_balance()
        print(
            f"Стартовый баланс: ${self.trading.starting_balance:.2f}\n"
            f"Реализованный P&L: ${stats['realized_pnl']:+.2f}\n"
            f"Открыто позиций: {stats['open']}\n"
            f"Зарезолвлено: {stats['resolved']}\n"
            f"В работе: ${stats['open_cost']:.2f}\n"
            f"Equity: ${equity:.2f}\n"
            f"Свободный баланс: ${free_balance:.2f}"
        )

        open_positions = self.db.get_open_positions()
        if not open_positions:
            return

        print("\nОткрытые позиции:")
        for position in open_positions:
            ask_sum = position.avg_price_a + position.avg_price_b
            print(
                f"  {position.question[:70]}\n"
                f"    {position.outcome_a}/{position.outcome_b} | shares={position.shares:.2f} "
                f"| ask_sum={ask_sum:.4f} | edge=${position.expected_edge:.2f}"
            )

    def print_live_snapshot(self) -> None:
        stats = self.db.stats()
        print(
            f"[WS][Status] рынков={len(self.live_markets)} | токенов={len(self.market_by_asset)} "
            f"| открыто={stats['open']} | realized=${stats['realized_pnl']:+.2f} "
            f"| free=${self.free_balance():.2f}"
        )

        opportunities = self.get_live_opportunities(limit=5)
        if not opportunities:
            print("[WS][Status] Сейчас rough-арбитражных окон по top-of-book нет.")
            return

        print("[WS][Status] Лучшие live-кандидаты:")
        for item in opportunities:
            print(
                f"  {item.question[:72]}\n"
                f"    {item.outcome_a}/{item.outcome_b} | ask_sum={item.ask_sum:.4f} "
                f"| rough_edge/share=${item.rough_edge_per_share:.4f}"
            )

    def on_ws_message(self, payload: dict) -> None:
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    self.on_ws_message(item)
            return

        event_type = payload.get("event_type")
        if event_type == "book":
            asset_id = str(payload.get("asset_id", ""))
            asks = payload.get("asks", [])
            bids = payload.get("bids", [])
            best_ask = float(asks[0]["price"]) if asks else 0.0
            best_bid = float(bids[0]["price"]) if bids else 0.0
            timestamp = int(payload.get("timestamp", 0) or 0)
            self.live_books[asset_id] = TopOfBook(best_bid=best_bid, best_ask=best_ask, updated_at_ms=timestamp)
            self._maybe_open_live_position(asset_id)
            return

        if event_type == "best_bid_ask":
            asset_id = str(payload.get("asset_id", ""))
            timestamp = int(payload.get("timestamp", 0) or 0)
            self.live_books[asset_id] = TopOfBook(
                best_bid=float(payload.get("best_bid", 0) or 0),
                best_ask=float(payload.get("best_ask", 0) or 0),
                updated_at_ms=timestamp,
            )
            self._maybe_open_live_position(asset_id)
            return

        if event_type == "market_resolved":
            market_id = str(payload.get("id") or self.market_by_asset.get(str(payload.get("winning_asset_id", "")), ""))
            if not market_id:
                return
            self._resolve_market_by_ws(market_id, str(payload.get("winning_outcome", "")))

    def get_live_opportunities(self, limit: int = 5) -> list[RoughOpportunity]:
        items: list[RoughOpportunity] = []
        seen_markets: set[str] = set()
        for market in self.live_markets.values():
            if market.id in seen_markets:
                continue
            if len(market.clob_token_ids) != 2:
                continue
            book_a = self.live_books.get(market.clob_token_ids[0])
            book_b = self.live_books.get(market.clob_token_ids[1])
            if book_a is None or book_b is None:
                continue
            if book_a.best_ask <= 0 or book_b.best_ask <= 0:
                continue
            ask_sum = book_a.best_ask + book_b.best_ask
            rough_edge = 1.0 - ask_sum
            if rough_edge <= 0:
                continue
            items.append(
                RoughOpportunity(
                    market_id=market.id,
                    question=market.question,
                    outcome_a=market.outcomes[0],
                    outcome_b=market.outcomes[1],
                    ask_sum=ask_sum,
                    rough_edge_per_share=rough_edge,
                    updated_at_ms=max(book_a.updated_at_ms, book_b.updated_at_ms),
                )
            )
            seen_markets.add(market.id)

        items.sort(key=lambda item: item.rough_edge_per_share, reverse=True)
        return items[:limit]

    def _load_live_market_universe(self) -> list[Market]:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        min_expiry = now + timedelta(days=self.strategy.min_days_to_expiry)
        max_expiry = now + timedelta(days=self.strategy.max_days_to_expiry)
        markets = self.gamma.fetch_all_active_markets()
        eligible = [
            market
            for market in markets
            if self._is_market_eligible(market, now, min_expiry, max_expiry)
        ]
        self.live_markets = {market.id: market for market in eligible}
        self.market_by_asset = {}
        self.outcome_by_asset = {}
        for market in eligible:
            for token_id, outcome in zip(market.clob_token_ids, market.outcomes):
                self.market_by_asset[token_id] = market.id
                self.outcome_by_asset[token_id] = outcome
        return eligible

    def _start_live_feed(self) -> MarketWebSocketClient | None:
        markets = self._load_live_market_universe()
        if not markets:
            print("[WS] Подходящих рынков для подписки нет.")
            return None

        asset_ids = [asset_id for market in markets for asset_id in market.clob_token_ids]
        print(f"[WS] Подписка на {len(markets)} рынков / {len(asset_ids)} токенов...")
        for market in markets[:10]:
            print(f"  [WS][Market] {market.question[:90]}")
        if len(markets) > 10:
            print(f"  [WS][Market] ... и еще {len(markets) - 10} рынков")

        ws = MarketWebSocketClient(
            url=self.api.market_ws_url,
            asset_ids=asset_ids,
            on_message=self.on_ws_message,
        )
        ws.start()
        return ws

    def _maybe_open_live_position(self, asset_id: str) -> None:
        market_id = self.market_by_asset.get(asset_id)
        if not market_id:
            return
        market = self.live_markets.get(market_id)
        if market is None or self.db.has_open_position(market_id):
            return

        if len(self.db.get_open_positions()) >= self.strategy.max_open_positions:
            return

        book_a = self.live_books.get(market.clob_token_ids[0])
        book_b = self.live_books.get(market.clob_token_ids[1])
        if book_a is None or book_b is None:
            return
        if book_a.best_ask <= 0 or book_b.best_ask <= 0:
            return

        rough_total = book_a.best_ask + book_b.best_ask
        if (1.0 - rough_total) < self.strategy.min_edge:
            return

        with self._signal_lock:
            if self.db.has_open_position(market_id):
                return
            quote = self._build_quote_from_rest(market)
            if quote is None:
                return
            if quote.edge < self.strategy.min_edge:
                return
            if self.free_balance() < quote.total_cost:
                return

            position = self.db.create_position(
                market_id=quote.market.id,
                question=quote.market.question,
                outcome_a=quote.outcome_a,
                outcome_b=quote.outcome_b,
                token_a=quote.token_a,
                token_b=quote.token_b,
                shares=quote.shares,
                avg_price_a=quote.avg_price_a,
                avg_price_b=quote.avg_price_b,
                gross_cost=quote.gross_cost,
                fee_cost=quote.fee_cost,
                total_cost=quote.total_cost,
                expected_payout=quote.expected_payout,
                expected_edge=quote.edge,
                placed_at=datetime.now(timezone.utc).replace(tzinfo=None),
                end_date=quote.market.end_date,
            )
            self.db.save_position(position)
            free_after = self.free_balance()
            print(
                f"[WS][PAIR] {quote.market.question[:70]}\n"
                f"           {quote.outcome_a}/{quote.outcome_b} | shares={quote.shares:.2f} "
                f"| ask_sum={quote.avg_price_a + quote.avg_price_b:.4f} "
                f"| expected_payout=${quote.expected_payout:.2f}\n"
                f"           total_cost=${quote.total_cost:.2f} | expected_profit=${quote.edge:.2f} "
                f"| free_balance=${free_after:.2f}"
            )

    def _build_quote_from_rest(self, market: Market) -> PairQuote | None:
        book_a = self.clob.get_orderbook(market.clob_token_ids[0])
        book_b = self.clob.get_orderbook(market.clob_token_ids[1])
        if not book_a or not book_a.asks or not book_b or not book_b.asks:
            return None

        top_shares = min(book_a.asks[0].size, book_b.asks[0].size)
        if top_shares < self.strategy.min_top_book_shares:
            return None

        max_affordable_shares = self.free_balance() / max(book_a.asks[0].price + book_b.asks[0].price, 1e-9)
        target_shares = min(top_shares, self.strategy.max_payout_per_trade, max_affordable_shares)
        if target_shares <= 0:
            return None

        gross_cost_a, filled_a = fill_cost_for_shares(book_a, target_shares)
        gross_cost_b, filled_b = fill_cost_for_shares(book_b, target_shares)
        paired_shares = min(filled_a, filled_b)
        if paired_shares <= 0:
            return None

        gross_cost = gross_cost_a + gross_cost_b
        fee_cost = gross_cost * self.trading.taker_fee
        total_cost = gross_cost + fee_cost
        payout = paired_shares
        edge = payout - total_cost
        if edge <= 0:
            return None

        return PairQuote(
            market=market,
            outcome_a=market.outcomes[0],
            outcome_b=market.outcomes[1],
            token_a=market.clob_token_ids[0],
            token_b=market.clob_token_ids[1],
            shares=paired_shares,
            avg_price_a=(gross_cost_a / paired_shares),
            avg_price_b=(gross_cost_b / paired_shares),
            gross_cost=gross_cost,
            fee_cost=fee_cost,
            total_cost=total_cost,
            expected_payout=payout,
            edge=edge,
        )

    def _resolve_market_by_ws(self, market_id: str, winning_outcome: str) -> None:
        if not winning_outcome:
            return
        for position in self.db.get_open_positions():
            if position.market_id != market_id:
                continue
            self.db.resolve_position(position.id, winning_outcome)
            stored = self.db.get_position(position.id)
            print(
                f"[WS][Resolve] {position.question[:70]}\n"
                f"              winner={winning_outcome} | pnl=${stored.pnl:+.2f}"
            )

    def _is_market_eligible(
        self,
        market: Market,
        now: datetime,
        min_expiry: datetime,
        max_expiry: datetime,
    ) -> bool:
        if len(market.outcomes) != 2 or len(market.clob_token_ids) != 2:
            return False
        if market.end_date is None or market.end_date < now:
            return False
        if market.end_date < min_expiry or market.end_date > max_expiry:
            return False
        if market.volume_num < self.strategy.min_volume:
            return False
        if market.liquidity_num < self.strategy.min_liquidity:
            return False
        if self.strategy.category and market.category.lower() != self.strategy.category.lower():
            return False
        if self.strategy.fee_type and market.fee_type != self.strategy.fee_type:
            return False
        if not self._question_matches_query(market.question):
            return False
        return True

    def _winning_outcome(self, market: Market) -> str | None:
        if len(market.outcomes) != len(market.outcome_prices):
            return None
        best_index = max(range(len(market.outcomes)), key=lambda idx: market.outcome_prices[idx])
        best_price = market.outcome_prices[best_index]
        if best_price < 0.9:
            return None
        return market.outcomes[best_index]

    def _question_matches_query(self, question: str) -> bool:
        raw_query = (self.strategy.market_query or "").strip().lower()
        if not raw_query:
            return True

        question_lc = question.lower()
        aliases = QUERY_ALIASES.get(raw_query, (raw_query,))
        return any(alias in question_lc for alias in aliases)
