from __future__ import annotations

from collections import deque
from datetime import datetime
from typing import Iterable

from oracle_arb_bot.models import OracleMarket
from oracle_arb_bot.scanner import OracleScanner
from src.api.clob import ClobClient, OrderBook

from jump_paper_bot.db import JumpPaperDB
from jump_paper_bot.models import PricePoint, TrackedSideState
from jump_paper_bot.telegram_notify import JumpTelegramNotifier


class JumpPaperEngine:
    def __init__(
        self,
        config: dict,
        db: JumpPaperDB,
        notifier: JumpTelegramNotifier | None = None,
        *,
        scanner: OracleScanner | None = None,
        clob: ClobClient | None = None,
    ) -> None:
        self.config = config
        self.db = db
        self.notifier = notifier
        self.strategy = config["strategy"]
        self.runtime = config["runtime"]
        self.market_filter = config["market_filter"]
        self._states: dict[tuple[str, str], TrackedSideState] = {}
        self.scanner = scanner or OracleScanner(config)
        self.scanner.set_pm_price_callback(self.on_pm_price)
        self._clob = clob or ClobClient(base_url=config["polymarket"]["clob_base_url"])

    def scan_markets(self) -> list[OracleMarket]:
        return [m for m in self.scanner.scan_and_subscribe() if m.venue == "polymarket"]

    def stop(self) -> None:
        self.scanner.stop()

    def on_pm_price(self, market: OracleMarket, side: str, best_ask: float) -> None:
        if market.venue != "polymarket" or side not in {"yes", "no"} or best_ask <= 0:
            return
        now = datetime.utcnow()
        state = self._states.setdefault((market.market_id, side), TrackedSideState())
        state.prices.append(PricePoint(timestamp=now, price=float(best_ask)))
        self._trim_history(state.prices, now)
        self.db.insert_price_point(market.market_id, side, now, float(best_ask))

        avg_prev = self._avg_prev_lookback(state.prices, now)
        if avg_prev is None:
            return
        signal_level = self._matching_signal_level(float(best_ask))
        if signal_level is None:
            return
        jump_cents = float(self.strategy["jump_cents"])
        if float(best_ask) - avg_prev + 1e-9 < jump_cents:
            return

        seconds_left = (market.expiry - now).total_seconds()
        if seconds_left <= 0:
            return

        for bucket_seconds in self._active_buckets(seconds_left):
            self._maybe_open_signal(
                market=market,
                side=side,
                signal_bucket_seconds=bucket_seconds,
                signal_level=signal_level,
                signal_price=float(best_ask),
                signal_avg_prev_10s=avg_prev,
            )

    def _maybe_open_signal(
        self,
        *,
        market: OracleMarket,
        side: str,
        signal_bucket_seconds: int,
        signal_level: float,
        signal_price: float,
        signal_avg_prev_10s: float,
    ) -> None:
        limit_price = signal_price + float(self.strategy["depth_limit_offset"])
        token_id = market.yes_token_id if side == "yes" else market.no_token_id
        if not token_id:
            self.db.try_record_signal(
                market_id=market.market_id,
                symbol=market.symbol,
                interval_minutes=market.interval_minutes,
                side=side,
                signal_bucket_seconds=signal_bucket_seconds,
                signal_level=signal_level,
                signal_price=signal_price,
                signal_avg_prev_10s=signal_avg_prev_10s,
                limit_price=limit_price,
                status="skip",
                skip_reason="token_id_missing",
            )
            return

        book = self._clob.get_orderbook(token_id)
        status, skip_reason, fill = self._evaluate_fill(book, limit_price=limit_price)
        signal_id = self.db.try_record_signal(
            market_id=market.market_id,
            symbol=market.symbol,
            interval_minutes=market.interval_minutes,
            side=side,
            signal_bucket_seconds=signal_bucket_seconds,
            signal_level=signal_level,
            signal_price=signal_price,
            signal_avg_prev_10s=signal_avg_prev_10s,
            limit_price=limit_price,
            status=status,
            skip_reason=skip_reason,
        )
        if signal_id is None or fill is None:
            return

        position = self.db.open_position(
            market_id=market.market_id,
            symbol=market.symbol,
            title=market.title,
            interval_minutes=market.interval_minutes,
            side=side,
            signal_bucket_seconds=signal_bucket_seconds,
            signal_level=signal_level,
            signal_price=signal_price,
            signal_avg_prev_10s=signal_avg_prev_10s,
            limit_price=limit_price,
            entry_price=fill["avg_fill"],
            filled_shares=fill["shares"],
            total_cost=fill["cost"],
            depth_usd=fill["depth_usd"],
            market_end=market.expiry,
        )
        self.db.attach_signal_position(signal_id, position.id)
        market_url = market.pm_event_slug and f"https://polymarket.com/event/{market.pm_event_slug}"
        print(
            f"[jump] OPEN {market.symbol} {market.interval_minutes}m {side.upper()} "
            f"| bucket={signal_bucket_seconds}s level={signal_level:.2f} "
            f"| signal={signal_price:.3f} avg10s={signal_avg_prev_10s:.3f} "
            f"| fill={fill['avg_fill']:.3f} shares={fill['shares']:.2f} depth=${fill['depth_usd']:.2f}"
        )
        if self.notifier is not None:
            message_id = self.notifier.notify_open(
                symbol=market.symbol,
                interval_minutes=market.interval_minutes,
                side=side,
                signal_bucket_seconds=signal_bucket_seconds,
                signal_level=signal_level,
                signal_price=signal_price,
                avg_prev_10s=signal_avg_prev_10s,
                fill_avg=fill["avg_fill"],
                stake_usd=float(self.strategy["paper_stake_usd"]),
                shares=fill["shares"],
                depth_usd=fill["depth_usd"],
                market_url=market_url,
            )
            if message_id is not None:
                self.db.set_open_message_id(position.id, message_id)

    def _evaluate_fill(self, book: OrderBook | None, *, limit_price: float) -> tuple[str, str | None, dict | None]:
        if book is None or not book.asks:
            return "skip", "orderbook_empty", None
        eligible = [lvl for lvl in book.asks if lvl.price <= limit_price]
        if not eligible:
            return "skip", "no_asks_within_limit", None
        stake = float(self.strategy["paper_stake_usd"])
        required_depth = float(self.strategy["required_depth_multiple"]) * stake
        depth_usd = sum(lvl.price * lvl.size for lvl in eligible)
        if depth_usd + 1e-9 < required_depth:
            return "skip", "insufficient_depth", None

        remaining = stake
        total_cost = 0.0
        total_shares = 0.0
        for lvl in eligible:
            level_usd = lvl.price * lvl.size
            spend = min(remaining, level_usd)
            total_cost += spend
            total_shares += spend / lvl.price
            remaining -= spend
            if remaining <= 1e-9:
                break
        if total_shares <= 0 or remaining > 1e-6:
            return "skip", "partial_fill", None
        avg_fill = total_cost / total_shares
        return "open", None, {
            "avg_fill": avg_fill,
            "shares": total_shares,
            "cost": total_cost,
            "depth_usd": depth_usd,
        }

    def resolve(self) -> None:
        now = datetime.utcnow()
        for position in self.db.get_open_positions():
            if position.market_end > now:
                continue
            winning_side = self._resolve_polymarket(position.market_id)
            if winning_side is None:
                continue
            pnl = (
                position.filled_shares - position.total_cost
                if winning_side == position.side
                else -position.total_cost
            )
            self.db.resolve_position(position.id, winning_side, pnl)
            print(
                f"[jump] RESOLVE {position.symbol} {position.interval_minutes}m {position.side.upper()} "
                f"| winner={winning_side} pnl=${pnl:+.2f}"
            )
            if self.notifier is not None:
                self.notifier.notify_resolve(
                    symbol=position.symbol,
                    interval_minutes=position.interval_minutes,
                    side=position.side,
                    winning_side=winning_side,
                    pnl=pnl,
                    reply_to_message_id=position.telegram_message_id,
                )

    def _resolve_polymarket(self, market_id: str) -> str | None:
        market = self.scanner._pm_feed.client.fetch_market(market_id)  # reuse scanner's PM client
        if market is None or len(market.outcomes) != len(market.outcome_prices):
            return None
        try:
            up_idx = next(i for i, outcome in enumerate(market.outcomes) if outcome.lower() == "up")
            down_idx = next(i for i, outcome in enumerate(market.outcomes) if outcome.lower() == "down")
        except StopIteration:
            return None
        if market.outcome_prices[up_idx] >= 0.9:
            return "yes"
        if market.outcome_prices[down_idx] >= 0.9:
            return "no"
        return None

    def get_status_text(self) -> str:
        stats = self.db.stats()
        resolved = stats["resolved_count"]
        won = stats["won_count"]
        wr = won / resolved * 100.0 if resolved else 0.0
        lines = [
            "<b>Jump Paper Bot</b>",
            f"signals={stats['total_signals']} | opens={stats['opened_signals']} | skips={stats['skipped_signals']}",
            f"open={stats['open_count']} | resolved={resolved} | wr={wr:.1f}% | pnl=${stats['realized_pnl']:+.2f}",
        ]
        symbol_rows = self.db.breakdown_by_symbol()
        if symbol_rows:
            lines.append("")
            lines.append("По символам:")
            for row in symbol_rows:
                row_resolved = int(row["resolved_count"] or 0)
                row_won = int(row["won_count"] or 0)
                row_wr = row_won / row_resolved * 100.0 if row_resolved else 0.0
                lines.append(
                    f"{row['symbol']}: total={int(row['total_count'])} resolved={row_resolved} "
                    f"wr={row_wr:.1f}% pnl=${float(row['pnl'] or 0.0):+.2f}"
                )
        recent = self.db.get_recent_positions(limit=5)
        if recent:
            lines.append("")
            lines.append("Последние позиции:")
            for pos in recent:
                pnl_text = "" if pos.pnl is None else f" pnl=${pos.pnl:+.2f}"
                lines.append(
                    f"{pos.symbol} {pos.interval_minutes}m {pos.side.upper()} "
                    f"| bucket={pos.signal_bucket_seconds}s level={pos.signal_level:.2f} "
                    f"| entry={pos.entry_price:.3f} cost=${pos.total_cost:.2f} status={pos.status}{pnl_text}"
                )
        return "\n".join(lines)

    def print_status(self) -> None:
        print(self.get_status_text())

    def _matching_signal_level(self, price: float) -> float | None:
        for level in self.strategy["signal_levels"]:
            level_f = float(level)
            if level_f < price <= level_f + float(self.strategy["depth_limit_offset"]) + 1e-9:
                return level_f
        return None

    def _active_buckets(self, seconds_left: float) -> Iterable[int]:
        for bucket in sorted((int(v) for v in self.strategy["time_buckets_seconds"]), reverse=True):
            if seconds_left <= bucket + 1e-9:
                yield bucket

    def _avg_prev_lookback(self, prices: deque[PricePoint], now: datetime) -> float | None:
        lookback_seconds = float(self.strategy["lookback_seconds"])
        points = [
            point.price
            for point in prices
            if 0.0 < (now - point.timestamp).total_seconds() <= lookback_seconds
        ]
        if not points:
            return None
        return sum(points) / len(points)

    def _trim_history(self, prices: deque[PricePoint], now: datetime) -> None:
        max_age = max(
            float(self.strategy["lookback_seconds"]) + 2.0,
            float(max(self.strategy["time_buckets_seconds"])) + 5.0,
        )
        while prices and (now - prices[0].timestamp).total_seconds() > max_age:
            prices.popleft()
