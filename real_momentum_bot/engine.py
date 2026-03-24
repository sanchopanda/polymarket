from __future__ import annotations

import math
import time
from datetime import datetime, timezone

from cross_arb_bot.kalshi_feed import KalshiFeed
from cross_arb_bot.matcher import kalshi_taker_fee, match_markets, polymarket_crypto_taker_fee
from cross_arb_bot.models import MatchedMarketPair
from cross_arb_bot.polymarket_feed import PolymarketFeed
from real_arb_bot.clients import KalshiTrader, PolymarketTrader

from real_momentum_bot.db import RealMomentumDB


class RealMomentumEngine:
    def __init__(self, config: dict, db: RealMomentumDB) -> None:
        self.config = config
        self.db = db
        self.strategy = config["strategy"]
        self.budget = config["budget"]
        self.market_filter = config["market_filter"]
        self.pm_feed = PolymarketFeed(
            base_url=config["polymarket"]["gamma_base_url"],
            page_size=config["polymarket"]["page_size"],
            request_delay_ms=config["polymarket"]["request_delay_ms"],
            market_filter=self.market_filter,
        )
        self.kalshi_feed = KalshiFeed(
            base_url=config["kalshi"]["base_url"],
            page_size=config["kalshi"]["page_size"],
            max_pages=config["kalshi"]["max_pages"],
            request_timeout_seconds=config["kalshi"]["request_timeout_seconds"],
            market_filter=self.market_filter,
            series_tickers=config["kalshi"].get("series_tickers", []),
        )
        self.pm_trader = PolymarketTrader()
        self.kalshi_trader = KalshiTrader()
        # cooldown после любой попытки ордера (даже неудачной)
        self._last_attempt: dict[tuple[str, str, str], float] = {}  # (pair_key, side, venue) -> ts

    def discover_pairs(self) -> list[MatchedMarketPair]:
        pm_markets = self.pm_feed.fetch_markets()
        kalshi_markets, _ = self.kalshi_feed.fetch_markets()
        return match_markets(
            pm_markets, kalshi_markets,
            self.market_filter["expiry_tolerance_seconds"],
        )

    def _stop_threshold(self) -> float:
        """Trailing floor: max(min_floor, cumulative_pnl).

        Защищаем всю прибыль выше начального бюджета.
        При нулевой прибыли — минимальный порог min_floor_usd.
        """
        cum_pnl = self.db.cumulative_pnl()
        min_floor = self.budget["min_floor_usd"]
        return max(min_floor, cum_pnl)

    def _total_balance(self) -> float:
        """Начальный бюджет + реализованный PnL."""
        return self.budget["total_usd"] + self.db.cumulative_pnl()

    def _free_balance(self) -> float:
        """Баланс за вычетом заблокированных ставок."""
        stats = self.db.stats()
        return self._total_balance() - stats["locked"]

    def is_stopped(self) -> bool:
        """Бот полностью остановлен: даже после резолва всех открытых позиций
        баланс будет <= порога (считаем что все открытые проиграют)."""
        return self._total_balance() <= self._stop_threshold()

    def can_place_new(self) -> bool:
        """Можно ли ставить новые ставки (свободных денег достаточно)."""
        return self._free_balance() > self._stop_threshold()

    def evaluate_signal(
        self,
        pair_key: str,
        side: str,
        leader_venue: str,
        follower_venue: str,
        leader_price: float,
        follower_price: float,
        spike_magnitude: float,
    ) -> str | None:
        """Returns None if signal is approved, or a rejection reason string."""
        strat = self.strategy

        if not self.can_place_new():
            return "no budget"

        # Направление PM→Kalshi отключено
        if leader_venue == "polymarket" and follower_venue == "kalshi":
            if strat.get("disable_pm_to_kalshi", False):
                return "pm->kalshi disabled"

        # Только последняя треть рынка (последние 5 минут из 15)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if now.minute % 15 < 9:
            return "outside window"

        if spike_magnitude > strat.get("max_spike_cents", 9999):
            return f"spike too large {spike_magnitude:.1f}c"

        if leader_price < strat.get("min_leader_price", 0.0):
            return f"leader too low {leader_price:.4f}"

        if leader_price > strat.get("max_leader_price", 1.0):
            return f"leader too high {leader_price:.4f}"

        if follower_price > strat.get("max_entry_price", 1.0):
            return f"entry too high {follower_price:.4f}"

        if follower_price < 0.5:
            return f"follower < 0.5 ({follower_price:.4f})"

        gap = leader_price - follower_price
        if gap <= 0:
            return f"gap <= 0 ({gap*100:.1f}c)"
        if leader_venue == "polymarket" and follower_venue == "kalshi":
            max_gap = strat.get("max_price_gap_cents_pm_to_kalshi", strat.get("max_price_gap_cents", 9999))
        else:
            max_gap = strat.get("max_price_gap_cents", 9999)
        if gap * 100 > max_gap:
            return f"gap too large {gap*100:.1f}c > {max_gap}c"

        if self.db.has_open_position(pair_key, side, follower_venue):
            return "duplicate position"

        if self.db.has_open_opposite_side(pair_key, side):
            return "opposite side open"

        last_trade = self.db.last_trade_time(pair_key, side)
        if last_trade is not None and (time.time() - last_trade) < strat["cooldown_seconds"]:
            remaining = int(strat["cooldown_seconds"] - (time.time() - last_trade))
            return f"cooldown {remaining}s"

        last_attempt = self._last_attempt.get((pair_key, side, follower_venue), 0.0)
        if (time.time() - last_attempt) < strat["cooldown_seconds"]:
            remaining = int(strat["cooldown_seconds"] - (time.time() - last_attempt))
            return f"attempt cooldown {remaining}s"

        if len(self.db.get_open_positions()) >= strat["max_open_positions"]:
            return f"max positions {strat['max_open_positions']}"

        return None

    def execute_trade(
        self,
        pair_key: str,
        symbol: str,
        side: str,
        leader_venue: str,
        follower_venue: str,
        leader_price: float,
        follower_price: float,
        spike_magnitude: float,
        matched: MatchedMarketPair,
        gap_cents: float = 0.0,
        get_current_price=None,  # callable() -> float | None — актуальная цена перед ордером
    ) -> bool:
        """Place a real order on the follower venue. Returns True if filled."""
        strat = self.strategy
        trades_per_budget = strat.get("trades_per_budget", 10)
        stake = max(1.0, round(self._free_balance() / trades_per_budget, 2))
        pm = matched.polymarket
        ka = matched.kalshi

        title = f"{pm.title} <> {ka.title}"
        expiry = min(pm.expiry, ka.expiry)
        pm_market_id = pm.market_id
        kalshi_ticker = ka.market_id

        # Ставим cooldown сразу при попытке, независимо от результата
        self._last_attempt[(pair_key, side, follower_venue)] = time.time()

        if follower_venue == "kalshi":
            price_cents = round(follower_price * 100)
            # -2 cents to get filled faster (bid below market ask)
            price_cents = max(price_cents - 2, 1)
            count = max(1, math.floor(stake / follower_price))
            fee = kalshi_taker_fee(count, price_cents / 100.0)
            total_cost = count * (price_cents / 100.0) + fee

            self.db.audit("order_attempt", None, {
                "venue": "kalshi", "ticker": kalshi_ticker,
                "side": side, "count": count, "price_cents": price_cents,
            })
            result = self.kalshi_trader.place_limit_order(
                ticker=kalshi_ticker, side=side,
                count=count, price_cents=price_cents,
            )
            if result.shares_matched <= 0 or result.status.startswith("error"):
                # Cancel resting order if any
                if result.order_id and result.status == "resting":
                    self.kalshi_trader.cancel_order(result.order_id)
                self.db.audit("order_failed", None, {
                    "venue": "kalshi", "status": result.status,
                })
                return False

            self.db.audit("order_filled", None, {
                "venue": "kalshi", "order_id": result.order_id,
                "fill": result.shares_matched, "fee": result.fee,
            })
            pos_id = self.db.open_position(
                pair_key=pair_key, symbol=symbol, title=title, expiry=expiry,
                side=side, bet_venue=follower_venue, leader_venue=leader_venue,
                entry_price=price_cents / 100.0, leader_price=leader_price,
                shares=result.shares_matched, total_cost=total_cost,
                spike_magnitude=spike_magnitude,
                order_id=result.order_id, fill_price=result.fill_price,
                fill_shares=result.shares_matched, order_fee=result.fee,
                pm_market_id=pm_market_id, kalshi_ticker=kalshi_ticker,
            )
            print(
                f"[RealMomentum][OPEN] {symbol} {side.upper()} @ kalshi"
                f" | leader={leader_venue} price={leader_price:.4f} spike={spike_magnitude:.1f}¢ gap={gap_cents:.1f}¢"
                f" | entry={price_cents/100:.2f} fill={result.shares_matched:.2f}"
                f" | cost=${total_cost:.2f} | id={pos_id[:8]}"
            )
            return True

        else:  # polymarket
            token_id = pm.yes_token_id if side == "yes" else pm.no_token_id
            if not token_id:
                return False

            fee = polymarket_crypto_taker_fee(stake / follower_price, follower_price)
            total_cost = stake + fee

            # Проверяем актуальную цену прямо перед отправкой
            if get_current_price is not None:
                current_price = get_current_price()
                if current_price is not None:
                    if current_price > strat.get("max_entry_price", 1.0):
                        print(
                            f"[RealMomentum][SKIP] {symbol} {side.upper()} @ polymarket"
                            f" | цена ушла вверх {current_price:.4f} > max={strat['max_entry_price']:.2f}"
                        )
                        return False
                    if current_price < 0.5:
                        print(
                            f"[RealMomentum][SKIP] {symbol} {side.upper()} @ polymarket"
                            f" | цена упала {current_price:.4f} < 0.50"
                        )
                        return False

            self.db.audit("order_attempt", None, {
                "venue": "polymarket", "token_id": token_id,
                "side": side, "amount_usd": stake,
            })
            try:
                result = self.pm_trader.place_fok_order(token_id=token_id, amount_usd=stake)
            except Exception as e:
                self.db.audit("order_failed", None, {"venue": "polymarket", "error": str(e)})
                return False

            if result.status not in {"matched", "MATCHED"}:
                self.db.audit("order_failed", None, {
                    "venue": "polymarket", "status": result.status,
                    "shares_matched": result.shares_matched,
                })
                return False

            # Если get_order вернул 0 (race condition) — оцениваем по цене входа
            fill_price = result.fill_price if result.fill_price > 0 else follower_price
            shares = result.shares_matched if result.shares_matched > 0 else round(stake / fill_price, 4)
            fee = result.fee if result.shares_matched > 0 else polymarket_crypto_taker_fee(shares, fill_price)
            actual_cost = shares * fill_price + fee

            # Предупреждение если fill вышел за ожидаемый диапазон
            if fill_price > strat.get("max_entry_price", 1.0) or fill_price < 0.5:
                print(
                    f"[RealMomentum][WARN] {symbol} {side.upper()} @ polymarket"
                    f" | fill={fill_price:.4f} вне диапазона [0.50, {strat.get('max_entry_price', 1.0):.2f}] — записываем всё равно"
                )

            self.db.audit("order_filled", None, {
                "venue": "polymarket", "order_id": result.order_id,
                "fill": shares, "fee": fee,
                "estimated": result.shares_matched <= 0,
            })
            pos_id = self.db.open_position(
                pair_key=pair_key, symbol=symbol, title=title, expiry=expiry,
                side=side, bet_venue=follower_venue, leader_venue=leader_venue,
                entry_price=fill_price, leader_price=leader_price,
                shares=shares, total_cost=actual_cost,
                spike_magnitude=spike_magnitude,
                order_id=result.order_id, fill_price=fill_price,
                fill_shares=shares, order_fee=fee,
                pm_market_id=pm_market_id, kalshi_ticker=kalshi_ticker,
            )
            print(
                f"[RealMomentum][OPEN] {symbol} {side.upper()} @ polymarket"
                f" | leader={leader_venue} price={leader_price:.4f} spike={spike_magnitude:.1f}¢ gap={gap_cents:.1f}¢"
                f" | entry={fill_price:.4f} fill={shares:.2f}"
                f" | cost=${actual_cost:.2f} | id={pos_id[:8]}"
            )
            return True

    def resolve(self) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for row in self.db.get_open_positions():
            expiry = datetime.fromisoformat(row["expiry"])
            if expiry > now:
                continue

            bet_venue = row["bet_venue"]
            side = row["side"]
            fill_shares = float(row["fill_shares"] or row["shares"])
            fill_price = float(row["fill_price"] or row["entry_price"])
            order_fee = float(row["order_fee"] or 0)

            if bet_venue == "polymarket":
                result = self._resolve_polymarket(row["pm_market_id"])
            else:
                result = self._resolve_kalshi(row["kalshi_ticker"])

            if result is None:
                continue

            if result == side:
                pnl = fill_shares * 1.0 - (fill_shares * fill_price + order_fee)
            else:
                pnl = -(fill_shares * fill_price + order_fee)

            self.db.resolve_position(row["id"], outcome=result, pnl=pnl)
            print(
                f"[RealMomentum][RESOLVE] {row['symbol']} {side.upper()} @ {bet_venue}"
                f" | entry={fill_price:.4f} shares={fill_shares:.2f} cost=${fill_shares * fill_price + order_fee:.2f}"
                f" | outcome={result} | pnl=${pnl:+.2f}"
            )

            # Redeem Polymarket if won — ждём 3 минуты от истечения для on-chain settlement
            if bet_venue == "polymarket" and result == side:
                redeem_delay = 180  # секунд
                elapsed = (now - expiry).total_seconds()
                wait = redeem_delay - elapsed
                if wait > 0:
                    print(f"[RealMomentum][REDEEM] ждём {wait:.0f}с on-chain settlement...")
                    time.sleep(wait)
                self._do_redeem(row["id"], row["pm_market_id"])
            elif bet_venue == "kalshi":
                self._print_balances()

            if self.is_stopped():
                print(
                    f"[RealMomentum][STOP] Cumulative PnL ${self.db.cumulative_pnl():.2f}"
                    f" <= stop_loss ${self.budget['stop_loss_usd']:.2f} — бот остановлен"
                )

    def retry_redeems(self) -> None:
        """Повторяем редим для всех выигравших Polymarket позиций где он ещё не прошёл."""
        self.db._ensure_redeem_column()
        for row in self.db.get_pending_redeems():
            print(f"[RealMomentum][REDEEM-RETRY] {row['symbol']} {row['side'].upper()} | market={row['pm_market_id']}")
            self._do_redeem(row["id"], row["pm_market_id"])

    def _do_redeem(self, position_id: str, pm_market_id: str) -> None:
        REDEEM_RETRIES = 3
        pending_tx = ""
        for attempt in range(1, REDEEM_RETRIES + 1):
            try:
                redeem = self.pm_trader.redeem(pm_market_id, pending_tx_hash=pending_tx)
            except Exception as e:
                print(f"[RealMomentum][REDEEM] attempt {attempt} exception: {e}")
                time.sleep(5)
                continue
            if redeem.success:
                print(f"[RealMomentum][REDEEM] OK ${redeem.payout_usdc:.2f} | tx={redeem.tx_hash[:16]}...")
                self.db.mark_redeemed(position_id)
                self._print_balances()
                return
            elif redeem.pending:
                # TX ушла в pending — ждём её на следующей попытке
                pending_tx = redeem.tx_hash
                print(f"[RealMomentum][REDEEM] TX pending, ждём... tx={pending_tx[:16]}...")
                time.sleep(15)
            else:
                print(f"[RealMomentum][REDEEM] attempt {attempt} FAIL: {redeem.error}")
                pending_tx = ""
                if attempt < REDEEM_RETRIES:
                    time.sleep(10)
        print(f"[RealMomentum][REDEEM] все попытки неудачны, запусти: python -m real_momentum_bot resolve")

    def _print_balances(self) -> None:
        try:
            pm = self.pm_trader.get_balance()
            ka = self.kalshi_trader.get_balance()
            print(f"[RealMomentum][Balances] polymarket=${pm:.2f} | kalshi=${ka:.2f} | total=${pm + ka:.2f}")
        except Exception as e:
            print(f"[RealMomentum][Balances] ошибка: {e}")

    def _resolve_polymarket(self, market_id: str) -> str | None:
        market = self.pm_feed.client.fetch_market(market_id)
        if market is None or len(market.outcomes) != len(market.outcome_prices):
            return None
        try:
            up_idx = next(i for i, o in enumerate(market.outcomes) if o.lower() == "up")
            down_idx = next(i for i, o in enumerate(market.outcomes) if o.lower() == "down")
        except StopIteration:
            return None
        if market.outcome_prices[up_idx] >= 0.9:
            return "yes"
        if market.outcome_prices[down_idx] >= 0.9:
            return "no"
        return None

    def _resolve_kalshi(self, ticker: str) -> str | None:
        payload, _ = self.kalshi_feed.fetch_market(ticker)
        if payload is None:
            return None
        result = str(payload.get("result") or "").lower()
        if result in {"yes", "no"}:
            return result
        return None

    def print_status(self) -> None:
        stats = self.db.stats()
        total = self._total_balance()
        free = self._free_balance()
        threshold = self._stop_threshold()
        cum_pnl = stats["cumulative_pnl"]

        if self.is_stopped():
            state = " [STOPPED]"
        elif not self.can_place_new():
            state = " [WAITING RESOLVES]"
        else:
            state = ""

        print(
            f"[RealMomentum][Status] total=${total:.2f} | free=${free:.2f}"
            f" | pnl=${cum_pnl:+.2f} | floor=${threshold:.2f}{state}"
            f" | open={stats['open_count']} locked=${stats['locked']:.2f}"
            f" | resolved={stats['resolved']} won={stats['won']} lost={stats['lost']}"
        )

        self._print_balances()
        for row in self.db.get_open_positions():
            gap = (float(row['leader_price_at_entry']) - float(row['entry_price'])) * 100
            spike = float(row['spike_magnitude'])
            signal_str = f"spike={spike:.1f}¢ gap={gap:.1f}¢"
            print(
                f"  {row['symbol']} {row['side'].upper()} @ {row['bet_venue']}"
                f" | leader={row['leader_venue']} {signal_str}"
                f" | entry={row['entry_price']:.4f} shares={float(row['fill_shares'] or row['shares']):.2f}"
                f" | cost=${row['total_cost']:.2f}"
                f" | expiry={datetime.fromisoformat(row['expiry']).strftime('%H:%M')}"
            )
