from __future__ import annotations

import json
import time
from dataclasses import replace
from datetime import datetime, timezone

from src.api.clob import ClobClient, OrderLevel

from cross_arb_bot.kalshi_feed import KalshiFeed
from cross_arb_bot.matcher import build_opportunities, match_markets
from cross_arb_bot.models import CrossVenueOpportunity, ExecutionLegInfo, MatchedMarketPair, NormalizedMarket
from cross_arb_bot.polymarket_feed import PolymarketFeed

from real_arb_bot.clients import KalshiTrader, PolymarketTrader
from real_arb_bot.db import RealArbDB
from real_arb_bot.executor import ExecutionResult, OrderExecutor
from real_arb_bot.resolver import PositionResolver
from real_arb_bot.safety import SafetyGuard


class RealArbEngine:
    def __init__(self, config: dict, db: RealArbDB) -> None:
        self.config = config
        self.db = db
        self.trading = config["trading"]
        self.market_filter = config["market_filter"]
        self.runtime = config["runtime"]

        # Discovery — переиспользуем из cross_arb_bot
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
        self.pm_clob = ClobClient(base_url="https://clob.polymarket.com", delay_ms=100)

        # Реальные клиенты бирж
        self.pm_trader = PolymarketTrader()
        self.kalshi_trader = KalshiTrader()

        # Safety, executor, resolver
        self.safety = SafetyGuard(config, db)
        first_leg = config.get("execution", {}).get("first_leg", "kalshi")
        print(f"[config] execution.first_leg={first_leg}")
        self.executor = OrderExecutor(self.pm_trader, self.kalshi_trader, self.safety, db, first_leg=first_leg)
        self.resolver = PositionResolver(
            self.pm_trader, self.kalshi_trader, self.pm_feed, self.kalshi_feed, db,
            notifier=None,  # устанавливается после _init_notifier
        )

        self.last_snapshot: tuple[list, list, list, list] = ([], [], [], [])
        self.last_kalshi_error: str | None = None
        self.last_match_diagnostics: list[str] = []
        self.notifier = None
        self._init_notifier()

    def _init_notifier(self) -> None:
        import os
        tg_cfg = self.config.get("telegram", {})
        token_env = tg_cfg.get("token_env", "SIMPLE_BOT_TOKEN")
        if not os.environ.get(token_env):
            return
        try:
            from real_arb_bot.telegram_notify import TelegramNotifier
            self.notifier = TelegramNotifier(get_status_fn=self.get_status_text, token_env=token_env)
            self.notifier.start()
            self.resolver.notifier = self.notifier
            print(f"[Telegram] Бот запущен ({token_env}).")
        except Exception as e:
            print(f"[Telegram] Не удалось запустить: {e}")

    # ── Баланс ─────────────────────────────────────────────────────────

    def get_real_balances(self) -> dict:
        try:
            pm = self.pm_trader.get_balance()
        except Exception as e:
            print(f"[engine] PM balance error: {e}")
            pm = None
        try:
            kalshi = self.kalshi_trader.get_balance()
        except Exception as e:
            print(f"[engine] Kalshi balance error: {e}")
            kalshi = None
        return {"polymarket": pm, "kalshi": kalshi}

    # ── Скан + исполнение ──────────────────────────────────────────────

    def scan(self, execute: bool = True) -> list[CrossVenueOpportunity]:
        pm_markets = self.pm_feed.fetch_markets()
        kalshi_markets, kalshi_error = self.kalshi_feed.fetch_markets()
        self.last_kalshi_error = kalshi_error

        matches = match_markets(
            polymarket_markets=pm_markets,
            kalshi_markets=kalshi_markets,
            expiry_tolerance_seconds=self.market_filter["expiry_tolerance_seconds"],
        )
        opportunities = build_opportunities(
            matches=matches,
            min_lock_edge=self.trading["min_lock_edge"],
            max_lock_edge=self.trading["max_lock_edge"],
            stake_per_pair_usd=self.trading["stake_per_pair_usd"],
        )
        self.last_snapshot = (pm_markets, kalshi_markets, matches, opportunities)
        self.last_match_diagnostics = self._build_match_diagnostics(matches)

        if execute and opportunities:
            balances = self.get_real_balances()
            self._try_open_positions(opportunities, balances)

        return opportunities

    def _build_match_diagnostics(self, matches: list[MatchedMarketPair]) -> list[str]:
        lines: list[str] = []
        min_lock_edge = float(self.trading["min_lock_edge"])
        max_lock_edge = float(self.trading["max_lock_edge"])
        stake_per_pair_usd = float(self.trading["stake_per_pair_usd"])

        for item in matches:
            pm = item.polymarket
            kalshi = item.kalshi
            lines.append(
                f"{pm.symbol} | expiry_delta={abs((pm.expiry - kalshi.expiry).total_seconds()):.0f}s | "
                f"PM={pm.title} <> Kalshi={kalshi.title}"
            )
            legs = [
                ("PM:YES + KA:NO", pm.yes_ask, kalshi.no_ask, min(pm.yes_depth, kalshi.no_depth)),
                ("KA:YES + PM:NO", kalshi.yes_ask, pm.no_ask, min(kalshi.yes_depth, pm.no_depth)),
            ]
            for label, yes_ask, no_ask, max_shares in legs:
                ask_sum = yes_ask + no_ask
                edge = 1.0 - ask_sum
                if ask_sum <= 0:
                    reason = "ask_sum<=0"
                elif edge < min_lock_edge:
                    reason = f"edge_low ({edge:.4f} < {min_lock_edge:.4f})"
                elif edge > max_lock_edge:
                    reason = f"edge_high ({edge:.4f} > {max_lock_edge:.4f})"
                else:
                    shares = min(stake_per_pair_usd / ask_sum, max_shares)
                    if shares <= 0:
                        reason = f"no_size (max_shares={max_shares:.2f})"
                    else:
                        reason = f"candidate shares={shares:.2f}"
                lines.append(
                    f"  {label} | yes={yes_ask:.4f} no={no_ask:.4f} "
                    f"ask_sum={ask_sum:.4f} edge={edge:.4f} max_shares={max_shares:.2f} | {reason}"
                )
        return lines

    def print_match_diagnostics(self, limit: int = 20) -> None:
        if not self.last_match_diagnostics:
            print("[debug] match diagnostics: none")
            return
        print("[debug] watched pairs / skip reasons:")
        for line in self.last_match_diagnostics[:limit]:
            print(f"  {line}")
        remaining = len(self.last_match_diagnostics) - limit
        if remaining > 0:
            print(f"  ... (+{remaining} more lines)")

    def _try_open_positions(
        self,
        opportunities: list[CrossVenueOpportunity],
        balances: dict,
    ) -> None:
        match_index = {
            f"{m.polymarket.market_id}:{m.kalshi.market_id}": m
            for m in self.last_snapshot[2]
        }
        for opp in opportunities:
            if self.db.has_open_position(opp.pair_key, opp.buy_yes_venue, opp.buy_no_venue):
                continue

            # Safety check
            ok, reason = self.safety.can_trade(opp, balances["polymarket"], balances["kalshi"])
            if not ok:
                print(f"[skip] {opp.symbol} | {reason}")
                continue

            # Execution pricing
            matched = match_index.get(opp.pair_key)
            if matched is None:
                continue
            executed, yes_leg, no_leg = self._apply_execution_pricing(opp, matched)
            if executed is None:
                print(f"[skip] {opp.symbol} | insufficient_liquidity")
                continue
            if executed.edge_per_share < self.trading["min_lock_edge"]:
                print(f"[skip] {opp.symbol} | edge_too_low after execution pricing ({executed.edge_per_share:.4f})")
                continue

            ok2, reason2 = self.safety.can_trade(executed, balances["polymarket"], balances["kalshi"])
            if not ok2:
                print(f"[skip] {opp.symbol} | {reason2} (after exec pricing)")
                continue

            # Confirmation
            if not self.safety.confirm_trade(executed):
                print(f"[skip] {opp.symbol} | not confirmed")
                continue

            # Выставляем реальные ордера
            self._execute_and_record(executed, matched, yes_leg, no_leg)

            # После сделки обновляем балансы
            balances = self.get_real_balances()

    def _execute_and_record(
        self,
        opp: CrossVenueOpportunity,
        matched: MatchedMarketPair,
        yes_leg: ExecutionLegInfo | None,
        no_leg: ExecutionLegInfo | None,
    ) -> None:
        print(
            f"\n[OPEN] {opp.symbol} | route={self.executor.first_leg}_first | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
            f"       ask_sum={opp.ask_sum:.4f} | edge={opp.edge_per_share:.4f} "
            f"| cost=${opp.total_cost:.2f} | exp_profit=${opp.expected_profit:.2f}"
        )

        # Прописываем правильный token_id в yes/no leg для Polymarket ноги
        mapped_yes_leg = yes_leg
        mapped_no_leg = no_leg
        if opp.buy_yes_venue == "polymarket" and yes_leg and matched.polymarket.yes_token_id:
            mapped_yes_leg = replace(yes_leg, market_id=matched.polymarket.yes_token_id)
        if opp.buy_no_venue == "polymarket" and no_leg and matched.polymarket.no_token_id:
            mapped_no_leg = replace(no_leg, market_id=matched.polymarket.no_token_id)

        result: ExecutionResult = self.executor.execute_pair(opp, mapped_yes_leg, mapped_no_leg)

        if result.execution_status == "both_filled":
            print(
                f"[OPEN] SUCCESS | route={result.route} | "
                f"kalshi={result.kalshi_order.shares_matched}@{result.kalshi_order.fill_price} | "
                f"pm={result.polymarket_order.shares_matched}@{result.polymarket_order.fill_price}"
            )
        else:
            print(f"[OPEN] {result.execution_status.upper()} | route={result.route} | {result.reason}")
            if result.execution_status == "failed":
                return  # Ничего не исполнилось — не записываем

        kalshi_res = result.kalshi_order or _empty_order()
        pm_res = result.polymarket_order or _empty_order()

        pos = self.db.open_position(
            opportunity=opp,
            kalshi_result=kalshi_res,
            polymarket_result=pm_res,
            execution_status=result.execution_status,
            route=result.route,
            polymarket_snapshot_open=self._pm_snapshot(matched.polymarket.market_id),
            kalshi_snapshot_open=self._kalshi_snapshot(matched.kalshi.market_id),
            yes_leg=yes_leg,
            no_leg=no_leg,
        )

        if result.execution_status in {"unwound_kalshi", "unwound_polymarket"}:
            if result.execution_status == "unwound_kalshi":
                buy_order = result.kalshi_order
                sell_order = result.unwind_order
                pm_result = "not_filled"
                kalshi_result = "unwound"
            else:
                buy_order = result.polymarket_order
                sell_order = result.unwind_order
                pm_result = "unwound"
                kalshi_result = "not_filled"

            if result.realized_pnl is not None:
                unwind_pnl = result.realized_pnl
            elif buy_order and sell_order and sell_order.shares_matched > 0:
                buy_cost = buy_order.fill_price * sell_order.shares_matched
                sell_proceeds = sell_order.fill_price * sell_order.shares_matched
                unwind_pnl = sell_proceeds - buy_cost - buy_order.fee - sell_order.fee
            elif buy_order:
                unwind_pnl = -(buy_order.fill_price * buy_order.shares_matched + buy_order.fee)
            else:
                unwind_pnl = 0.0
            self.db.resolve_position(
                position_id=pos.id,
                winning_side="n/a",
                pnl=unwind_pnl,
                actual_pnl=unwind_pnl,
                polymarket_result=pm_result,
                kalshi_result=kalshi_result,
                lock_valid=False,
            )
            print(f"[CLOSED] UNWOUND | pnl=${unwind_pnl:.2f}")
            if self.notifier:
                self.notifier.notify_resolve(
                    symbol=opp.symbol,
                    pm_result=pm_result,
                    kalshi_result=kalshi_result,
                    pnl=unwind_pnl,
                    lock_valid=False,
                )
            return

        if result.execution_status in {"orphaned_kalshi", "orphaned_polymarket"}:
            print("[OPEN] ORPHANED — торговля приостановлена до ручного разбора позиции!")
            self.safety.dry_run = True

        if self.notifier:
            kalshi_fill = result.kalshi_order.fill_price if result.kalshi_order else 0.0
            pm_fill = result.polymarket_order.fill_price if result.polymarket_order else 0.0
            self.notifier.notify_open(
                symbol=opp.symbol,
                yes_venue=opp.buy_yes_venue,
                no_venue=opp.buy_no_venue,
                yes_ask=opp.yes_ask,
                no_ask=opp.no_ask,
                ask_sum=opp.ask_sum,
                edge=opp.edge_per_share,
                cost=opp.total_cost,
                expected_profit=opp.expected_profit,
                execution_status=result.execution_status,
                kalshi_fill=kalshi_fill,
                pm_fill=pm_fill,
            )

    # ── Резолюция ──────────────────────────────────────────────────────

    def resolve(self) -> None:
        self.resolver.resolve_all()

    # ── Execution pricing (переиспользуем логику из cross_arb_bot) ─────

    def _stake_for_edge(self, edge: float) -> float:
        base = self.trading["stake_per_pair_usd"]
        if edge > self.trading.get("stake_tier_third_above", 0.30):
            return base / 3.0
        if edge > self.trading.get("stake_tier_half_above", 0.20):
            return base / 2.0
        return base

    def _apply_execution_pricing(
        self,
        opp: CrossVenueOpportunity,
        matched: MatchedMarketPair,
    ) -> tuple[CrossVenueOpportunity | None, ExecutionLegInfo | None, ExecutionLegInfo | None]:
        yes_market = matched.polymarket if opp.buy_yes_venue == "polymarket" else matched.kalshi
        no_market = matched.polymarket if opp.buy_no_venue == "polymarket" else matched.kalshi

        # Уменьшаем ставку при высоком edge
        tiered_stake = self._stake_for_edge(opp.edge_per_share)
        shares = min(tiered_stake / opp.ask_sum, opp.shares)

        min_edge = self.config.get("trading", {}).get("min_lock_edge", 0.04)
        max_yes_price = 1.0 - opp.no_ask - min_edge
        max_no_price = 1.0 - opp.yes_ask - min_edge
        yes_leg = self._execution_leg_info(opp.buy_yes_venue, yes_market, "yes", shares, max_price=max_yes_price)
        no_leg = self._execution_leg_info(opp.buy_no_venue, no_market, "no", shares, max_price=max_no_price)

        if yes_leg is None or no_leg is None:
            return None, yes_leg, no_leg
        if yes_leg.filled_shares + 1e-6 < shares or no_leg.filled_shares + 1e-6 < shares:
            return None, yes_leg, no_leg

        yes_ask = yes_leg.avg_price
        no_ask = no_leg.avg_price
        ask_sum = yes_ask + no_ask
        edge_per_share = 1.0 - ask_sum
        capital_used = yes_leg.total_cost + no_leg.total_cost

        pm_fee = 0.0
        kalshi_fee = 0.0
        if opp.buy_yes_venue == "polymarket":
            pm_fee += self._pm_fee(shares, yes_ask)
        else:
            kalshi_fee += self._kalshi_fee(shares, yes_ask)
        if opp.buy_no_venue == "polymarket":
            pm_fee += self._pm_fee(shares, no_ask)
        else:
            kalshi_fee += self._kalshi_fee(shares, no_ask)

        total_fee = pm_fee + kalshi_fee
        total_cost = capital_used + total_fee

        return (
            replace(
                opp,
                shares=shares,
                yes_ask=yes_ask, no_ask=no_ask, ask_sum=ask_sum,
                edge_per_share=edge_per_share, capital_used=capital_used,
                polymarket_fee=pm_fee, kalshi_fee=kalshi_fee,
                total_fee=total_fee, total_cost=total_cost,
                expected_profit=shares - total_cost,
            ),
            yes_leg,
            no_leg,
        )

    def _execution_leg_info(
        self, venue: str, market: NormalizedMarket, side: str, shares: float,
        max_price: float = 1.0,
    ) -> ExecutionLegInfo | None:
        if venue == "polymarket":
            token_id = market.yes_token_id if side == "yes" else market.no_token_id
            if not token_id:
                return None
            book = self.pm_clob.get_orderbook(token_id)
            if not book or not book.asks:
                return None
            asks = book.asks
        else:
            asks, _ = self.kalshi_feed.fetch_side_asks(market.market_id, side)
            if not asks:
                return None

        available = sum(l.size for l in asks)
        usable = sum(l.size for l in asks if l.price <= max_price)
        best_ask = asks[0].price if asks else 0.0
        remaining = shares
        total_cost = 0.0
        for level in asks:
            take = min(level.size, remaining)
            if take <= 0:
                continue
            total_cost += take * level.price
            remaining -= take
            if remaining <= 1e-9:
                break

        filled = shares - remaining
        avg_price = (total_cost / filled) if filled > 1e-9 else 0.0
        return ExecutionLegInfo(
            venue=venue,
            market_id=market.market_id,
            side=side,
            requested_shares=shares,
            filled_shares=filled,
            available_shares=available,
            usable_shares=usable,
            avg_price=avg_price,
            total_cost=total_cost,
            best_ask=best_ask,
            remaining_shares_after_fill=max(0.0, available - filled),
        )

    def _pm_fee(self, shares: float, price: float) -> float:
        return shares * price * 0.25 * ((price * (1 - price)) ** 2)

    def _kalshi_fee(self, shares: float, price: float) -> float:
        raw = 0.07 * shares * price * (1 - price)
        cents = int(raw * 100)
        if raw * 100 > cents:
            cents += 1
        return cents / 100.0

    def _format_leg_summary(self, leg) -> str:
        if leg is None:
            return "book unavailable"
        slippage = leg.avg_price - leg.best_ask if leg.filled_shares > 1e-9 else 0.0
        status = "ok" if leg.filled_shares + 1e-6 >= leg.requested_shares else "short"
        return (
            f"{leg.venue}:{leg.side.upper()} need {leg.requested_shares:.2f}sh | "
            f"usable {leg.usable_shares:.0f}sh (total {leg.available_shares:.0f}sh) | "
            f"filled {leg.filled_shares:.2f}sh | "
            f"best {leg.best_ask:.4f} -> avg {leg.avg_price:.4f} "
            f"(slippage {slippage:+.4f}) | "
            f"cost ${leg.total_cost:.2f} | {status}"
        )

    # ── Снапшоты ───────────────────────────────────────────────────────

    def _pm_snapshot(self, market_id: str) -> str | None:
        market = self.pm_feed.client.fetch_market(market_id)
        if not market:
            return None
        return json.dumps({
            "stage": "open", "venue": "polymarket",
            "market_id": market.id, "outcomes": market.outcomes,
            "outcome_prices": market.outcome_prices,
        })

    def _kalshi_snapshot(self, market_id: str) -> str | None:
        payload, _ = self.kalshi_feed.fetch_market(market_id)
        if not payload:
            return None
        return json.dumps({
            "stage": "open", "venue": "kalshi",
            "ticker": payload.get("ticker"),
            "status": payload.get("status"),
            "result": payload.get("result"),
        })

    # ── Статус ─────────────────────────────────────────────────────────

    def get_status_text(self) -> str:
        stats = self.db.stats()
        balances = self.get_real_balances()
        pm_markets, kalshi_markets, matches, opportunities = self.last_snapshot
        lines = [
            f"💼 <b>Балансы</b>",
            f"Polymarket: <b>${balances['polymarket']:.2f}</b>",
            f"Kalshi:     <b>${balances['kalshi']:.2f}</b>",
            f"",
            f"📊 <b>Позиции</b>",
            f"Открытых: {stats['open']} | Закрытых: {stats['resolved']}",
            f"P&L сегодня: <b>${stats['daily_pnl']:+.2f}</b> | всего: <b>${stats['realized_pnl']:+.2f}</b>",
            f"Orphaned: {stats['orphaned']}",
        ]
        open_positions = self.db.get_open_positions()
        if open_positions:
            lines.append("")
            lines.append("📌 <b>Открытые пары:</b>")
            for p in open_positions:
                paper_tag = " [PAPER]" if p.is_paper else ""
                lines.append(
                    f"  {p.symbol}{paper_tag} | {p.venue_yes}:YES + {p.venue_no}:NO"
                    f" | ${p.total_cost:.2f} | открыта {p.opened_at.strftime('%m-%d %H:%M')}"
                )
        if self.safety.dry_run:
            lines.append("")
            lines.append("⏸ <b>РЕЖИМ: dry_run / ПАУЗА</b>")
        return "\n".join(lines)

    def print_status(self, balances: dict | None = None) -> None:
        stats = self.db.stats()
        if balances is None:
            balances = self.get_real_balances()

        pm_markets, kalshi_markets, matches, opportunities = self.last_snapshot
        pm_bal = f"${balances['polymarket']:.2f}" if balances.get('polymarket') is not None else "N/A"
        ka_bal = f"${balances['kalshi']:.2f}" if balances.get('kalshi') is not None else "N/A"
        print(
            f"\n{'='*60}\n"
            f"  Polymarket:  {pm_bal}\n"
            f"  Kalshi:      {ka_bal}\n"
            f"  Open:        {stats['open']} | Resolved: {stats['resolved']}\n"
            f"  P&L сегодня: ${stats['daily_pnl']:+.2f} | всего: ${stats['realized_pnl']:+.2f}\n"
            f"  Orphaned:    {stats['orphaned']}\n"
            f"  Snapshot:    pm={len(pm_markets)} kalshi={len(kalshi_markets)} "
            f"matches={len(matches)} opps={len(opportunities)}"
        )
        if self.safety.dry_run:
            print("  РЕЖИМ: dry_run / ПАУЗА")
        if self.last_kalshi_error:
            print(f"  Kalshi error: {self.last_kalshi_error}")


def _empty_order():
    from real_arb_bot.clients import OrderResult
    return OrderResult(
        order_id="", status="no_order", fill_price=0.0,
        shares_matched=0.0, shares_requested=0.0, fee=0.0,
        latency_ms=0.0, raw_response={},
    )
