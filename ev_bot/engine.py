"""EV-бот: paper trading движок с EV-фильтрацией."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from src.api.clob import ClobClient
from src.api.gamma import GammaClient, Market
from ev_bot.config import EVBotConfig
from ev_bot.db import EVBet, EVSeries, EVStore
from ev_bot.filter import EVFilter


class EVPaperEngine:
    def __init__(self, config: EVBotConfig, store: EVStore, gamma: GammaClient, ev_filter: EVFilter) -> None:
        self.config = config
        self.store = store
        self.gamma = gamma
        self.ev_filter = ev_filter

    def _bet_size(self, depth: int) -> float:
        """Размер ставки: initial_bet * multiplier^depth."""
        multiplier = 2.0 * (1 + self.config.martingale.taker_fee)
        return round(self.config.martingale.initial_bet * (multiplier ** depth), 6)

    def scan(self, dry_run: bool = False) -> int:
        """Сканирует рынки и открывает новые серии Мартингейла."""
        cfg = self.config.strategy
        mc = self.config.martingale
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        print("[EV-Scan] Загружаем активные рынки...")
        markets = self.gamma.fetch_all_active_markets()

        candidates = []
        for market in markets:
            # Тип комиссии
            if cfg.fee_type == "crypto_fees" and market.fee_type != "crypto_fees":
                continue
            # Объём и ликвидность
            if market.volume_num < cfg.min_volume:
                continue
            if market.liquidity_num < cfg.min_liquidity:
                continue
            # Срок
            if not market.end_date:
                continue
            delta = (market.end_date - now).total_seconds() / 86400
            if delta < 0 or delta > cfg.max_days_to_expiry:
                continue

            # Перебираем исходы
            for i, (outcome, price) in enumerate(zip(market.outcomes, market.outcome_prices)):
                if not (cfg.price_min <= price <= cfg.price_max):
                    continue
                if i >= len(market.clob_token_ids) or not market.clob_token_ids[i]:
                    continue

                # EV-фильтр — ключевое условие
                if not self.ev_filter.passes(price, market.volume_num):
                    continue

                candidates.append((market, outcome, i, price, delta))

        print(f"[EV-Scan] Кандидатов после EV-фильтра: {len(candidates)}")

        active = self.store.get_active_series()
        # Динамический лимит: упрощённо capital / cost_deep_series
        max_series = max(1, int(mc.starting_balance / self._bet_size(2)))

        placed = 0
        available = mc.starting_balance - self.store.get_total_invested_active()

        for market, outcome, idx, price, days_left in candidates:
            if len(active) >= max_series:
                break
            if self.store.already_bet(market.id, outcome):
                continue

            size = self._bet_size(0)
            fee = round(size * mc.taker_fee, 6)
            total_cost = size + fee

            if available < total_cost:
                print(f"[EV-Scan] Недостаточно средств (${available:.2f} < ${total_cost:.2f})")
                break

            mode = "DRY" if dry_run else "BET"
            print(
                f"[{mode}] {market.question[:60]}\n"
                f"       Исход: {outcome} | Цена: {price:.4f} | "
                f"x{1/price:.1f} | Срок: {days_left*24:.1f}ч"
            )

            if not dry_run:
                series = EVSeries(initial_bet=mc.initial_bet, total_invested=total_cost)
                self.store.create_series(series)
                bet = EVBet(
                    series_id=series.id, series_depth=0,
                    market_id=market.id, market_question=market.question,
                    outcome=outcome, token_id=market.clob_token_ids[idx],
                    entry_price=price, amount_usd=size, fee_usd=fee,
                    shares=size / price,
                    placed_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    market_end_date=market.end_date,
                )
                self.store.save_bet(bet)
                active.append(series)
                available -= total_cost
                placed += 1

        print(f"[EV-Scan] Новых серий: {placed}")
        return placed

    def check_resolutions(self) -> int:
        """Проверяет открытые ставки. При выигрыше — серия закрыта. При проигрыше — эскалация."""
        open_bets = self.store.get_open_bets()
        if not open_bets:
            print("[EV-Resolve] Открытых позиций нет.")
            return 0

        print(f"[EV-Resolve] Проверяем {len(open_bets)} позиций...")
        resolved = 0
        to_escalate = []
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        for bet in open_bets:
            market = self.gamma.fetch_market(bet.market_id)
            if market is None:
                continue

            expired = bet.market_end_date and bet.market_end_date <= now
            prices_settled = any(p >= 0.95 or p <= 0.05 for p in market.outcome_prices)
            if not market.closed and not (expired and prices_settled):
                continue

            try:
                idx = market.outcomes.index(bet.outcome)
                exit_price = market.outcome_prices[idx]
            except (ValueError, IndexError):
                continue

            status = "won" if exit_price >= 0.9 else "lost"
            pnl = (exit_price * bet.shares) - bet.amount_usd - bet.fee_usd
            print(
                f"[EV-Resolve] {'✓ ВЫИГРЫШ' if status == 'won' else '✗ ПРОИГРЫШ'}: "
                f"{bet.market_question[:55]} | P&L: ${pnl:+.2f}"
            )
            self.store.resolve_bet(bet.id, exit_price)
            resolved += 1

            # Обновляем EV-фильтр свежими данными из реальных сделок
            from src.backtest.fetcher import HistoricalMarket
            self.ev_filter.add_resolved(HistoricalMarket(
                market_id=bet.market_id, question=bet.market_question,
                outcome=bet.outcome, token_id=bet.token_id,
                entry_price=bet.entry_price, final_price=exit_price,
                won=(status == "won"),
                volume_num=market.volume_num, liquidity_num=market.liquidity_num,
                end_date=bet.market_end_date,
            ))

            if bet.series_id:
                if status == "won":
                    bets = self.store.get_series_bets(bet.series_id)
                    total_pnl = sum(b.pnl or 0 for b in bets)
                    self.store.finish_series(bet.series_id, "won", total_pnl)
                    series = self.store.get_series_by_id(bet.series_id)
                    print(f"[EV-Series] ✓ Серия выиграна | depth={series.current_depth} | P&L=${total_pnl:+.2f}")
                else:
                    to_escalate.append((bet.series_id, bet.series_depth))

        # Эскалация проигравших серий
        if to_escalate:
            self._escalate_all(to_escalate)

        print(f"[EV-Resolve] Закрыто: {resolved}")
        return resolved

    def _escalate_all(self, escalations: list) -> None:
        """Загружает рынки один раз и эскалирует все проигравшие серии."""
        cfg = self.config.strategy
        mc = self.config.martingale
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        print("[EV-Escalate] Загружаем рынки для эскалации...")
        markets = self.gamma.fetch_all_active_markets()

        # Отфильтровать кандидатов с EV-фильтром
        candidates = []
        for market in markets:
            if cfg.fee_type == "crypto_fees" and market.fee_type != "crypto_fees":
                continue
            if market.volume_num < cfg.min_volume or market.liquidity_num < cfg.min_liquidity:
                continue
            if not market.end_date:
                continue
            delta = (market.end_date - now).total_seconds() / 86400
            if delta < 0 or delta > cfg.max_days_to_expiry:
                continue
            for i, (outcome, price) in enumerate(zip(market.outcomes, market.outcome_prices)):
                if not (cfg.price_min <= price <= cfg.price_max):
                    continue
                if i >= len(market.clob_token_ids) or not market.clob_token_ids[i]:
                    continue
                if not self.ev_filter.passes(price, market.volume_num):
                    continue
                if self.store.already_bet(market.id, outcome):
                    continue
                candidates.append((market, outcome, i, price))

        print(f"[EV-Escalate] Свободных +EV кандидатов: {len(candidates)} | Нужно эскалаций: {len(escalations)}")

        for series_id, current_depth in escalations:
            series = self.store.get_series_by_id(series_id)
            if not series:
                continue
            next_depth = current_depth + 1

            if next_depth >= mc.max_depth:
                bets = self.store.get_series_bets(series_id)
                total_pnl = sum(b.pnl or 0 for b in bets)
                self.store.finish_series(series_id, "abandoned", total_pnl)
                print(f"[EV-Series] ✗ Серия брошена (лимит глубины {mc.max_depth}) | P&L=${total_pnl:+.2f}")
                continue

            size = self._bet_size(next_depth)
            available = mc.starting_balance - self.store.get_total_invested_active()
            if available < size * (1 + mc.taker_fee):
                print(f"[EV-Series] ⏸ Серия {series_id[:8]} ждёт средств (${available:.2f} < ${size:.2f})")
                continue

            placed = False
            for market, outcome, idx, price in candidates:
                if self.store.already_bet(market.id, outcome):
                    continue
                fee = round(size * mc.taker_fee, 6)
                bet = EVBet(
                    series_id=series_id, series_depth=next_depth,
                    market_id=market.id, market_question=market.question,
                    outcome=outcome, token_id=market.clob_token_ids[idx],
                    entry_price=price, amount_usd=size, fee_usd=fee,
                    shares=size / price,
                    placed_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    market_end_date=market.end_date,
                )
                self.store.save_bet(bet)
                self.store.update_series_depth(series_id, next_depth, size + fee)
                print(
                    f"[EV-Series] ↑ Эскалация depth={next_depth} | ${size:.2f} | "
                    f"{market.question[:45]} / {outcome}"
                )
                placed = True
                break

            if not placed:
                print(f"[EV-Series] ⏳ Нет +EV рынка для эскалации depth={next_depth}")
