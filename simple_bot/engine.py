"""Движок simple_bot: scan + resolve."""

from __future__ import annotations

import sys
import os
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from src.api.gamma import GammaClient

from simple_bot.db import Bet, BotDB


GAMMA_URL = "https://gamma-api.polymarket.com"


class SimpleBotEngine:
    def __init__(self, config: dict, db: BotDB) -> None:
        self.config = config
        self.db = db
        self.gamma = GammaClient(base_url=GAMMA_URL, page_size=500, delay_ms=100)

    def _free_balance(self) -> float:
        s = self.config["trading"]
        stats = self.db.stats()
        return s["starting_balance"] + stats["realized_pnl"] - stats["open_invested"]

    def check_resolutions(self) -> None:
        """Проверяем открытые ставки, резолвим закрытые рынки."""
        open_bets = self.db.get_open_bets()
        if not open_bets:
            print("[Resolve] Открытых ставок нет.")
            return

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        resolved = 0

        for bet in open_bets:
            market = self.gamma.fetch_market(bet.market_id)
            if not market:
                continue

            ended = market.closed or (market.end_date and market.end_date < now)
            if not ended:
                continue

            outcome_idx = next(
                (i for i, o in enumerate(market.outcomes) if o == bet.outcome), None
            )
            if outcome_idx is None:
                continue

            exit_price = (
                market.outcome_prices[outcome_idx]
                if outcome_idx < len(market.outcome_prices)
                else 0.0
            )
            self.db.resolve_bet(bet.id, exit_price)
            status = "выиграл" if exit_price >= 0.9 else "проиграл"
            print(f"  [Resolve] {bet.outcome[:50]} — {status} (exit={exit_price:.3f})")
            resolved += 1

        print(
            f"[Resolve] Зарезолвлено: {resolved}"
            f" | Открытых осталось: {len(open_bets) - resolved}"
        )

    def run_scan(self, dry_run: bool = False) -> None:
        """Сканируем рынки, размещаем виртуальные ставки."""
        st = self.config["strategy"]
        tr = self.config["trading"]

        price_lo = st["price_min"]
        price_hi = st["price_max"]
        max_days = st["max_days_to_expiry"]
        min_volume = st["min_volume"]
        bet_size = tr["bet_size"]
        taker_fee = tr["taker_fee"]

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        max_expiry = now + timedelta(days=max_days)

        print(
            f"[Scan] Загрузка рынков"
            f" (экспирация ≤{max_days}д, цена {price_lo}–{price_hi}, vol≥{min_volume})..."
        )

        all_markets = self.gamma.fetch_all_active_markets()
        print(f"[Scan] Загружено {len(all_markets)} активных рынков.")

        # Фильтрация
        candidates = []
        for m in all_markets:
            if not m.end_date:
                continue
            if m.end_date > max_expiry:
                continue
            if m.end_date < now:
                continue
            if m.volume_num < min_volume:
                continue
            if len(m.outcomes) != 2 or len(m.outcome_prices) != 2:
                continue
            for i, price in enumerate(m.outcome_prices):
                if price_lo <= price <= price_hi:
                    candidates.append((m, i, price))
                    break

        # Сортировка по ближайшей экспирации
        candidates.sort(key=lambda x: x[0].end_date)

        free = self._free_balance()
        print(
            f"[Scan] Кандидатов: {len(candidates)}"
            f" | Свободный баланс: ${free:.2f}"
        )

        placed = skipped_dup = skipped_balance = 0

        for market, outcome_idx, price in candidates:
            outcome = market.outcomes[outcome_idx]

            if self.db.already_bet(market.id, outcome):
                skipped_dup += 1
                continue

            fee = bet_size * taker_fee
            if free < bet_size + fee:
                skipped_balance += 1
                continue

            shares = bet_size / price
            days_left = (market.end_date - now).total_seconds() / 86400

            print(
                f"  {'[DRY]' if dry_run else '[BET]'}"
                f" {market.question[:55]}"
                f" | {outcome} @ {price:.3f}"
                f" | {days_left:.1f}д"
            )

            if not dry_run:
                bet = Bet(
                    id=str(uuid.uuid4()),
                    market_id=market.id,
                    question=market.question,
                    outcome=outcome,
                    entry_price=price,
                    amount=bet_size,
                    fee=fee,
                    shares=shares,
                    placed_at=now,
                    end_date=market.end_date,
                )
                self.db.save_bet(bet)
                free -= bet_size + fee

            placed += 1

        print(
            f"[Scan] Размещено: {placed}"
            f" | Дубли: {skipped_dup}"
            f" | Нет баланса: {skipped_balance}"
        )
