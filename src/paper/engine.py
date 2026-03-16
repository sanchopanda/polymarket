from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from src.api.clob import ClobClient
from src.api.gamma import GammaClient
from src.config import Config
from src.db.models import BetSeries, ScanLog, SimulatedBet
from src.db.store import Store
from src.strategy.scanner import Candidate, MarketScanner
from src.strategy.scorer import CandidateScorer, ScoredCandidate
from src.strategy.sizing import PositionSizer, compute_max_deep_slots, compute_dynamic_active_series


class PaperTradingEngine:
    def __init__(self, config: Config, store: Store, gamma: GammaClient, clob: ClobClient) -> None:
        self.config = config
        self.store = store
        self.gamma = gamma
        self.clob = clob
        self.scanner = MarketScanner(gamma, config.strategy)
        self.scorer = CandidateScorer(config.strategy)
        self.sizer = PositionSizer(config.martingale, config.paper_trading.taker_fee)

    def run_scan(self, dry_run: bool = False) -> ScanLog:
        """
        Сканирует рынки и создаёт НОВЫЕ серии Мартингейла.
        dry_run=True — только показать кандидатов, не сохранять.
        """
        active_series = self.store.get_active_series()
        active_count = len(active_series)
        deep_slots = compute_max_deep_slots(
            self.config.paper_trading.starting_balance,
            self.config.martingale.initial_bet_size,
            self.config.martingale.max_series_depth,
        )
        max_series = compute_dynamic_active_series(deep_slots)
        pending = self.store.get_series_pending_escalation()

        # Если слоты заняты и нет ожидающих эскалации — скан только для статистики
        # TODO: убрать после отладки, вернуть ранний выход
        slots_full = active_count >= max_series and not pending
        if slots_full:
            print(f"[Scan] Все слоты заняты ({active_count}/{max_series}), сканируем только для статистики...")

        print("[Scan] Загружаем рынки с Gamma API...")
        candidates = self.scanner.scan()
        ranked = self.scorer.rank(candidates, top_n=50)
        s = self.config.strategy
        if s.price_min is not None and s.price_max is not None:
            price_range_str = f"{s.price_min}–{s.price_max}"
        else:
            price_range_str = f"{s.target_price}±{s.price_tolerance}"
        print(f"[Scan] Найдено кандидатов в диапазоне {price_range_str}: {len(candidates)}")

        # Повторная эскалация серий, которые ждали рынка
        for series in pending:
            bets = self.store.get_series_bets(series.id)
            last_depth = max((b.series_depth for b in bets), default=0)
            self._escalate_series(series.id, last_depth, ranked)

        bets_placed = 0
        # Обновляем active_count после возможных эскалаций
        active_count = len(self.store.get_active_series())

        current_base_bet = self.config.martingale.initial_bet_size
        print(f"[Scan] Базовая ставка: ${current_base_bet:.2f}")

        total_deployed = self.store.get_total_invested_in_active_series()
        available_balance = self.config.paper_trading.starting_balance - total_deployed

        skipped_limit = 0
        observe_only = dry_run or slots_full
        for sc in ranked:
            if active_count >= max_series:
                if not self.store.already_bet(sc.market.id, sc.outcome):
                    skipped_limit += 1
                continue

            if self.store.already_bet(sc.market.id, sc.outcome):
                continue

            size = self.sizer.calculate(0, initial_bet_size=current_base_bet)

            if available_balance < size:
                print(f"[Scan] Недостаточно средств для новой серии (свободно ${available_balance:.2f} < ${size:.2f})")
                break

            entry_price = sc.price
            if self.config.paper_trading.check_liquidity:
                liq = self.clob.check_liquidity(sc.token_id, size)
                if not liq.available:
                    print(f"[Scan] Нет ликвидности для {sc.market.question[:50]} / {sc.outcome} — пропускаем")
                    continue
                entry_price = liq.avg_fill_price if liq.avg_fill_price > 0 else sc.price

            fee = round(size * self.config.paper_trading.taker_fee, 6)
            shares = size / entry_price
            total_cost = size + fee

            mode = "DRY" if dry_run else ("OBS" if slots_full else "BET")
            print(
                f"[{mode}] {sc.market.question[:60]}\n"
                f"         Исход: {sc.outcome} | Цена: ${entry_price:.4f} | "
                f"Мульт.: {1/entry_price:.1f}x | Скор: {sc.total_score:.3f}"
            )
            print(f"         Серия: новая (depth=0) | Ставка: ${size:.2f} + комиссия ${fee:.4f}")

            if not observe_only:
                series = BetSeries(
                    initial_bet_size=current_base_bet,
                    total_invested=total_cost,
                )
                self.store.create_series(series)
                bet = SimulatedBet(
                    market_id=sc.market.id,
                    market_question=sc.market.question,
                    outcome=sc.outcome,
                    token_id=sc.token_id,
                    entry_price=entry_price,
                    amount_usd=size,
                    fee_usd=fee,
                    shares=shares,
                    score=sc.total_score,
                    placed_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    market_end_date=sc.market.end_date,
                    series_id=series.id,
                    series_depth=0,
                )
                self.store.save_bet(bet)
                bets_placed += 1
                active_count += 1
                available_balance -= total_cost

        log = ScanLog(
            scanned_at=datetime.now(timezone.utc).replace(tzinfo=None),
            total_markets=len(candidates),
            candidates_found=len(candidates),
            bets_placed=bets_placed,
            skipped_limit=skipped_limit,
        )
        if not dry_run:
            self.store.save_scan_log(log)

        skipped_str = f" | Пропущено из-за лимита: {skipped_limit}" if skipped_limit > 0 else ""
        print(f"[Scan] Готово. Кандидатов: {len(ranked)} | Новых серий: {bets_placed}{skipped_str}")
        return log

    def check_resolutions(self) -> int:
        """
        Проверяет открытые ставки. При победе — серия выиграна.
        При проигрыше — эскалация (удвоение ставки на новом рынке).
        """
        open_bets = self.store.get_open_bets()
        if not open_bets:
            print("[Resolve] Открытых позиций нет.")
            return 0

        print(f"[Resolve] Проверяем {len(open_bets)} открытых позиций...")
        resolved_count = 0
        series_to_escalate = []  # Копим эскалации, делаем один скан в конце

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for bet in open_bets:
            market = self.gamma.fetch_market(bet.market_id)
            if market is None:
                continue

            # Рынок закрыт — или флаг closed, или цены определились и срок истёк
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
                f"[Resolve] {'✓ ВЫИГРЫШ' if status == 'won' else '✗ ПРОИГРЫШ'}: "
                f"{bet.market_question[:60]} / {bet.outcome} | P&L: ${pnl:+.2f}"
            )
            self.store.resolve_bet(bet.id, exit_price)
            resolved_count += 1

            if bet.series_id:
                if status == "won":
                    self._finish_series_won(bet.series_id)
                else:
                    series_to_escalate.append((bet.series_id, bet.series_depth))

        # Один скан для всех эскалаций
        ranked = []
        if series_to_escalate:
            print("[Resolve] Загружаем рынки для эскалации...")
            candidates = self.scanner.scan()
            ranked = self.scorer.rank(candidates, top_n=50)
            already_used = sum(
                1 for sc in ranked if self.store.already_bet(sc.market.id, sc.outcome)
            )
            free_candidates = len(ranked) - already_used
            print(f"[Resolve] Кандидатов: {len(ranked)} | Свободных (не в портфеле): {free_candidates} | Нужно эскалаций: {len(series_to_escalate)}")
            for series_id, depth in series_to_escalate:
                self._escalate_series(series_id, depth, ranked)

        # После эскалаций: активируем waiting серии под свободные глубокие слоты
        active_series_now = self.store.get_active_series()
        current_deep = len([s for s in active_series_now if s.current_depth >= 2])
        max_deep = compute_max_deep_slots(
            self.config.paper_trading.starting_balance,
            self.config.martingale.initial_bet_size,
            self.config.martingale.max_series_depth,
        )
        free_deep_slots = max_deep - current_deep
        if free_deep_slots > 0:
            waiting = self.store.get_waiting_series()
            for w in waiting[:free_deep_slots]:
                self.store.conn.execute(
                    "UPDATE bet_series SET status = 'active' WHERE id = ?", (w.id,)
                )
                self.store.conn.commit()
                print(f"[Resolve] ▶ Серия {w.id[:8]} из очереди активирована (глубоких: {current_deep+1}/{max_deep})")
                if not ranked:
                    candidates = self.scanner.scan()
                    ranked = self.scorer.rank(candidates, top_n=50)
                self._escalate_series(w.id, w.current_depth, ranked)
                current_deep += 1

        print(f"[Resolve] Закрыто позиций: {resolved_count}")
        return resolved_count

    def _finish_series_won(self, series_id: str) -> None:
        bets = self.store.get_series_bets(series_id)
        total_pnl = sum(b.pnl or 0 for b in bets)
        self.store.finish_series(series_id, "won", total_pnl)
        series = self.store.get_series_by_id(series_id)
        print(
            f"[Series] ✓ Серия завершена ПОБЕДОЙ | "
            f"Глубина: {series.current_depth} | Вложено: ${series.total_invested:.2f} | "
            f"P&L серии: ${total_pnl:+.2f}"
        )

    def _escalate_series(self, series_id: str, current_depth: int, ranked: List[ScoredCandidate]) -> None:
        """Проигрыш — пытаемся удвоить ставку, используя уже загруженных кандидатов."""
        series = self.store.get_series_by_id(series_id)
        if not series:
            return

        next_depth = current_depth + 1

        # Проверяем лимит глубоких слотов (depth≥2)
        if next_depth >= 2:
            active_series = self.store.get_active_series()
            current_deep = len([
                s for s in active_series
                if s.current_depth >= 2 and s.id != series_id
            ])
            max_deep = compute_max_deep_slots(
                self.config.paper_trading.starting_balance,
                series.initial_bet_size,
                self.config.martingale.max_series_depth,
            )
            if current_deep >= max_deep:
                self.store.set_series_waiting(series_id)
                print(
                    f"[Series] ⏸ Серия {series_id[:8]} в очереди "
                    f"(глубоких: {current_deep}/{max_deep})"
                )
                return

        if next_depth >= self.config.martingale.max_series_depth:
            bets = self.store.get_series_bets(series_id)
            total_pnl = sum(b.pnl or 0 for b in bets)
            self.store.finish_series(series_id, "abandoned", total_pnl)
            print(
                f"[Series] ✗ Серия БРОШЕНА (лимит глубины {self.config.martingale.max_series_depth}) | "
                f"Вложено: ${series.total_invested:.2f} | P&L: ${total_pnl:+.2f}"
            )
            return

        # Проверяем виртуальный баланс
        size_needed = self.sizer.calculate(next_depth, initial_bet_size=series.initial_bet_size)
        total_deployed = self.store.get_total_invested_in_active_series()
        available = self.config.paper_trading.starting_balance - total_deployed
        if available < size_needed:
            self.store.set_series_waiting(series_id)
            print(
                f"[Series] ⏸ Серия {series_id[:8]} ждёт пополнения "
                f"(свободно ${available:.2f} < нужно ${size_needed:.2f})"
            )
            return

        placed = False
        for sc in ranked:
            if self.store.already_bet(sc.market.id, sc.outcome):
                continue

            entry_price = sc.price
            if self.config.paper_trading.check_liquidity:
                est_size = self.sizer.calculate(
                    next_depth,
                    initial_bet_size=series.initial_bet_size,
                )
                liq = self.clob.check_liquidity(sc.token_id, est_size)
                if not liq.available:
                    continue
                entry_price = liq.avg_fill_price if liq.avg_fill_price > 0 else sc.price

            size = self.sizer.calculate(
                next_depth,
                initial_bet_size=series.initial_bet_size,
            )

            fee = round(size * self.config.paper_trading.taker_fee, 6)
            shares = size / entry_price
            total_cost = size + fee

            bet = SimulatedBet(
                market_id=sc.market.id,
                market_question=sc.market.question,
                outcome=sc.outcome,
                token_id=sc.token_id,
                entry_price=entry_price,
                amount_usd=size,
                fee_usd=fee,
                shares=shares,
                score=sc.total_score,
                placed_at=datetime.now(timezone.utc).replace(tzinfo=None),
                market_end_date=sc.market.end_date,
                series_id=series_id,
                series_depth=next_depth,
            )
            self.store.save_bet(bet)
            self.store.update_series_depth(series_id, next_depth, total_cost)
            print(
                f"[Series] ↑ Эскалация: depth={next_depth} | Ставка: ${size:.2f} | "
                f"Цена: ${entry_price:.4f} | {sc.market.question[:50]} / {sc.outcome}"
            )
            placed = True
            break

        if not placed:
            print(f"[Series] ⏳ Нет рынка для эскалации depth={next_depth} — повторим в следующем цикле")
