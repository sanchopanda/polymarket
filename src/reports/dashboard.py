from __future__ import annotations

from typing import List

from rich.console import Console
from rich.table import Table
from rich import box

from src.db.models import BetSeries, PortfolioSnapshot, SimulatedBet
from src.db.store import Store

console = Console()


def _fmt_pnl(val: float) -> str:
    color = "green" if val >= 0 else "red"
    return f"[{color}]${val:+.2f}[/{color}]"


def _fmt_status(status: str) -> str:
    if status == "won":
        return "[green]✓ выигрыш[/green]"
    if status == "lost":
        return "[red]✗ проигрыш[/red]"
    if status == "abandoned":
        return "[red]✗ брошена[/red]"
    if status == "cancelled":
        return "[dim]отменена[/dim]"
    if status == "active":
        return "[yellow]активна[/yellow]"
    return f"[yellow]{status}[/yellow]"


def _fmt_bet_status(status: str) -> str:
    if status == "won":
        return "[green]✓ выигрыш[/green]"
    if status == "lost":
        return "[red]✗ проигрыш[/red]"
    return "[yellow]открыта[/yellow]"


class Dashboard:
    def __init__(self, store: Store, max_rows: int = 50, starting_balance: float = 50.0,
                 max_series_depth: int = 6, real: bool = False,
                 current_wallet_balance: float | None = None) -> None:
        self.store = store
        self.max_rows = max_rows
        self.starting_balance = starting_balance
        self.max_series_depth = max_series_depth
        self.real = real
        self.current_wallet_balance = current_wallet_balance  # on-chain баланс (только real)

    def show_summary(self) -> None:
        stats = self.store.get_portfolio_stats()
        all_bets = self.store.get_all_bets()
        total_bets = len(all_bets)
        total_spent = sum(b.amount_usd + b.fee_usd for b in all_bets)
        total_fees = sum(b.fee_usd for b in all_bets)
        open_bets = [b for b in all_bets if b.status == "open"]
        open_potential = sum(b.shares for b in open_bets)
        open_invested = sum(b.amount_usd + b.fee_usd for b in open_bets)

        title = "РЕАЛЬНЫЙ ПОРТФЕЛЬ" if self.real else "ВИРТУАЛЬНЫЙ ПОРТФЕЛЬ"
        console.print(f"\n[bold cyan]═══ {title} ═══[/bold cyan]")

        if self.real and self.current_wallet_balance is not None:
            # Реальный режим: текущий баланс = on-chain кошелёк + вложено во все активные серии
            total_in_series = self.store.get_total_invested_in_active_series()
            current_total = self.current_wallet_balance + total_in_series
            pnl = current_total - self.starting_balance
            best_case = self.current_wallet_balance + open_potential
            worst_case = self.current_wallet_balance

            console.print(f"  Начальный баланс:      ${self.starting_balance:.2f}")
            console.print(f"  Текущий баланс:        ${current_total:.2f}  ({_fmt_pnl(pnl)})")
            console.print(f"    свободно в кошельке: ${self.current_wallet_balance:.2f}")
            console.print(f"    в активных сериях:   ${total_in_series:.2f}  ({stats.active_series_count} серий, {len(open_bets)} открытых)")
            console.print(f"  Если все выиграют:     [green]${best_case:.2f}[/green]  ({_fmt_pnl(best_case - self.starting_balance)})")
            console.print(f"  Если все проиграют:    [red]${worst_case:.2f}[/red]  ({_fmt_pnl(worst_case - self.starting_balance)})")
        else:
            # Paper trading: виртуальный портфель
            free_cash = self.starting_balance + stats.total_pnl_realized - open_invested
            best_case = free_cash + open_potential
            worst_case = free_cash
            portfolio_value = self.starting_balance + stats.total_pnl_realized
            portfolio_color = "green" if portfolio_value >= self.starting_balance else "red"

            console.print(f"  Начальный баланс:      ${self.starting_balance:.2f}")
            console.print(f"  Вложено в ставки:      ${open_invested:.2f}  ({len(open_bets)} открытых позиций)")
            console.print(f"  Свободные средства:    ${free_cash:.2f}")
            console.print(f"  Реализованный P&L:     {_fmt_pnl(stats.total_pnl_realized)}")
            console.print(f"  Портфель (реализов.):  [{portfolio_color}]${portfolio_value:.2f}[/{portfolio_color}]  ({_fmt_pnl(portfolio_value - self.starting_balance)})")
            console.print(f"  Если все выиграют:     [green]${best_case:.2f}[/green]  ({_fmt_pnl(best_case - self.starting_balance)})")
            console.print(f"  Если все проиграют:    [red]${worst_case:.2f}[/red]  ({_fmt_pnl(worst_case - self.starting_balance)})")

        # Статистика серий
        all_series = self.store.get_all_series()
        won_series = sum(1 for s in all_series if s.status == "won")
        abandoned_series = sum(1 for s in all_series if s.status == "abandoned")
        cancelled_series = sum(1 for s in all_series if s.status == "cancelled")

        console.print("\n[bold cyan]═══ ПОРТФЕЛЬ (Мартингейл) ═══[/bold cyan]")
        console.print(f"  Всего ставок:          {total_bets}")
        console.print(f"  Открытых позиций:      {stats.open_positions}")
        console.print(f"  Закрытых позиций:      {stats.win_count + stats.loss_count}")
        console.print(f"  Потрачено всего:       ${total_spent:.2f}")
        console.print(f"    из них комиссии:     ${total_fees:.4f}")
        console.print(f"  Задеплоено (открытые): ${stats.total_deployed:.2f}")
        console.print(f"  Реализованный P&L:     {_fmt_pnl(stats.total_pnl_realized)}")
        if open_bets:
            console.print(f"  Потенциал (если все открытые выиграют): [green]${open_potential:.2f}[/green]")

        console.print(f"\n  [bold]Серии:[/bold]")
        console.print(f"    Активных:  {stats.active_series_count}")
        console.print(f"    Выигранных: {won_series}")
        console.print(f"    Брошенных:  {abandoned_series}")
        if cancelled_series:
            console.print(f"    Отменённых: {cancelled_series}")
        console.print(f"    Всего:      {len(all_series)}")

        if stats.total_bets > 0:
            console.print(f"\n  Побед / Поражений:   {stats.win_count} / {stats.loss_count}")
            console.print(f"  Winrate:             {stats.win_rate * 100:.1f}%")
            net_spent_closed = sum(b.amount_usd + b.fee_usd for b in all_bets if b.status != "open")
            roi = (stats.total_pnl_realized / net_spent_closed * 100) if net_spent_closed > 0 else 0
            console.print(f"  ROI (закрытые):      {_fmt_pnl(stats.total_pnl_realized)} / ${net_spent_closed:.2f} = [bold]{roi:.1f}%[/bold]")

        # Среднее время серии и темп
        from datetime import datetime, timezone
        finished = [s for s in all_series if s.finished_at and s.status in ("won", "abandoned")]
        if finished:
            avg_sec = sum((s.finished_at - s.started_at).total_seconds() for s in finished) / len(finished)
            if avg_sec < 3600:
                avg_dur_str = f"{avg_sec/60:.0f} мин."
            else:
                avg_dur_str = f"{avg_sec/3600:.1f} ч."
            now_dt = datetime.now(timezone.utc).replace(tzinfo=None)
            first_started = min(s.started_at for s in all_series)
            total_hours = max((now_dt - first_started).total_seconds() / 3600, 0.01)
            rate = len(finished) / total_hours
            avg_skipped = self.store.get_avg_skipped_limit()
            console.print(f"\n  Среднее время серии: [cyan]{avg_dur_str}[/cyan]")
            console.print(f"  Темп:                [cyan]{rate:.1f} серий/час[/cyan]")
            if avg_skipped > 0:
                console.print(f"  Упущено (среднее):   [yellow]{avg_skipped:.1f} кандидатов/скан[/yellow]")

    def show_real_cash_flow(self) -> None:
        """Динамика кошелька и история redeem (только real trading)."""
        redeems = self.store.get_redeems()
        snapshots = self.store.get_wallet_snapshots(limit=20)

        if redeems:
            total_redeemed = sum(r.amount_usd for r in redeems)
            table = Table(title=f"История redeem (всего ${total_redeemed:.2f})", box=box.SIMPLE)
            table.add_column("Дата/время", max_width=16)
            table.add_column("Сумма", justify="right")
            table.add_column("Рынок", max_width=55)
            table.add_column("TX", max_width=18)
            for r in redeems[:self.max_rows]:
                table.add_row(
                    r.redeemed_at.strftime("%m-%d %H:%M"),
                    f"[green]${r.amount_usd:.2f}[/green]",
                    r.market_question[:55],
                    r.tx_hash[:18] + "...",
                )
            console.print(table)
        else:
            console.print("\n[yellow]История redeem пуста.[/yellow]")

        if snapshots:
            snapshots_asc = list(reversed(snapshots))
            table2 = Table(title="Динамика баланса кошелька", box=box.SIMPLE)
            table2.add_column("Дата/время", max_width=16)
            table2.add_column("USDC.e", justify="right")
            table2.add_column("Изменение", justify="right")
            prev = None
            for s in snapshots_asc:
                delta = ""
                if prev is not None:
                    diff = s.balance_usdc - prev
                    delta = f"[green]+${diff:.2f}[/green]" if diff >= 0 else f"[red]-${abs(diff):.2f}[/red]"
                table2.add_row(
                    s.recorded_at.strftime("%m-%d %H:%M:%S"),
                    f"${s.balance_usdc:.2f}",
                    delta,
                )
                prev = s.balance_usdc
            console.print(table2)

    def show_depth_stats(self) -> None:
        """Статистика побед по глубине серии."""
        all_series = self.store.get_all_series()
        won = [s for s in all_series if s.status == "won"]
        if not won:
            console.print("\n[yellow]Нет выигранных серий для статистики по глубине.[/yellow]")
            return

        total_series = len(all_series)
        from collections import Counter
        depth_counts = Counter(s.current_depth for s in won)
        max_depth = max(depth_counts) if depth_counts else 0

        table = Table(title="Победы по кол-ву ставок в серии", box=box.SIMPLE)
        table.add_column("Ставок", justify="right")
        table.add_column("Побед", justify="right")
        table.add_column("% от всех серий", justify="right")
        table.add_column("% от побед", justify="right")

        total_won = len(won)
        for depth in range(max_depth + 1):
            count = depth_counts.get(depth, 0)
            pct_all = count / total_series * 100 if total_series > 0 else 0
            pct_won = count / total_won * 100 if total_won > 0 else 0
            table.add_row(
                str(depth + 1),
                str(count),
                f"{pct_all:.1f}%",
                f"{pct_won:.1f}%",
            )

        console.print(table)

    def show_series(self) -> None:
        """Таблица только активных серий."""
        active_series = self.store.get_active_series()
        if not active_series:
            console.print("\n[yellow]Активных серий нет.[/yellow]")
            return

        table = Table(title=f"Активные серии ({len(active_series)})", box=box.SIMPLE)
        table.add_column("ID", max_width=8)
        table.add_column("Ставок", justify="right")
        table.add_column("Нач. ставка", justify="right")
        table.add_column("Вложено", justify="right")
        table.add_column("Начало", max_width=16)

        for s in active_series[:self.max_rows]:
            table.add_row(
                s.id[:8],
                str(s.current_depth + 1),
                f"${s.initial_bet_size:.2f}",
                f"${s.total_invested:.2f}",
                s.started_at.strftime("%Y-%m-%d %H:%M"),
            )

        console.print(table)

    def show_open_positions(self) -> None:
        bets = self.store.get_open_bets()
        if not bets:
            console.print("\n[yellow]Открытых позиций нет.[/yellow]")
            return

        table = Table(title=f"Открытые позиции ({len(bets)})", box=box.SIMPLE)
        table.add_column("Вопрос", max_width=40)
        table.add_column("Исход", max_width=12)
        table.add_column("Цена", justify="right")
        table.add_column("Ставка $", justify="right")
        table.add_column("Серия", justify="right")
        table.add_column("До экспирации", justify="right")

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        for b in bets[:self.max_rows]:
            days_left = ""
            if b.market_end_date:
                delta = b.market_end_date - now
                total_minutes = int(delta.total_seconds() / 60)
                if total_minutes < 0:
                    days_left = f"[red]истёк {-total_minutes}м назад[/red]"
                elif total_minutes < 60:
                    days_left = f"{total_minutes}м"
                elif total_minutes < 1440:
                    days_left = f"{total_minutes // 60}ч {total_minutes % 60}м"
                else:
                    days_left = f"{delta.days}д"
            series_info = f"#{b.series_depth + 1}" if b.series_id else "—"
            table.add_row(
                b.market_question[:40],
                b.outcome[:12],
                f"${b.entry_price:.4f}",
                f"${b.amount_usd:.2f}",
                series_info,
                days_left,
            )

        console.print(table)

    def show_history(self, limit: int = 50) -> None:
        bets = [b for b in self.store.get_all_bets() if b.status != "open"]
        if not bets:
            console.print("\n[yellow]Нет закрытых ставок.[/yellow]")
            return

        table = Table(title=f"История ставок ({len(bets)} закрытых)", box=box.SIMPLE)
        table.add_column("Вопрос", max_width=35)
        table.add_column("Исход", max_width=12)
        table.add_column("Статус", max_width=12)
        table.add_column("Цена входа", justify="right")
        table.add_column("Ставка $", justify="right")
        table.add_column("Серия", justify="right")
        table.add_column("P&L", justify="right")

        for b in bets[:limit]:
            pnl_str = _fmt_pnl(b.pnl) if b.pnl is not None else "—"
            series_info = f"#{b.series_depth + 1}" if b.series_id else "—"
            table.add_row(
                b.market_question[:35],
                b.outcome[:12],
                _fmt_bet_status(b.status),
                f"${b.entry_price:.4f}",
                f"${b.amount_usd:.2f}",
                series_info,
                pnl_str,
            )

        console.print(table)

    def show_best_worst(self, n: int = 5) -> None:
        all_series = [s for s in self.store.get_all_series() if s.status in ("won", "abandoned")]
        if not all_series:
            console.print("\n[yellow]Нет завершённых серий для отображения.[/yellow]")
            return

        # Средний ROI
        rois = [s.total_pnl / s.initial_bet_size * 100 for s in all_series if s.initial_bet_size > 0]
        if rois:
            avg_roi = sum(rois) / len(rois)
            avg_color = "green" if avg_roi >= 0 else "red"
            console.print(f"\n[bold]Средний ROI (от нач. ставки):[/bold] [{avg_color}]{avg_roi:.1f}%[/{avg_color}]  ({len(rois)} серий)")

        all_series.sort(key=lambda s: s.total_pnl, reverse=True)
        winners = [s for s in all_series if s.total_pnl > 0][:n]
        losers = [s for s in reversed(all_series) if s.total_pnl <= 0][:n]

        if winners:
            console.print("\n[bold green]Лучшие серии:[/bold green]")
            for s in winners:
                bets = self.store.get_series_bets(s.id)
                winning_bet = next((b for b in bets if b.status == "won"), None)
                label = f"{winning_bet.market_question[:45]} / {winning_bet.outcome}" if winning_bet else "—"
                roi = s.total_pnl / s.initial_bet_size * 100 if s.initial_bet_size > 0 else 0
                console.print(
                    f"  {_fmt_pnl(s.total_pnl)} | нач. ставка ${s.initial_bet_size:.2f} | вложено ${s.total_invested:.2f} | "
                    f"ROI {roi:.0f}% | ставок {s.current_depth + 1} | {label}"
                )

        if losers:
            console.print("\n[bold red]Худшие серии:[/bold red]")
            for s in losers:
                bets = self.store.get_series_bets(s.id)
                label = bets[-1].market_question[:45] if bets else "—"
                roi = s.total_pnl / s.initial_bet_size * 100 if s.initial_bet_size > 0 else 0
                console.print(
                    f"  {_fmt_pnl(s.total_pnl)} | нач. ставка ${s.initial_bet_size:.2f} | вложено ${s.total_invested:.2f} | "
                    f"ROI {roi:.0f}% | ставок {s.current_depth + 1} | {label}"
                )
