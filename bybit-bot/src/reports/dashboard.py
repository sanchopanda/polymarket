from __future__ import annotations

from typing import List

from rich.console import Console
from rich.table import Table
from rich import box

from src.db.models import Series, Trade
from src.db.store import Store
from src.config import ReportsConfig

console = Console()


def _pnl_color(val: float) -> str:
    if val > 0:
        return f"[green]+${val:.4f}[/green]"
    elif val < 0:
        return f"[red]-${abs(val):.4f}[/red]"
    return f"${val:.4f}"


def show_summary(store: Store, cfg: ReportsConfig) -> None:
    all_series = store.get_all_series()
    active = [s for s in all_series if s.status == "active"]
    won = [s for s in all_series if s.status == "won"]
    lost = [s for s in all_series if s.status in ("abandoned", "lost")]

    total_invested = sum(s.total_invested for s in all_series)
    realized_pnl = sum(s.total_pnl for s in all_series if s.status != "active")

    open_trades = store.get_open_trades()
    open_margin = sum(t.margin_usdt for t in open_trades)

    free_cash = cfg.starting_balance - total_invested
    portfolio = cfg.starting_balance + realized_pnl

    console.print("\n[bold cyan]══════════════ Портфель ══════════════[/bold cyan]")
    t = Table(show_header=False, box=box.SIMPLE)
    t.add_column("", style="dim")
    t.add_column("")

    t.add_row("Начальный баланс", f"${cfg.starting_balance:.2f}")
    t.add_row("Вложено всего", f"${total_invested:.4f}")
    t.add_row("  в открытых позициях", f"${open_margin:.4f}")
    t.add_row("Реализованный P&L", _pnl_color(realized_pnl))
    t.add_row("Портфель (баланс + P&L)", f"${portfolio:.4f}")
    t.add_row("Свободных средств", f"${free_cash:.4f}")

    console.print(t)

    console.print("[bold cyan]══════════════ Серии ══════════════[/bold cyan]")
    s = Table(show_header=False, box=box.SIMPLE)
    s.add_column("", style="dim")
    s.add_column("")
    s.add_row("Активных", str(len(active)))
    s.add_row("Выиграно", f"[green]{len(won)}[/green]")
    s.add_row("Брошено", f"[red]{len(lost)}[/red]")
    s.add_row("Всего", str(len(all_series)))
    console.print(s)


def show_series(store: Store) -> None:
    active_series = store.get_active_series()
    if not active_series:
        console.print("[dim]Активных серий нет.[/dim]")
        return

    t = Table(title=f"Активные серии ({len(active_series)})", box=box.SIMPLE_HEAVY)
    t.add_column("ID", style="dim", no_wrap=True)
    t.add_column("Символ")
    t.add_column("Глубина", justify="right")
    t.add_column("Вложено", justify="right")
    t.add_column("Начало", no_wrap=True)

    for s in active_series:
        t.add_row(
            s.id[:8],
            s.symbol,
            str(s.current_depth),
            f"${s.total_invested:.4f}",
            s.started_at.strftime("%m-%d %H:%M"),
        )

    console.print(t)


def show_open_positions(store: Store) -> None:
    open_trades = store.get_open_trades()
    if not open_trades:
        console.print("[dim]Открытых позиций нет.[/dim]")
        return

    t = Table(title="Открытые позиции", box=box.SIMPLE_HEAVY)
    t.add_column("Символ")
    t.add_column("Сторона")
    t.add_column("Кол-во", justify="right")
    t.add_column("Вход", justify="right")
    t.add_column("TP", justify="right")
    t.add_column("SL", justify="right")
    t.add_column("Маржа", justify="right")
    t.add_column("Depth", justify="right")
    t.add_column("Открыта", no_wrap=True)

    for tr in open_trades:
        side_col = "[green]Buy[/green]" if tr.side == "Buy" else "[red]Sell[/red]"
        t.add_row(
            tr.symbol,
            side_col,
            str(tr.qty),
            f"${tr.entry_price:.4f}",
            f"${tr.take_profit:.4f}",
            f"${tr.stop_loss:.4f}",
            f"${tr.margin_usdt:.4f}",
            str(tr.series_depth),
            tr.opened_at.strftime("%m-%d %H:%M:%S"),
        )

    console.print(t)


def show_history(store: Store, limit: int = 30) -> None:
    all_trades = store.get_all_trades()
    closed = [t for t in all_trades if t.status != "open"][:limit]
    if not closed:
        console.print("[dim]Закрытых сделок нет.[/dim]")
        return

    t = Table(title=f"История сделок (последние {limit})", box=box.SIMPLE_HEAVY)
    t.add_column("Символ")
    t.add_column("Сторона")
    t.add_column("Статус")
    t.add_column("Вход", justify="right")
    t.add_column("Выход", justify="right")
    t.add_column("P&L", justify="right")
    t.add_column("Depth", justify="right")
    t.add_column("Закрыта", no_wrap=True)

    for tr in closed:
        side_col = "[green]Buy[/green]" if tr.side == "Buy" else "[red]Sell[/red]"
        status_col = "[green]won[/green]" if tr.status == "won" else "[red]lost[/red]"
        closed_at = tr.closed_at.strftime("%m-%d %H:%M") if tr.closed_at else "—"
        t.add_row(
            tr.symbol,
            side_col,
            status_col,
            f"${tr.entry_price:.4f}",
            f"${tr.exit_price:.4f}" if tr.exit_price else "—",
            _pnl_color(tr.pnl),
            str(tr.series_depth),
            closed_at,
        )

    console.print(t)


def show_depth_stats(store: Store) -> None:
    all_series = store.get_all_series()
    finished = [s for s in all_series if s.status != "active"]
    if not finished:
        console.print("[dim]Завершённых серий нет.[/dim]")
        return

    depth_counts: dict[int, dict[str, int]] = {}
    for s in finished:
        d = s.current_depth
        if d not in depth_counts:
            depth_counts[d] = {"won": 0, "total": 0}
        depth_counts[d]["total"] += 1
        if s.status == "won":
            depth_counts[d]["won"] += 1

    total_finished = len(finished)
    t = Table(title="Статистика по глубине серий", box=box.SIMPLE_HEAVY)
    t.add_column("Глубина", justify="right")
    t.add_column("Всего", justify="right")
    t.add_column("Выиграно", justify="right")
    t.add_column("% от всех", justify="right")
    t.add_column("% выигрышей", justify="right")

    for depth in sorted(depth_counts.keys()):
        d = depth_counts[depth]
        won_pct = d["won"] / d["total"] * 100 if d["total"] else 0
        all_pct = d["total"] / total_finished * 100 if total_finished else 0
        t.add_row(
            str(depth),
            str(d["total"]),
            str(d["won"]),
            f"{all_pct:.1f}%",
            f"{won_pct:.1f}%",
        )

    console.print(t)


def show_best_worst(store: Store, n: int = 5) -> None:
    all_series = [s for s in store.get_all_series() if s.status in ("won", "abandoned", "lost")]
    if not all_series:
        console.print("[dim]Нет завершённых серий.[/dim]")
        return

    # Средний ROI
    rois = [s.total_pnl / s.initial_margin * 100 for s in all_series if s.initial_margin > 0]
    if rois:
        avg_roi = sum(rois) / len(rois)
        avg_color = "green" if avg_roi >= 0 else "red"
        console.print(f"\n[bold]Средний ROI (от нач. маржи):[/bold] [{avg_color}]{avg_roi:.1f}%[/{avg_color}]  ({len(rois)} серий)")

    all_series.sort(key=lambda s: s.total_pnl, reverse=True)
    winners = [s for s in all_series if s.total_pnl > 0][:n]
    losers = [s for s in reversed(all_series) if s.total_pnl <= 0][:n]

    if winners:
        console.print(f"\n[bold green]Лучшие серии (топ {n}):[/bold green]")
        for s in winners:
            roi = s.total_pnl / s.initial_margin * 100 if s.initial_margin > 0 else 0
            console.print(
                f"  {_pnl_color(s.total_pnl)} | {s.symbol} | маржа ${s.initial_margin:.4f} | "
                f"вложено ${s.total_invested:.4f} | ROI {roi:.0f}% | depth={s.current_depth}"
            )

    if losers:
        console.print(f"\n[bold red]Худшие серии (топ {n}):[/bold red]")
        for s in losers:
            roi = s.total_pnl / s.initial_margin * 100 if s.initial_margin > 0 else 0
            console.print(
                f"  {_pnl_color(s.total_pnl)} | {s.symbol} | маржа ${s.initial_margin:.4f} | "
                f"вложено ${s.total_invested:.4f} | ROI {roi:.0f}% | depth={s.current_depth}"
            )


def cmd_dashboard(store: Store, cfg: ReportsConfig) -> None:
    show_summary(store, cfg)
    show_depth_stats(store)
    show_best_worst(store)
    show_series(store)
    show_open_positions(store)
    show_history(store, cfg.max_rows)
