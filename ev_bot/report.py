"""Rich-отчёт EV-анализа по ценовым бакетам."""
from __future__ import annotations

from rich.console import Console
from rich.table import Table

from ev_bot.filter import EVFilter, PRICE_STEP


def show_ev_analysis(ev_filter: EVFilter) -> None:
    console = Console()
    n = len(ev_filter.markets)

    if not ev_filter.buckets:
        console.print(
            f"[yellow]Нет бакетов с достаточной выборкой "
            f"(записей: {n}, мин.: {ev_filter.min_samples})[/yellow]"
        )
        console.print("[dim]Загрузите данные: python -m ev_bot fetch --limit 10000[/dim]")
        return

    table = Table(
        title=f"EV-анализ  ({n} записей, шаг цены {PRICE_STEP:.2f}, мин. выборка {ev_filter.min_samples})",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Цена", width=13)
    table.add_column("Записей", justify="right", width=9)
    table.add_column("Win%", justify="right", width=8)
    table.add_column("Breakeven", justify="right", width=11)
    table.add_column("EV/$", justify="right", width=10)
    table.add_column("", width=10)

    for b in ev_filter.buckets:
        price_str = f"{b.price_min:.2f}–{b.price_max:.2f}"
        win_pct = f"{b.win_rate * 100:.1f}%"
        be_pct = f"{b.breakeven_wr * 100:.1f}%"

        if b.ev_per_dollar > 0:
            ev_str = f"[bold green]{b.ev_per_dollar:+.3f}[/bold green]"
            action = "[green]← ставим[/green]"
        else:
            ev_str = f"[red]{b.ev_per_dollar:+.3f}[/red]"
            action = "[red]✗[/red]"

        table.add_row(price_str, str(b.total), win_pct, be_pct, ev_str, action)

    console.print(table)

    positive = [b for b in ev_filter.buckets if b.ev_per_dollar > 0]
    if positive:
        console.print(f"\n[bold green]Активные фильтры:[/bold green] {ev_filter.summary()}")
        console.print(
            "[dim]⚠  EV зависит от качества данных. "
            "Чем больше записей в бакете — тем надёжнее оценка.[/dim]"
        )
    else:
        console.print("\n[bold red]Нет +EV бакетов.[/bold red]")
