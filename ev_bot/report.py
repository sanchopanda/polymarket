"""Rich-отчёт EV-анализа по бакетам."""
from __future__ import annotations

from rich.console import Console
from rich.table import Table

from ev_bot.filter import EVBucket, EVFilter, VOLUME_TIERS, PRICE_STEP


def _fmt_vol(vol_min: float) -> str:
    def _k(v: int) -> str:
        return f"{v // 1000}k" if v >= 1000 else str(v)
    try:
        idx = VOLUME_TIERS.index(int(vol_min))
    except ValueError:
        return f"≥{vol_min:,.0f}"
    if idx == 0:
        return f"<{_k(VOLUME_TIERS[1])}"
    elif idx == len(VOLUME_TIERS) - 1:
        return f"≥{_k(VOLUME_TIERS[idx])}"
    else:
        return f"{_k(VOLUME_TIERS[idx])}–{_k(VOLUME_TIERS[idx + 1])}"


def show_ev_analysis(ev_filter: EVFilter) -> None:
    """Выводит таблицу EV-анализа."""
    console = Console()
    n = len(ev_filter.markets)

    if not ev_filter.buckets:
        console.print(
            f"[yellow]Нет бакетов с достаточной выборкой "
            f"(рынков: {n}, мин.: {ev_filter.min_samples})[/yellow]"
        )
        console.print("[dim]Запустите: python -m src.main fetch --limit 10000[/dim]")
        return

    table = Table(
        title=f"EV-анализ по бакетам  ({n} закрытых рынков, мин. выборка {ev_filter.min_samples})",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Цена", width=13)
    table.add_column("Объём", width=12)
    table.add_column("Рынков", justify="right", width=8)
    table.add_column("Win%", justify="right", width=8)
    table.add_column("EV/$", justify="right", width=10)
    table.add_column("", width=10)

    for b in ev_filter.buckets:
        price_str = f"{b.price_min:.2f}–{b.price_max:.2f}"
        vol_str = _fmt_vol(b.volume_min)
        win_pct = f"{b.win_rate * 100:.1f}%"

        if b.ev_per_dollar > 0:
            ev_str = f"[green]{b.ev_per_dollar:+.3f}[/green]"
            action = "[green]← ставим[/green]"
        else:
            ev_str = f"[red]{b.ev_per_dollar:+.3f}[/red]"
            action = "[red]✗[/red]"

        table.add_row(price_str, vol_str, str(b.total), win_pct, ev_str, action)

    console.print(table)

    positive = [b for b in ev_filter.buckets if b.ev_per_dollar > 0]
    if positive:
        console.print(f"\n[bold green]Активные фильтры:[/bold green] {ev_filter.summary()}")
        console.print(
            "[dim]⚠ EV на малых выборках может быть завышен. "
            "Используйте --min-samples 100+ для большей уверенности.[/dim]"
        )
    else:
        console.print("\n[bold red]Нет +EV бакетов. Попробуйте снизить --min-samples.[/bold red]")
