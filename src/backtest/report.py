from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from src.backtest.fetcher import HistoricalMarket
from src.backtest.simulator import BacktestResult


console = Console()


def show_backtest_report(
    result: BacktestResult,
    markets_count: int,
    use_price_history: bool,
    starting_balance: float = 0.0,
) -> None:
    """Выводит результаты бэктеста в Rich-таблице."""

    pnl_color = "green" if result.total_pnl >= 0 else "red"
    roi_color = "green" if result.roi >= 0 else "red"
    price_src = "CLOB история" if use_price_history else "0.50 (фикс.)"

    # Итоговая сводка
    bal_color = "green" if result.final_balance >= starting_balance else "red"
    summary_lines = [
        f"[bold]Рынков загружено:[/bold] {markets_count}   "
        f"[bold]Источник цены входа:[/bold] {price_src}",
        "",
        f"  Серий всего:    [bold]{result.total_series}[/bold]",
        f"  Выиграно:       [bold green]{result.won_series}[/bold green]",
        f"  Брошено:        [bold red]{result.abandoned_series}[/bold red]",
        f"  Win rate:       [bold]{result.win_rate * 100:.1f}%[/bold]",
        "",
        f"  Стартовый баланс: [bold]${starting_balance:.2f}[/bold]",
        f"  Финальный баланс: [{bal_color}][bold]${result.final_balance:.2f}[/bold][/{bal_color}]",
        f"  P&L:              [{pnl_color}][bold]${result.total_pnl:+.2f}[/bold][/{pnl_color}]",
        f"  ROI:              [{roi_color}][bold]{result.roi:+.2f}%[/bold][/{roi_color}]",
    ]
    console.print(Panel("\n".join(summary_lines), title="[bold cyan]Результаты бэктеста[/bold cyan]", expand=False))

    # Распределение по глубинам
    if result.depth_distribution:
        table = Table(title="Глубина серий", box=box.SIMPLE_HEAD)
        table.add_column("Глубина", style="cyan", justify="center")
        table.add_column("Кол-во серий", justify="right")
        table.add_column("% от всех", justify="right")

        for depth in sorted(result.depth_distribution.keys()):
            count = result.depth_distribution[depth]
            pct = count / result.total_series * 100 if result.total_series > 0 else 0
            table.add_row(str(depth), str(count), f"{pct:.1f}%")

        console.print(table)

    # Топ-5 худших серий (по P&L)
    if result.series_results:
        worst = sorted(result.series_results, key=lambda s: s.pnl)[:5]
        t2 = Table(title="Топ-5 худших серий", box=box.SIMPLE_HEAD)
        t2.add_column("Глубина", justify="center")
        t2.add_column("Инвестировано", justify="right")
        t2.add_column("P&L", justify="right")
        t2.add_column("Статус", justify="center")
        for s in worst:
            color = "green" if s.won else "red"
            status = "Выиграна" if s.won else "Брошена"
            t2.add_row(
                str(s.depth_reached),
                f"${s.total_invested:.2f}",
                f"[{color}]${s.pnl:+.2f}[/{color}]",
                f"[{color}]{status}[/{color}]",
            )
        console.print(t2)
