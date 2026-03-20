from src.api.clob import OrderBook, OrderLevel

from arb_bot.engine import fill_cost_for_shares


def test_fill_cost_for_shares_consumes_multiple_levels() -> None:
    book = OrderBook(
        bids=[],
        asks=[
            OrderLevel(price=0.48, size=10),
            OrderLevel(price=0.49, size=5),
        ],
    )

    spent, filled = fill_cost_for_shares(book, 12)

    assert filled == 12
    assert spent == 10 * 0.48 + 2 * 0.49


def test_fill_cost_for_shares_handles_missing_book() -> None:
    spent, filled = fill_cost_for_shares(None, 10)

    assert spent == 0.0
    assert filled == 0.0
