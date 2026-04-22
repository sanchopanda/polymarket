from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from jump_paper_bot.db import JumpPaperDB
from jump_paper_bot.engine import JumpPaperEngine
from jump_paper_bot.telegram_notify import JumpTelegramNotifier
from oracle_arb_bot.models import OracleMarket
from src.api.clob import OrderBook, OrderLevel


class DummyScanner:
    def __init__(self) -> None:
        self.callback = None
        self._pm_feed = SimpleNamespace(client=SimpleNamespace(fetch_market=lambda market_id: None))

    def set_pm_price_callback(self, cb):
        self.callback = cb

    def scan_and_subscribe(self):
        return []

    def stop(self):
        return None


class DummyClob:
    def __init__(self, book: OrderBook | None) -> None:
        self.book = book

    def get_orderbook(self, token_id: str) -> OrderBook | None:
        return self.book


class DummyNotifier:
    def __init__(self) -> None:
        self.opens = []
        self.resolves = []

    def notify_open(self, **kwargs):
        self.opens.append(kwargs)
        return 777

    def notify_resolve(self, **kwargs):
        self.resolves.append(kwargs)


def make_config(db_path: str) -> dict:
    return {
        "strategy": {
            "paper_stake_usd": 5.0,
            "required_depth_multiple": 2.0,
            "lookback_seconds": 10,
            "jump_cents": 0.05,
            "depth_limit_offset": 0.05,
            "signal_levels": [0.60, 0.65, 0.70],
            "time_buckets_seconds": [60, 40, 30],
        },
        "runtime": {"universe_refresh_seconds": 60, "status_interval_seconds": 15},
        "market_filter": {
            "symbols": ["BTC"],
            "interval_minutes": [5],
            "fee_type": "crypto_fees",
            "min_days_to_expiry": 0,
            "max_days_to_expiry": 0.02,
            "min_volume": 0,
            "min_liquidity": 0,
        },
        "db": {"path": db_path},
        "polymarket": {"clob_base_url": "https://clob.polymarket.com"},
    }


def make_market(seconds_left: int = 35) -> OracleMarket:
    now = datetime.utcnow()
    return OracleMarket(
        venue="polymarket",
        market_id="m1",
        title="BTC Up or Down - Test",
        symbol="BTC",
        interval_minutes=5,
        expiry=now + timedelta(seconds=seconds_left),
        market_start=now - timedelta(minutes=4),
        volume=1000,
        yes_ask=0.0,
        no_ask=0.0,
        yes_token_id="yes-token",
        no_token_id="no-token",
        pm_event_slug="btc-updown-5m-test",
    )


def make_engine(tmp_path: Path, book: OrderBook | None = None, notifier=None):
    db = JumpPaperDB(str(tmp_path / "jump.db"))
    scanner = DummyScanner()
    engine = JumpPaperEngine(
        make_config(str(tmp_path / "jump.db")),
        db,
        notifier=notifier,
        scanner=scanner,
        clob=DummyClob(book),
    )
    return engine, db


def test_signal_requires_jump_vs_avg10s(tmp_path: Path) -> None:
    book = OrderBook(asks=[OrderLevel(price=0.66, size=100)], bids=[])
    engine, db = make_engine(tmp_path, book=book)
    market = make_market(seconds_left=35)
    now = datetime.utcnow()
    state = engine._states.setdefault((market.market_id, "yes"), SimpleNamespace(prices=None))
    from collections import deque
    from jump_paper_bot.models import PricePoint
    state.prices = deque([
        PricePoint(timestamp=now - timedelta(seconds=8), price=0.61),
        PricePoint(timestamp=now - timedelta(seconds=4), price=0.62),
    ])
    engine.on_pm_price(market, "yes", 0.65)
    assert db.stats()["total_count"] == 0

    state.prices = deque([
        PricePoint(timestamp=now - timedelta(seconds=8), price=0.59),
        PricePoint(timestamp=now - timedelta(seconds=4), price=0.60),
    ])
    engine.on_pm_price(market, "yes", 0.65)
    assert db.stats()["total_count"] == 2


def test_signal_window_and_bucket_dedup(tmp_path: Path) -> None:
    book = OrderBook(asks=[OrderLevel(price=0.66, size=100)], bids=[])
    engine, db = make_engine(tmp_path, book=book)
    market = make_market(seconds_left=35)
    now = datetime.utcnow()
    from collections import deque
    from jump_paper_bot.models import PricePoint

    engine._states[(market.market_id, "yes")] = SimpleNamespace(
        prices=deque([
            PricePoint(timestamp=now - timedelta(seconds=7), price=0.59),
            PricePoint(timestamp=now - timedelta(seconds=3), price=0.60),
        ])
    )
    engine.on_pm_price(market, "yes", 0.76)
    assert db.stats()["total_count"] == 0

    engine._states[(market.market_id, "yes")] = SimpleNamespace(
        prices=deque([
            PricePoint(timestamp=now - timedelta(seconds=7), price=0.59),
            PricePoint(timestamp=now - timedelta(seconds=3), price=0.60),
        ])
    )
    engine.on_pm_price(market, "yes", 0.65)
    assert db.stats()["total_count"] == 2
    assert db.get_signal("m1", "yes", 60) is not None
    assert db.get_signal("m1", "yes", 40) is not None
    assert db.get_signal("m1", "yes", 30) is None

    engine._states[(market.market_id, "yes")] = SimpleNamespace(
        prices=deque([
            PricePoint(timestamp=now - timedelta(seconds=7), price=0.58),
            PricePoint(timestamp=now - timedelta(seconds=3), price=0.59),
        ])
    )
    engine.on_pm_price(market, "yes", 0.65)
    assert db.stats()["total_count"] == 2


def test_orderbook_skip_and_avg_fill(tmp_path: Path) -> None:
    engine, _ = make_engine(tmp_path)
    status, reason, fill = engine._evaluate_fill(None, limit_price=0.70)
    assert status == "skip"
    assert reason == "orderbook_empty"
    assert fill is None

    shallow = OrderBook(asks=[OrderLevel(price=0.66, size=10)], bids=[])
    status, reason, fill = engine._evaluate_fill(shallow, limit_price=0.70)
    assert status == "skip"
    assert reason == "insufficient_depth"
    assert fill is None

    book = OrderBook(
        asks=[
            OrderLevel(price=0.66, size=5),
            OrderLevel(price=0.68, size=10),
            OrderLevel(price=0.72, size=50),
        ],
        bids=[],
    )
    status, reason, fill = engine._evaluate_fill(book, limit_price=0.70)
    assert status == "open"
    assert reason is None
    assert fill is not None
    assert round(fill["depth_usd"], 2) == 10.10
    assert round(fill["avg_fill"], 4) == 0.6667


def test_resolve_pnl_and_status_text(tmp_path: Path) -> None:
    notifier = DummyNotifier()
    book = OrderBook(asks=[OrderLevel(price=0.66, size=100)], bids=[])
    engine, db = make_engine(tmp_path, book=book, notifier=notifier)
    market = make_market(seconds_left=35)
    now = datetime.utcnow()
    from collections import deque
    from jump_paper_bot.models import PricePoint

    engine._states[(market.market_id, "yes")] = SimpleNamespace(
        prices=deque([
            PricePoint(timestamp=now - timedelta(seconds=7), price=0.59),
            PricePoint(timestamp=now - timedelta(seconds=3), price=0.60),
        ])
    )
    engine.on_pm_price(market, "yes", 0.65)
    position = db.get_open_positions()[0]
    db.resolve_position(position.id, "yes", position.filled_shares - position.total_cost)
    text = engine.get_status_text()
    assert "wr=100.0%" in text
    assert "По символам:" in text
    assert "По bucket:" in text


def test_telegram_message_formatting() -> None:
    notifier = JumpTelegramNotifier.__new__(JumpTelegramNotifier)
    captured = {}

    def fake_send(text: str, reply_to_message_id: int | None = None):
        captured["text"] = text
        captured["reply_to"] = reply_to_message_id
        return 123

    notifier.send = fake_send
    message_id = notifier.notify_open(
        symbol="BTC",
        interval_minutes=5,
        side="yes",
        signal_bucket_seconds=30,
        signal_level=0.65,
        signal_price=0.67,
        avg_prev_10s=0.61,
        fill_avg=0.68,
        stake_usd=5.0,
        shares=7.35,
        depth_usd=14.0,
        market_url="https://polymarket.com/event/x",
    )
    assert message_id == 123
    assert "OPEN BTC 5m YES" in captured["text"]
    assert "avg10s=0.610" in captured["text"]

    notifier.notify_resolve(
        symbol="BTC",
        interval_minutes=5,
        side="yes",
        winning_side="yes",
        pnl=2.25,
        reply_to_message_id=123,
    )
    assert captured["reply_to"] == 123
