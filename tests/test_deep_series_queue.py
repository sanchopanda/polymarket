"""
Тесты логики очереди глубоких серий:
- Только одна серия может быть depth >= 2
- При занятом глубоком слоте серия уходит в waiting
- При освобождении глубокого слота первая waiting-серия эскалирует
"""
from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.config import Config, MartingaleConfig, PaperTradingConfig, StrategyConfig
from src.db.models import BetSeries, SimulatedBet
from src.db.store import Store
from src.real.engine import RealTradingEngine


# ── Фикстуры ──────────────────────────────────────────────────────────────

@pytest.fixture
def store(tmp_path):
    return Store(str(tmp_path / "test.db"))


@pytest.fixture
def config():
    cfg = Config()
    cfg.real_martingale = MartingaleConfig(
        initial_bet_size=1.0,
        max_series_depth=6,
        max_active_series=5,
    )
    cfg.paper_trading = PaperTradingConfig(taker_fee=0.02)
    cfg.strategy = StrategyConfig()
    return cfg


@pytest.fixture
def engine(store, config):
    gamma = MagicMock()
    eng = RealTradingEngine(config, store, gamma)
    eng._clob = MagicMock()
    eng._clob_balance = MagicMock()
    eng._creds = MagicMock()
    return eng


def _make_series(store: Store, depth: int, status: str = "active") -> BetSeries:
    s = BetSeries(initial_bet_size=1.0, total_invested=1.0)
    store.create_series(s)
    if depth > 0 or status != "active":
        store.conn.execute(
            "UPDATE bet_series SET current_depth = ?, status = ? WHERE id = ?",
            (depth, status, s.id),
        )
        store.conn.commit()
    return store.get_series_by_id(s.id)


def _make_open_bet(store: Store, series_id: str, depth: int = 0) -> SimulatedBet:
    bet = SimulatedBet(
        market_id=f"market_{series_id[:8]}_{depth}",
        market_question="Test market",
        outcome="Yes",
        token_id="token_123",
        entry_price=0.5,
        amount_usd=1.0,
        fee_usd=0.02,
        shares=2.0,
        score=0.8,
        placed_at=datetime.utcnow(),
        market_end_date=datetime.utcnow() + timedelta(hours=1),
        series_id=series_id,
        series_depth=depth,
    )
    store.save_bet(bet)
    return bet


# ── Тесты Store ───────────────────────────────────────────────────────────

class TestStoreWaiting:
    def test_get_active_series_includes_waiting(self, store):
        """get_active_series должен возвращать и active, и waiting серии."""
        s_active = _make_series(store, depth=0, status="active")
        s_waiting = _make_series(store, depth=1, status="waiting")
        _make_series(store, depth=0, status="won")  # не должна попасть

        active = store.get_active_series()
        ids = {s.id for s in active}
        assert s_active.id in ids
        assert s_waiting.id in ids
        assert len(ids) == 2

    def test_set_series_waiting(self, store):
        s = _make_series(store, depth=1)
        assert s.status == "active"

        store.set_series_waiting(s.id)
        updated = store.get_series_by_id(s.id)
        assert updated.status == "waiting"

    def test_get_waiting_series(self, store):
        _make_series(store, depth=0, status="active")
        w1 = _make_series(store, depth=1, status="waiting")
        w2 = _make_series(store, depth=1, status="waiting")

        waiting = store.get_waiting_series()
        assert len(waiting) == 2
        assert {w.id for w in waiting} == {w1.id, w2.id}

    def test_get_waiting_series_empty(self, store):
        _make_series(store, depth=0, status="active")
        assert store.get_waiting_series() == []


# ── Тесты engine._escalate_series ────────────────────────────────────────

class TestEscalateSeriesQueue:
    def _make_candidate(self):
        from src.strategy.scorer import ScoredCandidate
        from src.strategy.scanner import Candidate
        from src.api.gamma import Market
        market = Market(
            id="market_new",
            question="New market?",
            outcomes=["Yes", "No"],
            outcome_prices=[0.5, 0.5],
            clob_token_ids=["tok1", "tok2"],
            volume_num=10000,
            liquidity_num=10000,
            end_date=datetime.utcnow() + timedelta(hours=2),
            active=True,
            closed=False,
            neg_risk=False,
            category="crypto",
            fee_type="crypto_fees",
        )
        candidate = Candidate(
            market=market,
            outcome="Yes",
            outcome_idx=0,
            price=0.5,
            token_id="tok1",
            days_to_expiry=0.08,
        )
        return ScoredCandidate(
            candidate=candidate,
            total_score=0.9,
            liquidity_score=0.5,
            time_score=0.9,
        )

    def test_escalate_to_depth2_sets_waiting_when_deep_exists(self, engine, store):
        """Если уже есть серия depth>=2, новая серия должна уйти в waiting."""
        # Создаём глубокую серию
        deep = _make_series(store, depth=2, status="active")
        _make_open_bet(store, deep.id, depth=2)

        # Серия которая хочет эскалировать на depth=2
        loser = _make_series(store, depth=1, status="active")
        _make_open_bet(store, loser.id, depth=1)

        candidates = [self._make_candidate()]
        engine._escalate_series(loser.id, current_depth=1, ranked=candidates)

        updated = store.get_series_by_id(loser.id)
        assert updated.status == "waiting"

    def test_escalate_to_depth2_proceeds_when_no_deep(self, engine, store):
        """Если нет серий depth>=2, эскалация должна пройти (или упасть на ордере)."""
        series = _make_series(store, depth=1, status="active")
        _make_open_bet(store, series.id, depth=1)

        candidates = [self._make_candidate()]

        # Мокаем CLOB — ордер возвращает MATCHED
        engine._clob.create_market_order.return_value = MagicMock()
        engine._clob.post_order.return_value = {"orderID": "0xabc", "status": "MATCHED"}

        engine._escalate_series(series.id, current_depth=1, ranked=candidates)

        updated = store.get_series_by_id(series.id)
        # Серия должна быть active (не waiting), глубина стала 2
        assert updated.status == "active"
        assert updated.current_depth == 2

    def test_escalate_depth1_not_blocked(self, engine, store):
        """Эскалация на depth=1 не должна блокироваться даже если есть глубокая серия."""
        deep = _make_series(store, depth=2, status="active")
        _make_open_bet(store, deep.id, depth=2)

        series = _make_series(store, depth=0, status="active")
        _make_open_bet(store, series.id, depth=0)

        candidates = [self._make_candidate()]
        engine._clob.create_market_order.return_value = MagicMock()
        engine._clob.post_order.return_value = {"orderID": "0xabc", "status": "MATCHED"}

        engine._escalate_series(series.id, current_depth=0, ranked=candidates)

        updated = store.get_series_by_id(series.id)
        assert updated.status == "active"
        assert updated.current_depth == 1


# ── Тесты check_resolutions: активация из очереди ────────────────────────

class TestWaitingActivation:
    def test_waiting_series_activated_when_deep_closes(self, engine, store):
        """После закрытия глубокой серии первая waiting должна активироваться."""
        # Создаём глубокую серию на max_depth-1, чтобы при проигрыше она стала abandoned
        deep = _make_series(store, depth=5, status="active")
        deep_bet = _make_open_bet(store, deep.id, depth=5)
        # Проигрываем ставку
        store.conn.execute(
            "UPDATE simulated_bets SET market_end_date = ? WHERE id = ?",
            ((datetime.utcnow() - timedelta(hours=1)).isoformat(), deep_bet.id),
        )
        store.conn.commit()

        # Waiting серия
        waiting = _make_series(store, depth=1, status="waiting")

        # Мокаем Gamma API — рынок закрыт, цена проигрыша
        mock_market = MagicMock()
        mock_market.closed = True
        mock_market.outcomes = ["Yes", "No"]
        mock_market.outcome_prices = [0.02, 0.98]
        engine.gamma.fetch_market.return_value = mock_market

        # Мокаем scanner для эскалации waiting серии
        engine.scanner.scan = MagicMock(return_value=[])
        engine.scorer.rank = MagicMock(return_value=[])

        engine.check_resolutions()

        # Waiting серия должна стать active (или уйти в эскалацию)
        updated = store.get_series_by_id(waiting.id)
        assert updated.status in ("active", "abandoned")  # активирована и попытка эскалации

    def test_waiting_series_stays_if_deep_still_active(self, engine, store):
        """Waiting серия не должна активироваться пока есть активная глубокая."""
        deep = _make_series(store, depth=2, status="active")
        _make_open_bet(store, deep.id, depth=2)  # ставка ещё активна (не истекла)

        waiting = _make_series(store, depth=1, status="waiting")

        engine.gamma.fetch_market.return_value = None  # рынки не резолвятся

        engine.check_resolutions()

        updated = store.get_series_by_id(waiting.id)
        assert updated.status == "waiting"
