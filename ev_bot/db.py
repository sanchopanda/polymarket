"""Изолированная БД для EV-бота. data/ev_bot.db, отдельные таблицы."""
from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional


@dataclass
class EVSeries:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: str = "active"  # active | won | abandoned
    current_depth: int = 0
    initial_bet: float = 0.0
    total_invested: float = 0.0
    total_pnl: float = 0.0
    started_at: datetime = field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None


@dataclass
class EVBet:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    series_id: str = ""
    series_depth: int = 0
    market_id: str = ""
    market_question: str = ""
    outcome: str = ""
    token_id: str = ""
    entry_price: float = 0.0
    amount_usd: float = 0.0
    fee_usd: float = 0.0
    shares: float = 0.0
    placed_at: datetime = field(default_factory=datetime.utcnow)
    market_end_date: Optional[datetime] = None
    status: str = "open"  # open | won | lost
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    resolved_at: Optional[datetime] = None


class EVStore:
    def __init__(self, path: str = "data/ev_bot.db") -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS ev_series (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'active',
                current_depth INTEGER NOT NULL DEFAULT 0,
                initial_bet REAL NOT NULL DEFAULT 0,
                total_invested REAL NOT NULL DEFAULT 0,
                total_pnl REAL NOT NULL DEFAULT 0,
                started_at TEXT NOT NULL,
                finished_at TEXT
            );

            CREATE TABLE IF NOT EXISTS ev_bets (
                id TEXT PRIMARY KEY,
                series_id TEXT NOT NULL,
                series_depth INTEGER NOT NULL DEFAULT 0,
                market_id TEXT NOT NULL,
                market_question TEXT NOT NULL,
                outcome TEXT NOT NULL,
                token_id TEXT NOT NULL,
                entry_price REAL NOT NULL,
                amount_usd REAL NOT NULL,
                fee_usd REAL NOT NULL DEFAULT 0,
                shares REAL NOT NULL,
                placed_at TEXT NOT NULL,
                market_end_date TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                exit_price REAL,
                pnl REAL,
                resolved_at TEXT,
                FOREIGN KEY (series_id) REFERENCES ev_series(id)
            );
        """)
        self.conn.commit()

    # --- Series ---

    def create_series(self, s: EVSeries) -> None:
        self.conn.execute(
            "INSERT INTO ev_series VALUES (?,?,?,?,?,?,?,?)",
            (s.id, s.status, s.current_depth, s.initial_bet,
             s.total_invested, s.total_pnl,
             s.started_at.isoformat(), None),
        )
        self.conn.commit()

    def get_active_series(self) -> List[EVSeries]:
        rows = self.conn.execute(
            "SELECT * FROM ev_series WHERE status='active'"
        ).fetchall()
        return [self._row_to_series(r) for r in rows]

    def get_all_series(self) -> List[EVSeries]:
        rows = self.conn.execute(
            "SELECT * FROM ev_series ORDER BY started_at DESC"
        ).fetchall()
        return [self._row_to_series(r) for r in rows]

    def get_series_by_id(self, series_id: str) -> Optional[EVSeries]:
        row = self.conn.execute(
            "SELECT * FROM ev_series WHERE id=?", (series_id,)
        ).fetchone()
        return self._row_to_series(row) if row else None

    def finish_series(self, series_id: str, status: str, total_pnl: float) -> None:
        self.conn.execute(
            "UPDATE ev_series SET status=?, total_pnl=?, finished_at=? WHERE id=?",
            (status, total_pnl, datetime.utcnow().isoformat(), series_id),
        )
        self.conn.commit()

    def update_series_depth(self, series_id: str, depth: int, added_cost: float) -> None:
        self.conn.execute(
            "UPDATE ev_series SET current_depth=?, total_invested=total_invested+? WHERE id=?",
            (depth, added_cost, series_id),
        )
        self.conn.commit()

    def get_series_bets(self, series_id: str) -> List[EVBet]:
        rows = self.conn.execute(
            "SELECT * FROM ev_bets WHERE series_id=? ORDER BY series_depth",
            (series_id,),
        ).fetchall()
        return [self._row_to_bet(r) for r in rows]

    def _row_to_series(self, r) -> EVSeries:
        return EVSeries(
            id=r["id"], status=r["status"], current_depth=r["current_depth"],
            initial_bet=r["initial_bet"], total_invested=r["total_invested"],
            total_pnl=r["total_pnl"],
            started_at=datetime.fromisoformat(r["started_at"]),
            finished_at=datetime.fromisoformat(r["finished_at"]) if r["finished_at"] else None,
        )

    # --- Bets ---

    def save_bet(self, b: EVBet) -> None:
        self.conn.execute(
            "INSERT INTO ev_bets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (b.id, b.series_id, b.series_depth, b.market_id, b.market_question,
             b.outcome, b.token_id, b.entry_price, b.amount_usd, b.fee_usd,
             b.shares, b.placed_at.isoformat(),
             b.market_end_date.isoformat() if b.market_end_date else None,
             b.status, b.exit_price, b.pnl,
             b.resolved_at.isoformat() if b.resolved_at else None),
        )
        self.conn.commit()

    def get_open_bets(self) -> List[EVBet]:
        rows = self.conn.execute(
            "SELECT * FROM ev_bets WHERE status='open' ORDER BY placed_at"
        ).fetchall()
        return [self._row_to_bet(r) for r in rows]

    def get_all_bets(self) -> List[EVBet]:
        rows = self.conn.execute(
            "SELECT * FROM ev_bets ORDER BY placed_at DESC"
        ).fetchall()
        return [self._row_to_bet(r) for r in rows]

    def resolve_bet(self, bet_id: str, exit_price: float) -> None:
        status = "won" if exit_price >= 0.9 else "lost"
        row = self.conn.execute(
            "SELECT shares, amount_usd, fee_usd FROM ev_bets WHERE id=?", (bet_id,)
        ).fetchone()
        pnl = (exit_price * row["shares"]) - row["amount_usd"] - row["fee_usd"]
        self.conn.execute(
            "UPDATE ev_bets SET status=?, exit_price=?, pnl=?, resolved_at=? WHERE id=?",
            (status, exit_price, pnl, datetime.utcnow().isoformat(), bet_id),
        )
        self.conn.commit()

    def already_bet(self, market_id: str, outcome: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM ev_bets WHERE market_id=? AND outcome=? AND status='open'",
            (market_id, outcome),
        ).fetchone()
        return row is not None

    def get_total_invested_active(self) -> float:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(total_invested),0) FROM ev_series WHERE status='active'"
        ).fetchone()
        return row[0]

    def _row_to_bet(self, r) -> EVBet:
        return EVBet(
            id=r["id"], series_id=r["series_id"], series_depth=r["series_depth"],
            market_id=r["market_id"], market_question=r["market_question"],
            outcome=r["outcome"], token_id=r["token_id"],
            entry_price=r["entry_price"], amount_usd=r["amount_usd"],
            fee_usd=r["fee_usd"], shares=r["shares"],
            placed_at=datetime.fromisoformat(r["placed_at"]),
            market_end_date=datetime.fromisoformat(r["market_end_date"]) if r["market_end_date"] else None,
            status=r["status"], exit_price=r["exit_price"], pnl=r["pnl"],
            resolved_at=datetime.fromisoformat(r["resolved_at"]) if r["resolved_at"] else None,
        )

    # --- Stats ---

    def get_stats(self) -> dict:
        row = self.conn.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status='active') as active_series,
                COUNT(*) FILTER (WHERE status='won') as won_series,
                COUNT(*) FILTER (WHERE status='abandoned') as abandoned_series,
                COALESCE(SUM(total_pnl) FILTER (WHERE status IN ('won','abandoned')), 0) as realized_pnl
            FROM ev_series
        """).fetchone()
        bets = self.conn.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status='open') as open_bets,
                COUNT(*) FILTER (WHERE status='won') as won_bets,
                COUNT(*) FILTER (WHERE status='lost') as lost_bets
            FROM ev_bets
        """).fetchone()
        return {
            "active_series": row["active_series"],
            "won_series": row["won_series"],
            "abandoned_series": row["abandoned_series"],
            "realized_pnl": row["realized_pnl"],
            "open_bets": bets["open_bets"],
            "won_bets": bets["won_bets"],
            "lost_bets": bets["lost_bets"],
        }
