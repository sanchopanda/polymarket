from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class PairPosition:
    id: str
    market_id: str
    question: str
    outcome_a: str
    outcome_b: str
    token_a: str
    token_b: str
    shares: float
    avg_price_a: float
    avg_price_b: float
    gross_cost: float
    fee_cost: float
    total_cost: float
    expected_payout: float
    expected_edge: float
    placed_at: datetime
    end_date: datetime
    status: str = "open"
    winning_outcome: Optional[str] = None
    pnl: Optional[float] = None
    resolved_at: Optional[datetime] = None


class ArbBotDB:
    def __init__(self, path: str) -> None:
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pair_positions (
                id TEXT PRIMARY KEY,
                market_id TEXT NOT NULL,
                question TEXT NOT NULL,
                outcome_a TEXT NOT NULL,
                outcome_b TEXT NOT NULL,
                token_a TEXT NOT NULL,
                token_b TEXT NOT NULL,
                shares REAL NOT NULL,
                avg_price_a REAL NOT NULL,
                avg_price_b REAL NOT NULL,
                gross_cost REAL NOT NULL,
                fee_cost REAL NOT NULL,
                total_cost REAL NOT NULL,
                expected_payout REAL NOT NULL,
                expected_edge REAL NOT NULL,
                placed_at TEXT NOT NULL,
                end_date TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                winning_outcome TEXT,
                pnl REAL,
                resolved_at TEXT
            )
            """
        )
        self._conn.commit()

    def has_open_position(self, market_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM pair_positions WHERE market_id=? AND status='open'",
            (market_id,),
        ).fetchone()
        return row is not None

    def save_position(self, position: PairPosition) -> None:
        self._conn.execute(
            """
            INSERT INTO pair_positions (
                id, market_id, question, outcome_a, outcome_b, token_a, token_b,
                shares, avg_price_a, avg_price_b, gross_cost, fee_cost, total_cost,
                expected_payout, expected_edge, placed_at, end_date, status
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                position.id,
                position.market_id,
                position.question,
                position.outcome_a,
                position.outcome_b,
                position.token_a,
                position.token_b,
                position.shares,
                position.avg_price_a,
                position.avg_price_b,
                position.gross_cost,
                position.fee_cost,
                position.total_cost,
                position.expected_payout,
                position.expected_edge,
                position.placed_at.isoformat(),
                position.end_date.isoformat(),
                position.status,
            ),
        )
        self._conn.commit()

    def create_position(self, **kwargs) -> PairPosition:
        return PairPosition(id=str(uuid.uuid4()), **kwargs)

    def get_open_positions(self) -> list[PairPosition]:
        rows = self._conn.execute(
            "SELECT * FROM pair_positions WHERE status='open' ORDER BY end_date ASC"
        ).fetchall()
        return [self._row_to_position(row) for row in rows]

    def get_all_positions(self) -> list[PairPosition]:
        rows = self._conn.execute(
            "SELECT * FROM pair_positions ORDER BY placed_at DESC"
        ).fetchall()
        return [self._row_to_position(row) for row in rows]

    def resolve_position(self, position_id: str, winning_outcome: str) -> None:
        position = self.get_position(position_id)
        if position is None:
            return

        payout = position.shares
        pnl = payout - position.total_cost
        self._conn.execute(
            """
            UPDATE pair_positions
            SET status='resolved', winning_outcome=?, pnl=?, resolved_at=?
            WHERE id=?
            """,
            (winning_outcome, pnl, datetime.utcnow().isoformat(), position_id),
        )
        self._conn.commit()

    def get_position(self, position_id: str) -> Optional[PairPosition]:
        row = self._conn.execute(
            "SELECT * FROM pair_positions WHERE id=?",
            (position_id,),
        ).fetchone()
        return self._row_to_position(row) if row else None

    def stats(self) -> dict:
        positions = self.get_all_positions()
        open_positions = [p for p in positions if p.status == "open"]
        resolved = [p for p in positions if p.status == "resolved"]
        open_cost = sum(p.total_cost for p in open_positions)
        realized_pnl = sum(p.pnl or 0.0 for p in resolved)
        return {
            "total": len(positions),
            "open": len(open_positions),
            "resolved": len(resolved),
            "open_cost": open_cost,
            "realized_pnl": realized_pnl,
        }

    def _row_to_position(self, row) -> PairPosition:
        return PairPosition(
            id=row["id"],
            market_id=row["market_id"],
            question=row["question"],
            outcome_a=row["outcome_a"],
            outcome_b=row["outcome_b"],
            token_a=row["token_a"],
            token_b=row["token_b"],
            shares=row["shares"],
            avg_price_a=row["avg_price_a"],
            avg_price_b=row["avg_price_b"],
            gross_cost=row["gross_cost"],
            fee_cost=row["fee_cost"],
            total_cost=row["total_cost"],
            expected_payout=row["expected_payout"],
            expected_edge=row["expected_edge"],
            placed_at=datetime.fromisoformat(row["placed_at"]),
            end_date=datetime.fromisoformat(row["end_date"]),
            status=row["status"],
            winning_outcome=row["winning_outcome"],
            pnl=row["pnl"],
            resolved_at=datetime.fromisoformat(row["resolved_at"]) if row["resolved_at"] else None,
        )
