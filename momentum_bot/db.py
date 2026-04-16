from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime
from typing import Optional

from momentum_bot.models import MomentumPosition, SpikeSignal


class MomentumDB:
    def __init__(self, path: str) -> None:
        self._path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id TEXT PRIMARY KEY,
                pair_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                title TEXT NOT NULL,
                expiry TEXT NOT NULL,
                side TEXT NOT NULL,
                bet_venue TEXT NOT NULL,
                leader_venue TEXT NOT NULL,
                entry_price REAL NOT NULL,
                leader_price_at_entry REAL NOT NULL,
                shares REAL NOT NULL,
                total_cost REAL NOT NULL,
                spike_magnitude REAL NOT NULL,
                opened_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                resolved_at TEXT,
                outcome TEXT,
                pnl REAL
            )
        """)
        self._conn.commit()

    def open_position(self, signal: SpikeSignal, shares: float, entry_price: float,
                      total_cost: float, title: str, expiry: datetime) -> MomentumPosition:
        pos_id = str(uuid.uuid4())
        now = datetime.utcnow()
        self._conn.execute(
            """
            INSERT INTO positions
              (id, pair_key, symbol, title, expiry, side, bet_venue, leader_venue,
               entry_price, leader_price_at_entry, shares, total_cost, spike_magnitude, opened_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')
            """,
            (
                pos_id,
                signal.pair_key,
                signal.symbol,
                title,
                expiry.isoformat(),
                signal.side,
                signal.follower_venue,
                signal.leader_venue,
                entry_price,
                signal.leader_price,
                shares,
                total_cost,
                signal.spike_magnitude,
                now.isoformat(),
            ),
        )
        self._conn.commit()
        return MomentumPosition(
            id=pos_id,
            pair_key=signal.pair_key,
            symbol=signal.symbol,
            title=title,
            expiry=expiry,
            side=signal.side,
            bet_venue=signal.follower_venue,
            leader_venue=signal.leader_venue,
            entry_price=entry_price,
            leader_price_at_entry=signal.leader_price,
            shares=shares,
            total_cost=total_cost,
            spike_magnitude=signal.spike_magnitude,
            opened_at=now,
        )

    def get_open_positions(self) -> list[MomentumPosition]:
        rows = self._conn.execute(
            "SELECT * FROM positions WHERE status = 'open' ORDER BY opened_at DESC"
        ).fetchall()
        return [self._row_to_position(r) for r in rows]

    def get_primary_open_positions(self) -> list[MomentumPosition]:
        rows = self._conn.execute(
            """
            WITH ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY pair_key
                        ORDER BY opened_at ASC, id ASC
                    ) AS rn
                FROM positions
            )
            SELECT *
            FROM ranked
            WHERE rn = 1 AND status = 'open'
            ORDER BY opened_at DESC
            """
        ).fetchall()
        return [self._row_to_position(r) for r in rows]

    def get_all_positions(self) -> list[MomentumPosition]:
        rows = self._conn.execute(
            "SELECT * FROM positions ORDER BY opened_at DESC"
        ).fetchall()
        return [self._row_to_position(r) for r in rows]

    def count_positions_for_pair(self, pair_key: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS c FROM positions WHERE pair_key=?",
            (pair_key,),
        ).fetchone()
        return int(row["c"]) if row is not None else 0

    def is_primary_position(self, position_id: str) -> bool:
        row = self._conn.execute(
            """
            WITH ranked AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY pair_key
                        ORDER BY opened_at ASC, id ASC
                    ) AS rn
                FROM positions
            )
            SELECT rn
            FROM ranked
            WHERE id=?
            """,
            (position_id,),
        ).fetchone()
        return row is not None and int(row["rn"]) == 1

    def has_open_position(self, pair_key: str, side: str, bet_venue: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM positions WHERE pair_key=? AND side=? AND bet_venue=? AND status='open'",
            (pair_key, side, bet_venue),
        ).fetchone()
        return row is not None

    def has_open_opposite_side(self, pair_key: str, side: str) -> bool:
        """True if we already have any open position on the opposite side of this pair (any venue)."""
        opposite = "no" if side == "yes" else "yes"
        row = self._conn.execute(
            "SELECT 1 FROM positions WHERE pair_key=? AND side=? AND status='open'",
            (pair_key, opposite),
        ).fetchone()
        return row is not None

    def last_trade_time(self, pair_key: str, side: str) -> Optional[float]:
        """Return epoch timestamp of last opened position for this pair+side, or None."""
        row = self._conn.execute(
            "SELECT opened_at FROM positions WHERE pair_key=? AND side=? ORDER BY opened_at DESC LIMIT 1",
            (pair_key, side),
        ).fetchone()
        if row is None:
            return None
        try:
            dt = datetime.fromisoformat(row["opened_at"])
            return dt.timestamp()
        except Exception:
            return None

    def resolve_position(self, position_id: str, outcome: str, pnl: float) -> None:
        now = datetime.utcnow().isoformat()
        self._conn.execute(
            "UPDATE positions SET status='resolved', resolved_at=?, outcome=?, pnl=? WHERE id=?",
            (now, outcome, pnl, position_id),
        )
        self._conn.commit()

    def stats(self) -> dict:
        open_positions = self.get_open_positions()
        total_count = self._conn.execute("SELECT COUNT(*) as c FROM positions").fetchone()["c"]

        row = self._conn.execute(
            """
            WITH ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY pair_key
                        ORDER BY opened_at ASC, id ASC
                    ) AS rn
                FROM positions
            )
            SELECT
                COALESCE(SUM(CASE WHEN status='resolved' THEN pnl ELSE 0 END), 0) AS realized_pnl,
                SUM(CASE WHEN status='open' THEN 1 ELSE 0 END) AS open_count,
                SUM(CASE WHEN status='resolved' THEN 1 ELSE 0 END) AS resolved_count,
                SUM(CASE WHEN status='resolved' AND pnl > 0 THEN 1 ELSE 0 END) AS won_count,
                SUM(CASE WHEN status='resolved' AND pnl <= 0 THEN 1 ELSE 0 END) AS lost_count,
                COUNT(*) AS primary_total_count
            FROM ranked
            WHERE rn = 1
            """
        ).fetchone()

        realized_pnl = float(row["realized_pnl"] or 0.0)
        open_count = int(row["open_count"] or 0)
        resolved_count = int(row["resolved_count"] or 0)
        won_count = int(row["won_count"] or 0)
        lost_count = int(row["lost_count"] or 0)
        primary_total_count = int(row["primary_total_count"] or 0)
        return {
            "realized_pnl": realized_pnl,
            "open_count": open_count,
            "total_count": total_count,
            "primary_total_count": primary_total_count,
            "resolved_count": resolved_count,
            "won_count": won_count,
            "lost_count": lost_count,
            "analytics_open_count": len(open_positions),
        }

    def _row_to_position(self, row: sqlite3.Row) -> MomentumPosition:
        def _dt(s: Optional[str]) -> Optional[datetime]:
            if s is None:
                return None
            try:
                return datetime.fromisoformat(s)
            except Exception:
                return None

        return MomentumPosition(
            id=row["id"],
            pair_key=row["pair_key"],
            symbol=row["symbol"],
            title=row["title"],
            expiry=_dt(row["expiry"]) or datetime.utcnow(),
            side=row["side"],
            bet_venue=row["bet_venue"],
            leader_venue=row["leader_venue"],
            entry_price=float(row["entry_price"]),
            leader_price_at_entry=float(row["leader_price_at_entry"]),
            shares=float(row["shares"]),
            total_cost=float(row["total_cost"]),
            spike_magnitude=float(row["spike_magnitude"]),
            opened_at=_dt(row["opened_at"]) or datetime.utcnow(),
            status=row["status"],
            resolved_at=_dt(row["resolved_at"]),
            outcome=row["outcome"],
            pnl=float(row["pnl"]) if row["pnl"] is not None else None,
        )
