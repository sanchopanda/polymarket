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

    def get_all_positions(self) -> list[MomentumPosition]:
        rows = self._conn.execute(
            "SELECT * FROM positions ORDER BY opened_at DESC"
        ).fetchall()
        return [self._row_to_position(r) for r in rows]

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
        locked = sum(p.total_cost for p in open_positions)
        row = self._conn.execute(
            "SELECT COALESCE(SUM(pnl), 0) as realized_pnl FROM positions WHERE status='resolved'"
        ).fetchone()
        realized_pnl = float(row["realized_pnl"])
        total_count = self._conn.execute("SELECT COUNT(*) as c FROM positions").fetchone()["c"]
        resolved_count = self._conn.execute(
            "SELECT COUNT(*) as c FROM positions WHERE status='resolved'"
        ).fetchone()["c"]
        won_count = self._conn.execute(
            "SELECT COUNT(*) as c FROM positions WHERE status='resolved' AND pnl > 0"
        ).fetchone()["c"]
        lost_count = self._conn.execute(
            "SELECT COUNT(*) as c FROM positions WHERE status='resolved' AND pnl <= 0"
        ).fetchone()["c"]
        return {
            "realized_pnl": realized_pnl,
            "locked": locked,
            "open_count": len(open_positions),
            "total_count": total_count,
            "resolved_count": resolved_count,
            "won_count": won_count,
            "lost_count": lost_count,
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
