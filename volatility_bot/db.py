from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from volatility_bot.models import Bet


_ISO = "%Y-%m-%dT%H:%M:%S.%f"


def _dt(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    for fmt in (_ISO, "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _iso(dt: datetime | None) -> Optional[str]:
    return dt.isoformat() if dt else None


class VolatilityDB:
    def __init__(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS bets (
                id                TEXT    PRIMARY KEY,
                venue             TEXT    NOT NULL,
                market_id         TEXT    NOT NULL,
                symbol            TEXT    NOT NULL,
                interval_minutes  INTEGER NOT NULL,
                market_start      TEXT    NOT NULL,
                market_end        TEXT    NOT NULL,
                opened_at         TEXT    NOT NULL,
                market_minute     INTEGER NOT NULL,
                market_quarter    INTEGER NOT NULL,
                position_pct      REAL    NOT NULL,
                side              TEXT    NOT NULL,
                entry_price       REAL    NOT NULL,
                trigger_bucket    TEXT    NOT NULL,
                shares            REAL    NOT NULL,
                total_cost        REAL    NOT NULL,
                order_id          TEXT    NOT NULL DEFAULT '',
                order_status      TEXT    NOT NULL DEFAULT '',
                order_fill_price  REAL    NOT NULL DEFAULT 0.0,
                order_fee         REAL    NOT NULL DEFAULT 0.0,
                order_latency_ms  REAL    NOT NULL DEFAULT 0.0,
                status            TEXT    NOT NULL DEFAULT 'open',
                resolved_at       TEXT,
                winning_side      TEXT,
                pnl               REAL,
                is_paper          INTEGER NOT NULL DEFAULT 0,
                is_legacy         INTEGER NOT NULL DEFAULT 0,
                legacy_source     TEXT,
                legacy_pair_key   TEXT,
                UNIQUE (venue, market_id, side, trigger_bucket)
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp  TEXT NOT NULL,
                event_type TEXT NOT NULL,
                bet_id     TEXT,
                details    TEXT
            );
        """)
        self.conn.commit()

    # ── Write ────────────────────────────────────────────────────────────

    def record_bet(self, bet: Bet) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO bets (
                id, venue, market_id, symbol, interval_minutes,
                market_start, market_end, opened_at,
                market_minute, market_quarter, position_pct,
                side, entry_price, trigger_bucket,
                shares, total_cost,
                order_id, order_status, order_fill_price, order_fee, order_latency_ms,
                status, resolved_at, winning_side, pnl,
                is_paper, is_legacy, legacy_source, legacy_pair_key
            ) VALUES (
                ?,?,?,?,?,  ?,?,?,  ?,?,?,  ?,?,?,  ?,?,  ?,?,?,?,?,  ?,?,?,?,  ?,?,?,?
            )
            """,
            (
                bet.id, bet.venue, bet.market_id, bet.symbol, bet.interval_minutes,
                _iso(bet.market_start), _iso(bet.market_end), _iso(bet.opened_at),
                bet.market_minute, bet.market_quarter, bet.position_pct,
                bet.side, bet.entry_price, bet.trigger_bucket,
                bet.shares, bet.total_cost,
                bet.order_id, bet.order_status, bet.order_fill_price,
                bet.order_fee, bet.order_latency_ms,
                bet.status, _iso(bet.resolved_at), bet.winning_side, bet.pnl,
                bet.is_paper, bet.is_legacy,
                bet.legacy_source, bet.legacy_pair_key,
            ),
        )
        self.conn.commit()

    def resolve_bet(self, bet_id: str, winning_side: str, pnl: float) -> None:
        self.conn.execute(
            """
            UPDATE bets
            SET status='resolved', resolved_at=?, winning_side=?, pnl=?
            WHERE id=?
            """,
            (_iso(datetime.utcnow()), winning_side, round(pnl, 6), bet_id),
        )
        self.conn.commit()

    def audit(self, event_type: str, bet_id: Optional[str], details: dict) -> None:
        self.conn.execute(
            "INSERT INTO audit_log (timestamp, event_type, bet_id, details) VALUES (?,?,?,?)",
            (_iso(datetime.utcnow()), event_type, bet_id, json.dumps(details)),
        )
        self.conn.commit()

    # ── Read ─────────────────────────────────────────────────────────────

    def has_bet(self, venue: str, market_id: str, side: str, trigger_bucket: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM bets WHERE venue=? AND market_id=? AND side=? AND trigger_bucket=?",
            (venue, market_id, side, trigger_bucket),
        ).fetchone()
        return row is not None

    def get_open_bets(self) -> list[Bet]:
        rows = self.conn.execute(
            "SELECT * FROM bets WHERE status='open' OR status='paper'"
        ).fetchall()
        return [self._row_to_bet(r) for r in rows]

    def get_bet(self, bet_id: str) -> Optional[Bet]:
        row = self.conn.execute("SELECT * FROM bets WHERE id=?", (bet_id,)).fetchone()
        return self._row_to_bet(row) if row else None

    def get_recent_bets(self, limit: int = 50) -> list[Bet]:
        rows = self.conn.execute(
            "SELECT * FROM bets ORDER BY opened_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [self._row_to_bet(r) for r in rows]

    def stats(self) -> dict:
        total = self.conn.execute("SELECT COUNT(*) FROM bets").fetchone()[0]
        resolved = self.conn.execute(
            "SELECT COUNT(*), SUM(pnl) FROM bets WHERE status='resolved'"
        ).fetchone()
        open_cnt = self.conn.execute(
            "SELECT COUNT(*) FROM bets WHERE status='open'"
        ).fetchone()[0]
        legacy = self.conn.execute(
            "SELECT COUNT(*) FROM bets WHERE is_legacy=1"
        ).fetchone()[0]
        return {
            "total": total,
            "resolved": resolved[0],
            "realized_pnl": round(resolved[1] or 0.0, 4),
            "open": open_cnt,
            "legacy": legacy,
        }

    def bucket_stats(self) -> list[dict]:
        """Per-bucket stats for non-legacy bets only."""
        rows = self.conn.execute("""
            SELECT
                trigger_bucket,
                COUNT(*) AS total,
                SUM(CASE WHEN status='resolved' THEN 1 ELSE 0 END) AS resolved,
                SUM(CASE WHEN status='resolved' AND pnl > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN status='resolved' THEN pnl ELSE 0 END) AS pnl
            FROM bets
            WHERE is_legacy = 0
            GROUP BY trigger_bucket
            ORDER BY trigger_bucket
        """).fetchall()
        return [dict(r) for r in rows]

    def _row_to_bet(self, row: sqlite3.Row) -> Bet:
        return Bet(
            id=row["id"],
            venue=row["venue"],
            market_id=row["market_id"],
            symbol=row["symbol"],
            interval_minutes=row["interval_minutes"],
            market_start=_dt(row["market_start"]),
            market_end=_dt(row["market_end"]),
            opened_at=_dt(row["opened_at"]),
            market_minute=row["market_minute"],
            market_quarter=row["market_quarter"],
            position_pct=row["position_pct"],
            side=row["side"],
            entry_price=row["entry_price"],
            trigger_bucket=row["trigger_bucket"],
            shares=row["shares"],
            total_cost=row["total_cost"],
            order_id=row["order_id"] or "",
            order_status=row["order_status"] or "",
            order_fill_price=row["order_fill_price"] or 0.0,
            order_fee=row["order_fee"] or 0.0,
            order_latency_ms=row["order_latency_ms"] or 0.0,
            status=row["status"],
            resolved_at=_dt(row["resolved_at"]),
            winning_side=row["winning_side"],
            pnl=row["pnl"],
            is_paper=row["is_paper"] or 0,
            is_legacy=row["is_legacy"] or 0,
            legacy_source=row["legacy_source"],
            legacy_pair_key=row["legacy_pair_key"],
        )
