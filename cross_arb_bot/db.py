from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime

from cross_arb_bot.models import CrossPosition, CrossVenueOpportunity


class CrossArbDB:
    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                id TEXT PRIMARY KEY,
                pair_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                title TEXT NOT NULL,
                expiry TEXT NOT NULL,
                venue_yes TEXT NOT NULL,
                market_yes TEXT NOT NULL,
                venue_no TEXT NOT NULL,
                market_no TEXT NOT NULL,
                polymarket_title TEXT,
                kalshi_title TEXT,
                match_score REAL,
                expiry_delta_seconds REAL,
                polymarket_reference_price REAL,
                kalshi_reference_price REAL,
                polymarket_rules TEXT,
                kalshi_rules TEXT,
                polymarket_snapshot_open TEXT,
                kalshi_snapshot_open TEXT,
                polymarket_snapshot_resolved TEXT,
                kalshi_snapshot_resolved TEXT,
                shares REAL NOT NULL,
                yes_ask REAL NOT NULL,
                no_ask REAL NOT NULL,
                yes_requested_shares REAL,
                yes_filled_shares REAL,
                yes_available_shares REAL,
                yes_avg_price REAL,
                yes_best_ask REAL,
                yes_remaining_shares_after_fill REAL,
                no_requested_shares REAL,
                no_filled_shares REAL,
                no_available_shares REAL,
                no_avg_price REAL,
                no_best_ask REAL,
                no_remaining_shares_after_fill REAL,
                ask_sum REAL NOT NULL,
                total_cost REAL NOT NULL,
                expected_profit REAL NOT NULL,
                opened_at TEXT NOT NULL,
                status TEXT NOT NULL,
                resolved_at TEXT,
                winning_side TEXT,
                pnl REAL,
                polymarket_result TEXT,
                kalshi_result TEXT,
                lock_valid INTEGER
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transfers (
                id TEXT PRIMARY KEY,
                from_venue TEXT NOT NULL,
                to_venue TEXT NOT NULL,
                amount REAL NOT NULL,
                note TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        self._ensure_column("positions", "polymarket_result", "TEXT")
        self._ensure_column("positions", "kalshi_result", "TEXT")
        self._ensure_column("positions", "lock_valid", "INTEGER")
        self._ensure_column("positions", "polymarket_title", "TEXT")
        self._ensure_column("positions", "kalshi_title", "TEXT")
        self._ensure_column("positions", "match_score", "REAL")
        self._ensure_column("positions", "expiry_delta_seconds", "REAL")
        self._ensure_column("positions", "polymarket_reference_price", "REAL")
        self._ensure_column("positions", "kalshi_reference_price", "REAL")
        self._ensure_column("positions", "polymarket_rules", "TEXT")
        self._ensure_column("positions", "kalshi_rules", "TEXT")
        self._ensure_column("positions", "polymarket_snapshot_open", "TEXT")
        self._ensure_column("positions", "kalshi_snapshot_open", "TEXT")
        self._ensure_column("positions", "polymarket_snapshot_resolved", "TEXT")
        self._ensure_column("positions", "kalshi_snapshot_resolved", "TEXT")
        self._ensure_column("positions", "yes_requested_shares", "REAL")
        self._ensure_column("positions", "yes_filled_shares", "REAL")
        self._ensure_column("positions", "yes_available_shares", "REAL")
        self._ensure_column("positions", "yes_avg_price", "REAL")
        self._ensure_column("positions", "yes_best_ask", "REAL")
        self._ensure_column("positions", "yes_remaining_shares_after_fill", "REAL")
        self._ensure_column("positions", "no_requested_shares", "REAL")
        self._ensure_column("positions", "no_filled_shares", "REAL")
        self._ensure_column("positions", "no_available_shares", "REAL")
        self._ensure_column("positions", "no_avg_price", "REAL")
        self._ensure_column("positions", "no_best_ask", "REAL")
        self._ensure_column("positions", "no_remaining_shares_after_fill", "REAL")
        self.conn.commit()

    def _ensure_column(self, table: str, column: str, column_type: str) -> None:
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        existing = {row["name"] for row in rows}
        if column in existing:
            return
        self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")

    def has_open_position(self, pair_key: str, yes_venue: str, no_venue: str) -> bool:
        row = self.conn.execute(
            """
            SELECT 1 FROM positions
            WHERE pair_key=? AND venue_yes=? AND venue_no=? AND status='open'
            """,
            (pair_key, yes_venue, no_venue),
        ).fetchone()
        return row is not None

    def count_positions_for_pair(self, pair_key: str, yes_venue: str, no_venue: str) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS cnt FROM positions
            WHERE pair_key=? AND venue_yes=? AND venue_no=?
            """,
            (pair_key, yes_venue, no_venue),
        ).fetchone()
        return int(row["cnt"] or 0)

    def open_position(
        self,
        opportunity: CrossVenueOpportunity,
        polymarket_snapshot_open: str | None = None,
        kalshi_snapshot_open: str | None = None,
        yes_leg=None,
        no_leg=None,
    ) -> CrossPosition:
        position = CrossPosition(
            id=str(uuid.uuid4()),
            pair_key=opportunity.pair_key,
            symbol=opportunity.symbol,
            title=opportunity.title,
            expiry=opportunity.expiry,
            venue_yes=opportunity.buy_yes_venue,
            market_yes=(opportunity.polymarket_market_id if opportunity.buy_yes_venue == "polymarket" else opportunity.kalshi_market_id),
            venue_no=opportunity.buy_no_venue,
            market_no=(opportunity.polymarket_market_id if opportunity.buy_no_venue == "polymarket" else opportunity.kalshi_market_id),
            shares=opportunity.shares,
            yes_ask=opportunity.yes_ask,
            no_ask=opportunity.no_ask,
            ask_sum=opportunity.ask_sum,
            total_cost=opportunity.total_cost,
            expected_profit=opportunity.expected_profit,
            opened_at=datetime.utcnow(),
            yes_requested_shares=getattr(yes_leg, "requested_shares", None),
            yes_filled_shares=getattr(yes_leg, "filled_shares", None),
            yes_available_shares=getattr(yes_leg, "available_shares", None),
            yes_avg_price=getattr(yes_leg, "avg_price", None),
            yes_best_ask=getattr(yes_leg, "best_ask", None),
            yes_remaining_shares_after_fill=getattr(yes_leg, "remaining_shares_after_fill", None),
            no_requested_shares=getattr(no_leg, "requested_shares", None),
            no_filled_shares=getattr(no_leg, "filled_shares", None),
            no_available_shares=getattr(no_leg, "available_shares", None),
            no_avg_price=getattr(no_leg, "avg_price", None),
            no_best_ask=getattr(no_leg, "best_ask", None),
            no_remaining_shares_after_fill=getattr(no_leg, "remaining_shares_after_fill", None),
            polymarket_snapshot_open=polymarket_snapshot_open,
            kalshi_snapshot_open=kalshi_snapshot_open,
        )
        self.conn.execute(
            """
            INSERT INTO positions (
                id, pair_key, symbol, title, expiry, venue_yes, market_yes, venue_no, market_no,
                polymarket_title, kalshi_title, match_score, expiry_delta_seconds,
                polymarket_reference_price, kalshi_reference_price, polymarket_rules, kalshi_rules,
                polymarket_snapshot_open, kalshi_snapshot_open,
                shares, yes_ask, no_ask,
                yes_requested_shares, yes_filled_shares, yes_available_shares, yes_avg_price, yes_best_ask, yes_remaining_shares_after_fill,
                no_requested_shares, no_filled_shares, no_available_shares, no_avg_price, no_best_ask, no_remaining_shares_after_fill,
                ask_sum, total_cost, expected_profit, opened_at, status
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                position.id,
                position.pair_key,
                position.symbol,
                position.title,
                position.expiry.isoformat(),
                position.venue_yes,
                position.market_yes,
                position.venue_no,
                position.market_no,
                opportunity.polymarket_title,
                opportunity.kalshi_title,
                opportunity.match_score,
                opportunity.expiry_delta_seconds,
                opportunity.polymarket_reference_price,
                opportunity.kalshi_reference_price,
                opportunity.polymarket_rules,
                opportunity.kalshi_rules,
                position.polymarket_snapshot_open,
                position.kalshi_snapshot_open,
                position.shares,
                position.yes_ask,
                position.no_ask,
                position.yes_requested_shares,
                position.yes_filled_shares,
                position.yes_available_shares,
                position.yes_avg_price,
                position.yes_best_ask,
                position.yes_remaining_shares_after_fill,
                position.no_requested_shares,
                position.no_filled_shares,
                position.no_available_shares,
                position.no_avg_price,
                position.no_best_ask,
                position.no_remaining_shares_after_fill,
                position.ask_sum,
                position.total_cost,
                position.expected_profit,
                position.opened_at.isoformat(),
                position.status,
            ),
        )
        self.conn.commit()
        return position

    def get_open_positions(self) -> list[CrossPosition]:
        rows = self.conn.execute("SELECT * FROM positions WHERE status='open' ORDER BY expiry ASC").fetchall()
        return [self._row_to_position(row) for row in rows]

    def resolve_position(
        self,
        position_id: str,
        winning_side: str,
        pnl: float,
        polymarket_result: str | None,
        kalshi_result: str | None,
        lock_valid: bool,
        polymarket_snapshot_resolved: str | None = None,
        kalshi_snapshot_resolved: str | None = None,
    ) -> None:
        position = self.get_position(position_id)
        if position is None:
            return
        self.conn.execute(
            """
            UPDATE positions
            SET status='resolved', resolved_at=?, winning_side=?, pnl=?, polymarket_result=?, kalshi_result=?, lock_valid=?,
                polymarket_snapshot_resolved=?, kalshi_snapshot_resolved=?
            WHERE id=?
            """,
            (
                datetime.utcnow().isoformat(),
                winning_side,
                pnl,
                polymarket_result,
                kalshi_result,
                1 if lock_valid else 0,
                polymarket_snapshot_resolved,
                kalshi_snapshot_resolved,
                position_id,
            ),
        )
        self.conn.commit()

    def get_position(self, position_id: str) -> CrossPosition | None:
        row = self.conn.execute("SELECT * FROM positions WHERE id=?", (position_id,)).fetchone()
        return self._row_to_position(row) if row else None

    def stats(self) -> dict:
        positions = self.get_all_positions()
        open_positions = [p for p in positions if p.status == "open"]
        realized = sum(p.pnl or 0.0 for p in positions if p.pnl is not None)
        realized_pm = 0.0
        realized_kalshi = 0.0
        transfer_pm = 0.0
        transfer_kalshi = 0.0
        for p in positions:
            if p.pnl is None:
                continue
            realized_split = self._split_realized_pnl(p)
            realized_pm += realized_split["polymarket"]
            realized_kalshi += realized_split["kalshi"]
        for transfer in self.get_all_transfers():
            if transfer["from_venue"] == "polymarket":
                transfer_pm -= float(transfer["amount"] or 0.0)
            else:
                transfer_kalshi -= float(transfer["amount"] or 0.0)
            if transfer["to_venue"] == "polymarket":
                transfer_pm += float(transfer["amount"] or 0.0)
            else:
                transfer_kalshi += float(transfer["amount"] or 0.0)
        locked_pm = sum(
            (p.yes_ask * p.shares if p.venue_yes == "polymarket" else 0.0) +
            (p.no_ask * p.shares if p.venue_no == "polymarket" else 0.0)
            for p in open_positions
        )
        locked_kalshi = sum(
            (p.yes_ask * p.shares if p.venue_yes == "kalshi" else 0.0) +
            (p.no_ask * p.shares if p.venue_no == "kalshi" else 0.0)
            for p in open_positions
        )
        return {
            "total": len(positions),
            "open": len(open_positions),
            "resolved": len([p for p in positions if p.status == "resolved"]),
            "realized_pnl": realized,
            "realized_pnl_polymarket": realized_pm,
            "realized_pnl_kalshi": realized_kalshi,
            "transfer_net_polymarket": transfer_pm,
            "transfer_net_kalshi": transfer_kalshi,
            "locked_polymarket": locked_pm,
            "locked_kalshi": locked_kalshi,
            "transfer_count": len(self.get_all_transfers()),
        }

    def record_transfer(self, from_venue: str, to_venue: str, amount: float, note: str = "") -> str:
        transfer_id = str(uuid.uuid4())
        self.conn.execute(
            """
            INSERT INTO transfers (id, from_venue, to_venue, amount, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                transfer_id,
                from_venue,
                to_venue,
                amount,
                note,
                datetime.utcnow().isoformat(),
            ),
        )
        self.conn.commit()
        return transfer_id

    def get_all_transfers(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM transfers ORDER BY created_at ASC").fetchall()

    def get_positions_with_liquidity(self) -> list[CrossPosition]:
        rows = self.conn.execute(
            """
            SELECT * FROM positions
            WHERE yes_available_shares IS NOT NULL AND no_available_shares IS NOT NULL
            ORDER BY opened_at DESC
            """
        ).fetchall()
        return [self._row_to_position(row) for row in rows]

    def _split_realized_pnl(self, position: CrossPosition) -> dict[str, float]:
        pm_cost = 0.0
        kalshi_cost = 0.0

        yes_cost = position.yes_ask * position.shares
        no_cost = position.no_ask * position.shares

        if position.venue_yes == "polymarket":
            pm_cost += yes_cost
            pm_cost += self._polymarket_fee(position.shares, position.yes_ask)
        else:
            kalshi_cost += yes_cost
            kalshi_cost += self._kalshi_fee(position.shares, position.yes_ask)

        if position.venue_no == "polymarket":
            pm_cost += no_cost
            pm_cost += self._polymarket_fee(position.shares, position.no_ask)
        else:
            kalshi_cost += no_cost
            kalshi_cost += self._kalshi_fee(position.shares, position.no_ask)

        pm_payout = 0.0
        kalshi_payout = 0.0

        if position.venue_yes == "polymarket" and position.polymarket_result == "yes":
            pm_payout += position.shares
        if position.venue_no == "polymarket" and position.polymarket_result == "no":
            pm_payout += position.shares

        if position.venue_yes == "kalshi" and position.kalshi_result == "yes":
            kalshi_payout += position.shares
        if position.venue_no == "kalshi" and position.kalshi_result == "no":
            kalshi_payout += position.shares

        return {
            "polymarket": pm_payout - pm_cost,
            "kalshi": kalshi_payout - kalshi_cost,
        }

    def _polymarket_fee(self, shares: float, price: float) -> float:
        fee_rate = 0.25
        exponent = 2
        return shares * price * fee_rate * ((price * (1 - price)) ** exponent)

    def _kalshi_fee(self, shares: float, price: float) -> float:
        raw = 0.07 * shares * price * (1 - price)
        cents = int(raw * 100)
        if raw * 100 > cents:
            cents += 1
        return cents / 100.0

    def get_all_positions(self) -> list[CrossPosition]:
        rows = self.conn.execute("SELECT * FROM positions ORDER BY opened_at DESC").fetchall()
        return [self._row_to_position(row) for row in rows]

    def _row_to_position(self, row) -> CrossPosition:
        return CrossPosition(
            id=row["id"],
            pair_key=row["pair_key"],
            symbol=row["symbol"],
            title=row["title"],
            expiry=datetime.fromisoformat(row["expiry"]),
            venue_yes=row["venue_yes"],
            market_yes=row["market_yes"],
            venue_no=row["venue_no"],
            market_no=row["market_no"],
            polymarket_title=row["polymarket_title"] if "polymarket_title" in row.keys() else None,
            kalshi_title=row["kalshi_title"] if "kalshi_title" in row.keys() else None,
            match_score=row["match_score"] if "match_score" in row.keys() else None,
            expiry_delta_seconds=row["expiry_delta_seconds"] if "expiry_delta_seconds" in row.keys() else None,
            polymarket_reference_price=row["polymarket_reference_price"] if "polymarket_reference_price" in row.keys() else None,
            kalshi_reference_price=row["kalshi_reference_price"] if "kalshi_reference_price" in row.keys() else None,
            polymarket_rules=row["polymarket_rules"] if "polymarket_rules" in row.keys() else None,
            kalshi_rules=row["kalshi_rules"] if "kalshi_rules" in row.keys() else None,
            polymarket_snapshot_open=row["polymarket_snapshot_open"] if "polymarket_snapshot_open" in row.keys() else None,
            kalshi_snapshot_open=row["kalshi_snapshot_open"] if "kalshi_snapshot_open" in row.keys() else None,
            polymarket_snapshot_resolved=row["polymarket_snapshot_resolved"] if "polymarket_snapshot_resolved" in row.keys() else None,
            kalshi_snapshot_resolved=row["kalshi_snapshot_resolved"] if "kalshi_snapshot_resolved" in row.keys() else None,
            shares=row["shares"],
            yes_ask=row["yes_ask"],
            no_ask=row["no_ask"],
            yes_requested_shares=row["yes_requested_shares"] if "yes_requested_shares" in row.keys() else None,
            yes_filled_shares=row["yes_filled_shares"] if "yes_filled_shares" in row.keys() else None,
            yes_available_shares=row["yes_available_shares"] if "yes_available_shares" in row.keys() else None,
            yes_avg_price=row["yes_avg_price"] if "yes_avg_price" in row.keys() else None,
            yes_best_ask=row["yes_best_ask"] if "yes_best_ask" in row.keys() else None,
            yes_remaining_shares_after_fill=row["yes_remaining_shares_after_fill"] if "yes_remaining_shares_after_fill" in row.keys() else None,
            no_requested_shares=row["no_requested_shares"] if "no_requested_shares" in row.keys() else None,
            no_filled_shares=row["no_filled_shares"] if "no_filled_shares" in row.keys() else None,
            no_available_shares=row["no_available_shares"] if "no_available_shares" in row.keys() else None,
            no_avg_price=row["no_avg_price"] if "no_avg_price" in row.keys() else None,
            no_best_ask=row["no_best_ask"] if "no_best_ask" in row.keys() else None,
            no_remaining_shares_after_fill=row["no_remaining_shares_after_fill"] if "no_remaining_shares_after_fill" in row.keys() else None,
            ask_sum=row["ask_sum"],
            total_cost=row["total_cost"],
            expected_profit=row["expected_profit"],
            opened_at=datetime.fromisoformat(row["opened_at"]),
            status=row["status"],
            resolved_at=datetime.fromisoformat(row["resolved_at"]) if row["resolved_at"] else None,
            winning_side=row["winning_side"],
            pnl=row["pnl"],
            polymarket_result=row["polymarket_result"],
            kalshi_result=row["kalshi_result"],
            lock_valid=bool(row["lock_valid"]) if row["lock_valid"] is not None else None,
        )
