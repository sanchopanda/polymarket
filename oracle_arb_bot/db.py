from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from oracle_arb_bot.models import OracleBet, OracleSignal, RealBet


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


class OracleDB:
    def __init__(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS bets (
                id                      TEXT    PRIMARY KEY,
                market_id               TEXT    NOT NULL,
                symbol                  TEXT    NOT NULL,
                interval_minutes        INTEGER NOT NULL,
                market_start            TEXT    NOT NULL,
                market_end              TEXT    NOT NULL,
                opened_at               TEXT    NOT NULL,
                market_minute           INTEGER NOT NULL,
                position_pct            REAL    NOT NULL,
                side                    TEXT    NOT NULL,
                entry_price             REAL    NOT NULL,
                shares                  REAL    NOT NULL,
                total_cost              REAL    NOT NULL,
                binance_price_at_start  REAL,
                binance_price_at_bet    REAL    NOT NULL,
                delta_pct               REAL    NOT NULL,
                pm_open_price           REAL    NOT NULL,
                pm_close_price          REAL,
                status                  TEXT    NOT NULL DEFAULT 'open',
                resolved_at             TEXT,
                winning_side            TEXT,
                pnl                     REAL,
                UNIQUE (market_id, side)
            );

            CREATE TABLE IF NOT EXISTS signals (
                id              TEXT    PRIMARY KEY,
                market_id       TEXT    NOT NULL,
                symbol          TEXT    NOT NULL,
                interval_minutes INTEGER NOT NULL,
                market_minute   INTEGER NOT NULL,
                position_pct    REAL    NOT NULL,
                fired_at        TEXT    NOT NULL,
                side            TEXT    NOT NULL,       -- "yes" | "no"
                delta_pct       REAL    NOT NULL,
                pm_open_price   REAL    NOT NULL,
                binance_price   REAL    NOT NULL,
                pm_yes_ask      REAL    NOT NULL,       -- PM цена YES в момент сигнала
                pm_no_ask       REAL    NOT NULL,       -- PM цена NO в момент сигнала
                bet_placed      INTEGER NOT NULL DEFAULT 0,
                UNIQUE (market_id, side)
            );

            CREATE TABLE IF NOT EXISTS real_deposit (
                id          INTEGER PRIMARY KEY,   -- always row 1
                balance     REAL    NOT NULL,
                peak        REAL    NOT NULL,
                updated_at  TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS real_bets (
                id                  TEXT    PRIMARY KEY,
                market_id           TEXT    NOT NULL,
                symbol              TEXT    NOT NULL,
                interval_minutes    INTEGER NOT NULL,
                market_start        TEXT,
                market_end          TEXT,
                placed_at           TEXT    NOT NULL,
                market_minute       INTEGER NOT NULL,
                side                TEXT    NOT NULL,
                requested_price     REAL    NOT NULL,
                fill_price          REAL    NOT NULL,
                shares_requested    REAL    NOT NULL,
                shares_filled       REAL    NOT NULL,
                stake_usd           REAL    NOT NULL,
                order_id            TEXT,
                order_status        TEXT    NOT NULL,
                delta_pct           REAL    NOT NULL,
                pm_open_price       REAL,
                binance_price_at_bet REAL,
                pm_close_price      REAL,
                status              TEXT    NOT NULL DEFAULT 'open',
                resolved_at         TEXT,
                winning_side        TEXT,
                pnl                 REAL,
                pm_price_10s        REAL
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT    NOT NULL,
                event_type  TEXT    NOT NULL,
                bet_id      TEXT,
                details     TEXT
            );
        """)
        self.conn.commit()
        self._migrate_alter()

    def _migrate_alter(self) -> None:
        """Добавляет новые колонки и снимает устаревшие ограничения."""
        for sql in [
            "ALTER TABLE bets ADD COLUMN pm_price_10s REAL",
            "ALTER TABLE real_bets ADD COLUMN pm_price_10s REAL",
            "ALTER TABLE bets ADD COLUMN crossing_seq INTEGER DEFAULT 1",
            "ALTER TABLE bets ADD COLUMN venue TEXT DEFAULT 'polymarket'",
            "ALTER TABLE bets ADD COLUMN seconds_to_close INTEGER",
            "ALTER TABLE bets ADD COLUMN opposite_ask REAL",
            "ALTER TABLE bets ADD COLUMN depth_usd REAL",
            "ALTER TABLE bets ADD COLUMN volume REAL",
            "ALTER TABLE bets ADD COLUMN binance_price_at_close REAL",
            "ALTER TABLE bets ADD COLUMN strategy TEXT DEFAULT 'crossing'",
            "ALTER TABLE bets ADD COLUMN signal_ask REAL",
        ]:
            try:
                self.conn.execute(sql)
                self.conn.commit()
            except Exception:
                pass  # колонка уже существует

        # Снимаем UNIQUE(market_id, side) — разрешаем множественные ставки на пересечениях
        row = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='bets'"
        ).fetchone()
        if row and "UNIQUE" in (row[0] or ""):
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS bets_new (
                    id                      TEXT    PRIMARY KEY,
                    market_id               TEXT    NOT NULL,
                    symbol                  TEXT    NOT NULL,
                    interval_minutes        INTEGER NOT NULL,
                    market_start            TEXT    NOT NULL,
                    market_end              TEXT    NOT NULL,
                    opened_at               TEXT    NOT NULL,
                    market_minute           INTEGER NOT NULL,
                    position_pct            REAL    NOT NULL,
                    side                    TEXT    NOT NULL,
                    entry_price             REAL    NOT NULL,
                    shares                  REAL    NOT NULL,
                    total_cost              REAL    NOT NULL,
                    binance_price_at_start  REAL,
                    binance_price_at_bet    REAL    NOT NULL,
                    delta_pct               REAL    NOT NULL,
                    pm_open_price           REAL    NOT NULL,
                    pm_close_price          REAL,
                    status                  TEXT    NOT NULL DEFAULT 'open',
                    resolved_at             TEXT,
                    winning_side            TEXT,
                    pnl                     REAL,
                    pm_price_10s            REAL,
                    crossing_seq            INTEGER DEFAULT 1,
                    venue                   TEXT    DEFAULT 'polymarket',
                    seconds_to_close        INTEGER,
                    opposite_ask            REAL,
                    depth_usd               REAL,
                    volume                  REAL,
                    binance_price_at_close  REAL,
                    strategy                TEXT    DEFAULT 'crossing'
                );
                INSERT INTO bets_new SELECT
                    id, market_id, symbol, interval_minutes,
                    market_start, market_end, opened_at,
                    market_minute, position_pct, side,
                    entry_price, shares, total_cost,
                    binance_price_at_start, binance_price_at_bet,
                    delta_pct, pm_open_price, pm_close_price,
                    status, resolved_at, winning_side, pnl,
                    pm_price_10s, crossing_seq, 'polymarket',
                    NULL, NULL, NULL, NULL, NULL, 'crossing'
                FROM bets;
                DROP TABLE bets;
                ALTER TABLE bets_new RENAME TO bets;
            """)

        # Снимаем UNIQUE(market_id, side) с real_bets
        row = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='real_bets'"
        ).fetchone()
        if row and "UNIQUE" in (row[0] or ""):
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS real_bets_new (
                    id                  TEXT    PRIMARY KEY,
                    market_id           TEXT    NOT NULL,
                    symbol              TEXT    NOT NULL,
                    interval_minutes    INTEGER NOT NULL,
                    market_start        TEXT    NOT NULL,
                    market_end          TEXT    NOT NULL,
                    placed_at           TEXT    NOT NULL,
                    market_minute       INTEGER NOT NULL,
                    side                TEXT    NOT NULL,
                    requested_price     REAL    NOT NULL,
                    fill_price          REAL    NOT NULL,
                    shares_requested    REAL    NOT NULL,
                    shares_filled       REAL    NOT NULL,
                    stake_usd           REAL    NOT NULL,
                    order_id            TEXT,
                    order_status        TEXT    NOT NULL,
                    delta_pct           REAL    NOT NULL,
                    pm_open_price       REAL    NOT NULL,
                    binance_price_at_bet REAL   NOT NULL,
                    pm_close_price      REAL,
                    status              TEXT    NOT NULL DEFAULT 'open',
                    resolved_at         TEXT,
                    winning_side        TEXT,
                    pnl                 REAL,
                    pm_price_10s        REAL
                );
                INSERT INTO real_bets_new SELECT * FROM real_bets;
                DROP TABLE real_bets;
                ALTER TABLE real_bets_new RENAME TO real_bets;
            """)

        # Снимаем NOT NULL с market_start/market_end/pm_open_price/binance_price_at_bet
        # (могут быть None после перезапуска бота)
        row = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='real_bets'"
        ).fetchone()
        if row and "market_start" in (row[0] or "") and "NOT NULL" in (row[0] or "").split("market_start")[1].split(",")[0]:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS real_bets_v3 (
                    id                  TEXT    PRIMARY KEY,
                    market_id           TEXT    NOT NULL,
                    symbol              TEXT    NOT NULL,
                    interval_minutes    INTEGER NOT NULL,
                    market_start        TEXT,
                    market_end          TEXT,
                    placed_at           TEXT    NOT NULL,
                    market_minute       INTEGER NOT NULL,
                    side                TEXT    NOT NULL,
                    requested_price     REAL    NOT NULL,
                    fill_price          REAL    NOT NULL,
                    shares_requested    REAL    NOT NULL,
                    shares_filled       REAL    NOT NULL,
                    stake_usd           REAL    NOT NULL,
                    order_id            TEXT,
                    order_status        TEXT    NOT NULL,
                    delta_pct           REAL    NOT NULL,
                    pm_open_price       REAL,
                    binance_price_at_bet REAL,
                    pm_close_price      REAL,
                    status              TEXT    NOT NULL DEFAULT 'open',
                    resolved_at         TEXT,
                    winning_side        TEXT,
                    pnl                 REAL,
                    pm_price_10s        REAL
                );
                INSERT INTO real_bets_v3 SELECT * FROM real_bets;
                DROP TABLE real_bets;
                ALTER TABLE real_bets_v3 RENAME TO real_bets;
            """)

    # ── Write ─────────────────────────────────────────────────────────────

    def record_signal(self, signal: OracleSignal) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO signals (
                id, market_id, symbol, interval_minutes,
                market_minute, position_pct, fired_at,
                side, delta_pct, pm_open_price, binance_price,
                pm_yes_ask, pm_no_ask, bet_placed
            ) VALUES (?,?,?,?,  ?,?,?,  ?,?,?,?,  ?,?,?)
            """,
            (
                signal.id, signal.market_id, signal.symbol, signal.interval_minutes,
                signal.market_minute, signal.position_pct, _iso(signal.fired_at),
                signal.side, signal.delta_pct, signal.pm_open_price, signal.binance_price,
                signal.pm_yes_ask, signal.pm_no_ask, int(signal.bet_placed),
            ),
        )
        self.conn.commit()

    def mark_signal_bet_placed(self, market_id: str, side: str) -> None:
        self.conn.execute(
            "UPDATE signals SET bet_placed=1 WHERE market_id=? AND side=?",
            (market_id, side),
        )
        self.conn.commit()

    def record_bet(self, bet: OracleBet, crossing_seq: int = 1,
                   signal_ask: float = 0.0) -> None:
        self.conn.execute(
            """
            INSERT INTO bets (
                id, market_id, symbol, interval_minutes,
                market_start, market_end, opened_at,
                market_minute, position_pct,
                side, entry_price, shares, total_cost,
                binance_price_at_start, binance_price_at_bet, delta_pct, pm_open_price,
                pm_close_price, status, resolved_at, winning_side, pnl,
                crossing_seq, venue,
                seconds_to_close, opposite_ask, depth_usd, volume,
                strategy, signal_ask
            ) VALUES (
                ?,?,?,?,  ?,?,?,  ?,?,  ?,?,?,?,  ?,?,?,?,  ?,?,?,?,?,  ?,?,  ?,?,?,?,  ?,?
            )
            """,
            (
                bet.id, bet.market_id, bet.symbol, bet.interval_minutes,
                _iso(bet.market_start), _iso(bet.market_end), _iso(bet.opened_at),
                bet.market_minute, bet.position_pct,
                bet.side, bet.entry_price, bet.shares, bet.total_cost,
                bet.binance_price_at_start, bet.binance_price_at_bet,
                bet.delta_pct, bet.pm_open_price,
                bet.pm_close_price, bet.status, _iso(bet.resolved_at),
                bet.winning_side, bet.pnl,
                crossing_seq, bet.venue,
                bet.seconds_to_close, bet.opposite_ask, bet.depth_usd, bet.volume,
                bet.strategy, signal_ask or None,
            ),
        )
        self.conn.commit()

    def resolve_bet(
        self,
        bet_id: str,
        winning_side: str,
        pm_close_price: Optional[float],
        pnl: float,
        binance_price_at_close: Optional[float] = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE bets
            SET status='resolved', resolved_at=?, winning_side=?, pm_close_price=?, pnl=?,
                binance_price_at_close=?
            WHERE id=?
            """,
            (_iso(datetime.utcnow()), winning_side, pm_close_price, round(pnl, 6),
             binance_price_at_close, bet_id),
        )
        self.conn.commit()

    def update_price_10s(self, bet_id: str, price: float, table: str = "bets") -> None:
        self.conn.execute(
            f"UPDATE {table} SET pm_price_10s=? WHERE id=?",
            (round(price, 6), bet_id),
        )
        self.conn.commit()

    def audit(self, event_type: str, bet_id: Optional[str], details: dict) -> None:
        self.conn.execute(
            "INSERT INTO audit_log (timestamp, event_type, bet_id, details) VALUES (?,?,?,?)",
            (_iso(datetime.utcnow()), event_type, bet_id, json.dumps(details)),
        )
        self.conn.commit()

    # ── Read ──────────────────────────────────────────────────────────────

    def has_bet(self, market_id: str, side: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM bets WHERE market_id=? AND side=?",
            (market_id, side),
        ).fetchone()
        return row is not None

    def has_any_bet(self, market_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM bets WHERE market_id=?",
            (market_id,),
        ).fetchone()
        return row is not None

    def count_bets_for_market(self, market_id: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) FROM bets WHERE market_id=?", (market_id,)
        ).fetchone()
        return row[0]

    def has_both_sides(self, market_id: str) -> bool:
        """True если есть ставки и на YES, и на NO — арбитраж."""
        row = self.conn.execute(
            "SELECT COUNT(DISTINCT side) FROM bets WHERE market_id=?", (market_id,)
        ).fetchone()
        return row[0] >= 2

    def get_open_bets(self) -> list[OracleBet]:
        rows = self.conn.execute(
            "SELECT * FROM bets WHERE status='open'"
        ).fetchall()
        return [self._row_to_bet(r) for r in rows]

    def get_recent_bets(self, limit: int = 50) -> list[OracleBet]:
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
        return {
            "total": total,
            "resolved": resolved[0] or 0,
            "realized_pnl": round(resolved[1] or 0.0, 4),
            "open": open_cnt,
        }

    def get_status_text(self) -> str:
        s = self.stats()
        wins = self.conn.execute(
            "SELECT COUNT(*) FROM bets WHERE status='resolved' AND winning_side=side"
        ).fetchone()[0]
        win_rate = round(wins / s["resolved"] * 100) if s["resolved"] else 0
        pnl_sign = "+" if s["realized_pnl"] >= 0 else ""
        lines = [
            "<b>OracleArb — статус</b>",
            "",
            "<b>Paper</b>",
            f"Ставок:      {s['total']} (открыто {s['open']})",
            f"Резолвнуто:  {s['resolved']} (win {win_rate}%)",
            f"PnL:         {pnl_sign}${s['realized_pnl']:.2f}",
        ]
        rs = self.real_stats()
        if rs["total"] > 0 or rs["balance"] > 0:
            r_pnl_sign = "+" if rs["realized_pnl"] >= 0 else ""
            lines += [
                "",
                "<b>Real</b>",
                f"Депозит:     ${rs['balance']:.2f} (peak ${rs['peak']:.2f})",
                f"Ставок:      {rs['total']} (открыто {rs['open']})",
                f"Резолвнуто:  {rs['resolved']} (win {rs['win_rate']}%)",
                f"PnL:         {r_pnl_sign}${rs['realized_pnl']:.2f}",
            ]
        return "\n".join(lines)

    # ── Real deposit ──────────────────────────────────────────────────────

    def init_real_deposit(self, amount: float) -> None:
        """Создаёт запись депозита если ещё нет."""
        now = _iso(datetime.utcnow())
        self.conn.execute(
            "INSERT OR IGNORE INTO real_deposit (id, balance, peak, updated_at) VALUES (1,?,?,?)",
            (amount, amount, now),
        )
        self.conn.commit()

    def get_real_deposit(self) -> tuple[float, float]:
        """Возвращает (balance, peak)."""
        row = self.conn.execute("SELECT balance, peak FROM real_deposit WHERE id=1").fetchone()
        if row is None:
            return 0.0, 0.0
        return row["balance"], row["peak"]

    def _update_real_deposit(self, balance: float, peak: float) -> None:
        self.conn.execute(
            "UPDATE real_deposit SET balance=?, peak=?, updated_at=? WHERE id=1",
            (round(balance, 6), round(peak, 6), _iso(datetime.utcnow())),
        )
        self.conn.commit()

    def set_real_balance(self, balance: float) -> None:
        """Синхронизирует баланс с реальным значением, обновляет peak если нужно."""
        _, peak = self.get_real_deposit()
        self._update_real_deposit(balance, max(peak, balance))

    def deduct_real_deposit(self, amount: float) -> None:
        bal, peak = self.get_real_deposit()
        self._update_real_deposit(bal - amount, peak)

    def add_real_deposit(self, amount: float) -> None:
        bal, peak = self.get_real_deposit()
        new_bal = bal + amount
        self._update_real_deposit(new_bal, max(peak, new_bal))

    # ── Real bets ─────────────────────────────────────────────────────────

    def has_real_bet(self, market_id: str, side: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM real_bets WHERE market_id=? AND side=?",
            (market_id, side),
        ).fetchone()
        return row is not None

    def record_real_bet(self, bet: RealBet) -> None:
        try:
            self.conn.execute(
                """
                INSERT INTO real_bets (
                    id, market_id, symbol, interval_minutes,
                    market_start, market_end, placed_at,
                    market_minute, side,
                    requested_price, fill_price, shares_requested, shares_filled,
                    stake_usd, order_id, order_status,
                    delta_pct, pm_open_price, binance_price_at_bet,
                    pm_close_price, status, resolved_at, winning_side, pnl
                ) VALUES (?,?,?,?,  ?,?,?,  ?,?,  ?,?,?,?,  ?,?,?,  ?,?,?,  ?,?,?,?,?)
                """,
                (
                    bet.id, bet.market_id, bet.symbol, bet.interval_minutes,
                    _iso(bet.market_start), _iso(bet.market_end), _iso(bet.placed_at),
                    bet.market_minute, bet.side,
                    bet.requested_price, bet.fill_price, bet.shares_requested, bet.shares_filled,
                    bet.stake_usd, bet.order_id, bet.order_status,
                    bet.delta_pct, bet.pm_open_price, bet.binance_price_at_bet,
                    bet.pm_close_price, bet.status, _iso(bet.resolved_at),
                    bet.winning_side, bet.pnl,
                ),
            )
            self.conn.commit()
        except Exception as exc:
            print(f"[db] ОШИБКА record_real_bet {bet.id}: {exc}")
            print(f"[db]   market_start={bet.market_start} market_end={bet.market_end} "
                  f"pm_open_price={bet.pm_open_price}")
            raise

    def resolve_real_bet(
        self,
        bet_id: str,
        winning_side: str,
        pm_close_price: Optional[float],
        pnl: float,
    ) -> None:
        self.conn.execute(
            """
            UPDATE real_bets
            SET status='resolved', resolved_at=?, winning_side=?, pm_close_price=?, pnl=?
            WHERE id=?
            """,
            (_iso(datetime.utcnow()), winning_side, pm_close_price, round(pnl, 6), bet_id),
        )
        self.conn.commit()

    def get_open_real_bets(self) -> list[RealBet]:
        rows = self.conn.execute(
            "SELECT * FROM real_bets WHERE status='open'"
        ).fetchall()
        return [self._row_to_real_bet(r) for r in rows]

    def real_stats(self) -> dict:
        row = self.conn.execute("SELECT balance, peak FROM real_deposit WHERE id=1").fetchone()
        balance = row["balance"] if row else 0.0
        peak = row["peak"] if row else 0.0
        resolved = self.conn.execute(
            "SELECT COUNT(*), SUM(pnl) FROM real_bets WHERE status='resolved'"
        ).fetchone()
        wins = self.conn.execute(
            "SELECT COUNT(*) FROM real_bets WHERE status='resolved' AND winning_side=side"
        ).fetchone()[0]
        total = self.conn.execute("SELECT COUNT(*) FROM real_bets").fetchone()[0]
        open_cnt = self.conn.execute(
            "SELECT COUNT(*) FROM real_bets WHERE status='open'"
        ).fetchone()[0]
        resolved_cnt = resolved[0] or 0
        win_rate = round(wins / resolved_cnt * 100) if resolved_cnt else 0
        return {
            "balance": round(balance, 2),
            "peak": round(peak, 2),
            "total": total,
            "resolved": resolved_cnt,
            "wins": wins,
            "win_rate": win_rate,
            "realized_pnl": round(resolved[1] or 0.0, 2),
            "open": open_cnt,
        }

    def _row_to_real_bet(self, row: sqlite3.Row) -> RealBet:
        return RealBet(
            id=row["id"],
            market_id=row["market_id"],
            symbol=row["symbol"],
            interval_minutes=row["interval_minutes"],
            market_start=_dt(row["market_start"]),
            market_end=_dt(row["market_end"]),
            placed_at=_dt(row["placed_at"]),
            market_minute=row["market_minute"],
            side=row["side"],
            requested_price=row["requested_price"],
            fill_price=row["fill_price"],
            shares_requested=row["shares_requested"],
            shares_filled=row["shares_filled"],
            stake_usd=row["stake_usd"],
            order_id=row["order_id"],
            order_status=row["order_status"],
            delta_pct=row["delta_pct"],
            pm_open_price=row["pm_open_price"],
            binance_price_at_bet=row["binance_price_at_bet"],
            pm_close_price=row["pm_close_price"],
            status=row["status"],
            resolved_at=_dt(row["resolved_at"]),
            winning_side=row["winning_side"],
            pnl=row["pnl"],
        )

    def _row_to_bet(self, row: sqlite3.Row) -> OracleBet:
        keys = row.keys()
        return OracleBet(
            id=row["id"],
            market_id=row["market_id"],
            symbol=row["symbol"],
            interval_minutes=row["interval_minutes"],
            market_start=_dt(row["market_start"]),
            market_end=_dt(row["market_end"]),
            opened_at=_dt(row["opened_at"]),
            market_minute=row["market_minute"],
            position_pct=row["position_pct"],
            side=row["side"],
            entry_price=row["entry_price"],
            shares=row["shares"],
            total_cost=row["total_cost"],
            binance_price_at_start=row["binance_price_at_start"],
            binance_price_at_bet=row["binance_price_at_bet"],
            delta_pct=row["delta_pct"],
            pm_open_price=row["pm_open_price"],
            pm_close_price=row["pm_close_price"],
            pm_price_10s=row["pm_price_10s"] if "pm_price_10s" in keys else None,
            venue=row["venue"] if "venue" in keys else "polymarket",
            seconds_to_close=row["seconds_to_close"] if "seconds_to_close" in keys else None,
            opposite_ask=row["opposite_ask"] if "opposite_ask" in keys else None,
            depth_usd=row["depth_usd"] if "depth_usd" in keys else None,
            volume=row["volume"] if "volume" in keys else None,
            binance_price_at_close=row["binance_price_at_close"] if "binance_price_at_close" in keys else None,
            strategy=row["strategy"] if "strategy" in keys else "crossing",
            status=row["status"],
            resolved_at=_dt(row["resolved_at"]),
            winning_side=row["winning_side"],
            pnl=row["pnl"],
        )
