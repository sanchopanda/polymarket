"""
One-time migration: read existing bot DBs → insert into volatility_bot.db as is_legacy=1.

Sources:
  fast_arb_bot.db  → 2 legs per position (YES + NO)
  real_arb_bot.db  → 2 legs per position
  momentum_bot.db  → 1 leg per position
  real_momentum_bot.db → 1 leg per position
"""
from __future__ import annotations

import re
import sqlite3
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# ── Adjust path so we can import project modules ───────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from cross_arb_bot.polymarket_feed import PolymarketFeed  # noqa: E402 (for _extract_interval_minutes)
from volatility_bot.db import VolatilityDB  # noqa: E402
from volatility_bot.models import Bet  # noqa: E402
from volatility_bot.strategy import (  # noqa: E402
    compute_market_minute,
    compute_market_quarter,
    compute_position_pct,
)

SOURCES = [
    ("fast_arb_bot",     str(ROOT / "data/fast_arb_bot.db"),     "arb"),
    ("real_arb_bot",     str(ROOT / "data/real_arb_bot.db"),     "arb"),
    ("momentum_bot",     str(ROOT / "data/momentum_bot.db"),     "momentum"),
    ("real_momentum_bot",str(ROOT / "data/real_momentum_bot.db"), "real_momentum"),
]

TARGET = str(ROOT / "data/volatility_bot.db")

# Reuse _extract_interval_minutes from PolymarketFeed
_feed = PolymarketFeed.__new__(PolymarketFeed)
_extract_interval_minutes = _feed._extract_interval_minutes


def classify_bucket(price: float) -> str:
    if 0.00 <= price <= 0.10:
        return "0-0.1"
    if 0.20 <= price <= 0.40:
        return "0.2-0.4"
    if 0.85 <= price <= 0.95:
        return "0.85-0.95"
    return "legacy_other"


def _dt(s: str | None) -> datetime | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def compute_timing(opened_at: datetime | None, expiry: datetime | None, interval: int) -> tuple[int, int, float]:
    """Returns (market_minute, market_quarter, position_pct) or (0, 1, 0.0) on failure."""
    if not opened_at or not expiry or interval <= 0:
        return 0, 1, 0.0
    market_start = expiry - timedelta(minutes=interval)
    pct = compute_position_pct(opened_at, market_start, interval)
    minute = compute_market_minute(opened_at, market_start)
    quarter = compute_market_quarter(pct)
    return minute, quarter, round(pct, 4)


def migrate_arb(source_name: str, source_path: str) -> list[Bet]:
    """Each arb position → 2 Bet objects (YES leg + NO leg)."""
    bets: list[Bet] = []
    try:
        conn = sqlite3.connect(source_path)
        conn.row_factory = sqlite3.Row
    except Exception as e:
        print(f"  [{source_name}] cannot open: {e}")
        return bets

    rows = conn.execute(
        """
        SELECT pair_key, symbol, polymarket_title, kalshi_title, expiry,
               venue_yes, market_yes, venue_no, market_no,
               yes_ask, no_ask, shares, pnl, actual_pnl,
               winning_side, opened_at, status, is_paper
        FROM positions
        WHERE status = 'resolved' OR status = 'open'
        """
    ).fetchall()
    conn.close()

    for r in rows:
        # Derive interval from title
        title = r["polymarket_title"] or r["kalshi_title"] or ""
        interval = _extract_interval_minutes(title) or 15

        expiry = _dt(r["expiry"])
        opened_at = _dt(r["opened_at"])
        market_start = expiry - timedelta(minutes=interval) if expiry else None

        market_minute, market_quarter, position_pct = compute_timing(opened_at, expiry, interval)
        winning_side = r["winning_side"]
        status = r["status"]

        # YES leg
        yes_price = r["yes_ask"] or 0.0
        shares = r["shares"] or 0.0
        yes_won = winning_side == "yes"
        yes_pnl = (shares - yes_price * shares) if yes_won else (-yes_price * shares)

        bets.append(Bet(
            id=str(uuid.uuid4()),
            venue=r["venue_yes"],
            market_id=r["market_yes"],
            symbol=r["symbol"],
            interval_minutes=interval,
            market_start=market_start,
            market_end=expiry,
            opened_at=opened_at,
            market_minute=market_minute,
            market_quarter=market_quarter,
            position_pct=position_pct,
            side="yes",
            entry_price=yes_price,
            trigger_bucket=classify_bucket(yes_price),
            shares=shares,
            total_cost=yes_price * shares,
            status=status,
            winning_side=winning_side if status == "resolved" else None,
            pnl=round(yes_pnl, 6) if status == "resolved" else None,
            is_paper=int(r["is_paper"] or 0),
            is_legacy=1,
            legacy_source=source_name,
            legacy_pair_key=r["pair_key"],
        ))

        # NO leg
        no_price = r["no_ask"] or 0.0
        no_won = winning_side == "no"
        no_pnl = (shares - no_price * shares) if no_won else (-no_price * shares)

        bets.append(Bet(
            id=str(uuid.uuid4()),
            venue=r["venue_no"],
            market_id=r["market_no"],
            symbol=r["symbol"],
            interval_minutes=interval,
            market_start=market_start,
            market_end=expiry,
            opened_at=opened_at,
            market_minute=market_minute,
            market_quarter=market_quarter,
            position_pct=position_pct,
            side="no",
            entry_price=no_price,
            trigger_bucket=classify_bucket(no_price),
            shares=shares,
            total_cost=no_price * shares,
            status=status,
            winning_side=winning_side if status == "resolved" else None,
            pnl=round(no_pnl, 6) if status == "resolved" else None,
            is_paper=int(r["is_paper"] or 0),
            is_legacy=1,
            legacy_source=source_name,
            legacy_pair_key=r["pair_key"],
        ))

    return bets


def migrate_momentum(source_name: str, source_path: str, is_paper: int) -> list[Bet]:
    """Each momentum position → 1 Bet object."""
    bets: list[Bet] = []
    try:
        conn = sqlite3.connect(source_path)
        conn.row_factory = sqlite3.Row
    except Exception as e:
        print(f"  [{source_name}] cannot open: {e}")
        return bets

    rows = conn.execute(
        """
        SELECT pair_key, symbol, title, expiry, bet_venue, side,
               entry_price, shares, total_cost, pnl, outcome,
               opened_at, status
        FROM positions
        WHERE status = 'resolved' OR status = 'open'
        """
    ).fetchall()
    conn.close()

    for r in rows:
        title = r["title"] or ""
        interval = _extract_interval_minutes(title) or 15
        expiry = _dt(r["expiry"])
        opened_at = _dt(r["opened_at"])

        market_minute, market_quarter, position_pct = compute_timing(opened_at, expiry, interval)
        market_start = expiry - timedelta(minutes=interval) if expiry else None

        entry_price = float(r["entry_price"] or 0.0)
        status = r["status"]
        outcome = r["outcome"]
        winning_side = outcome if status == "resolved" else None
        pnl = float(r["pnl"]) if r["pnl"] is not None and status == "resolved" else None

        # Derive market_id from pair_key
        pair_key = r["pair_key"] or ""
        venue = r["bet_venue"] or "polymarket"
        # pair_key format: "{pm_market_id}:{kalshi_ticker}" or similar with | separator
        market_id = pair_key
        if ":" in pair_key:
            parts = pair_key.split(":", 1)
            market_id = parts[0] if venue == "polymarket" else parts[1]
        elif "|" in pair_key:
            parts = pair_key.split("|", 1)
            market_id = parts[0] if venue == "polymarket" else parts[1]

        bets.append(Bet(
            id=str(uuid.uuid4()),
            venue=venue,
            market_id=market_id,
            symbol=r["symbol"],
            interval_minutes=interval,
            market_start=market_start,
            market_end=expiry,
            opened_at=opened_at,
            market_minute=market_minute,
            market_quarter=market_quarter,
            position_pct=position_pct,
            side=r["side"],
            entry_price=entry_price,
            trigger_bucket=classify_bucket(entry_price),
            shares=float(r["shares"] or 0.0),
            total_cost=float(r["total_cost"] or 0.0),
            status=status,
            winning_side=winning_side,
            pnl=round(pnl, 6) if pnl is not None else None,
            is_paper=is_paper,
            is_legacy=1,
            legacy_source=source_name,
            legacy_pair_key=pair_key,
        ))

    return bets


def main() -> None:
    db = VolatilityDB(TARGET)
    print(f"Target: {TARGET}\n")

    total_inserted = 0
    total_skipped = 0

    for source_name, source_path, kind in SOURCES:
        if not Path(source_path).exists():
            print(f"  [{source_name}] NOT FOUND, skipping")
            continue

        if kind == "arb":
            bets = migrate_arb(source_name, source_path)
        elif kind == "momentum":
            bets = migrate_momentum(source_name, source_path, is_paper=1)
        else:  # real_momentum
            bets = migrate_momentum(source_name, source_path, is_paper=0)

        inserted = 0
        skipped = 0
        for bet in bets:
            try:
                db.record_bet(bet)
                inserted += 1
            except Exception:
                skipped += 1

        print(f"  [{source_name}] {len(bets)} rows → inserted={inserted} skipped(dup)={skipped}")
        total_inserted += inserted
        total_skipped += skipped

    print(f"\nDone: {total_inserted} inserted, {total_skipped} skipped")
    stats = db.stats()
    print(f"DB stats: {stats}")


if __name__ == "__main__":
    main()
