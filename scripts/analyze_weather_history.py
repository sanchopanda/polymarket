"""Анализ исторических weather events (data/weather_history.db).

В истории у нас НЕТ модельного p_model (исторический NWS forecast не архивирован).
Поэтому мы тестируем market-only стратегии:

  1. Calibration: bin по YES-цене в момент T-X часов до resolution — какая
     реальная частота выигрыша.
  2. Backtest: «buy-NO когда YES-цена ≥ threshold», «buy-YES когда YES-цена ≤ threshold».
  3. Per-city & per-bucket-type разрезы.

Для каждого bucket находим последнюю цену до момента T = end_date - X часов
(default X=6). Это «входная цена» в бэктесте.
"""
from __future__ import annotations

import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from statistics import mean

DB_PATH = "data/weather_history.db"


def rms(xs):
    xs = list(xs)
    return (sum(x*x for x in xs)/len(xs))**0.5 if xs else 0.0


def sharpe(pnls):
    if len(pnls) < 2: return 0.0
    m = mean(pnls)
    sd = (sum((p-m)**2 for p in pnls)/len(pnls))**0.5
    return m/sd if sd > 0 else 0.0


@dataclass
class Entry:
    event_id: str
    bucket_id: int
    city: str
    event_date: str
    title: str
    lo: float
    hi: float
    yes_price: float   # цена YES за X часов до resolution
    is_winner: bool    # = final_outcome_yes
    is_tail: bool

    @property
    def buy_no_price(self) -> float:
        return 1.0 - self.yes_price


def load_entries(conn: sqlite3.Connection, hours_before: float = 6.0) -> list[Entry]:
    rows = conn.execute("""
        SELECT e.event_id, e.city_slug, e.event_date, e.end_date_utc,
               b.id AS bucket_id, b.title, b.lo, b.hi, b.final_outcome_yes,
               e.winning_bucket_id
        FROM events e JOIN buckets b ON b.event_id = e.event_id
        WHERE e.winning_bucket_id IS NOT NULL AND b.final_outcome_yes IS NOT NULL
    """).fetchall()

    out: list[Entry] = []
    for r in rows:
        end_ts = int(datetime.fromisoformat(r["end_date_utc"]).timestamp())
        target_ts = end_ts - int(hours_before * 3600)
        # последняя цена не позже target_ts (если есть), иначе скипаем
        px = conn.execute(
            "SELECT yes_price FROM prices WHERE bucket_id=? AND ts<=? ORDER BY ts DESC LIMIT 1",
            (r["bucket_id"], target_ts),
        ).fetchone()
        if px is None:
            continue
        yp = float(px["yes_price"])
        if yp <= 0 or yp >= 1:
            continue
        out.append(Entry(
            event_id=r["event_id"], bucket_id=r["bucket_id"], city=r["city_slug"],
            event_date=r["event_date"], title=r["title"],
            lo=r["lo"], hi=r["hi"], yes_price=yp,
            is_winner=bool(r["final_outcome_yes"]),
            is_tail=(r["lo"] <= -1e8 or r["hi"] >= 1e8),
        ))
    return out


def section_coverage(conn: sqlite3.Connection, entries: list[Entry]) -> None:
    n_ev = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    n_ev_res = conn.execute("SELECT COUNT(*) FROM events WHERE winning_bucket_id IS NOT NULL").fetchone()[0]
    n_bk = conn.execute("SELECT COUNT(*) FROM buckets").fetchone()[0]
    n_pr = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    dates = conn.execute("SELECT MIN(event_date), MAX(event_date) FROM events WHERE winning_bucket_id IS NOT NULL").fetchone()
    print("=== Coverage ===")
    print(f"  events total={n_ev} resolved={n_ev_res}")
    print(f"  buckets={n_bk}  price points={n_pr}")
    print(f"  date range: {dates[0]} .. {dates[1]}")
    print(f"  entries with price ≥6h before: {len(entries)}  (buckets with trade history)")
    winners = sum(1 for e in entries if e.is_winner)
    print(f"  winner entries: {winners} ({winners/max(len(entries),1)*100:.1f}%)")


def section_calibration(entries: list[Entry], lead_label: str) -> None:
    print(f"\n=== Market calibration ({lead_label}) ===")
    bins = [(0,0.05),(0.05,0.10),(0.10,0.20),(0.20,0.35),(0.35,0.55),(0.55,0.75),(0.75,0.90),(0.90,1.001)]
    print(f"    {'yes_price bin':>14}  n      emp_hit  mid    delta")
    for lo, hi in bins:
        sub = [e for e in entries if lo <= e.yes_price < hi]
        if not sub: continue
        hr = sum(1 for e in sub if e.is_winner) / len(sub)
        mid = (lo+hi)/2
        print(f"    {lo:>5.2f}-{hi:<7.2f}  {len(sub):>5}  {hr:>6.3f}  {mid:>5.2f}  {hr-mid:+.3f}")


def section_backtest(entries: list[Entry], label: str) -> None:
    print(f"\n=== Backtest 1 entry/bucket — first-signal analog ({label}) ===")
    print(f"    {'rule':>26}  n     wr     paid   pnl/$   total   sharpe")
    # Стратегия 1: buy NO когда yes_price >= thresh (т.е. рынок говорит YES >= thresh)
    for thresh in [0.50, 0.60, 0.70, 0.80, 0.85, 0.90]:
        ts = [e for e in entries if e.yes_price >= thresh]
        if not ts: continue
        pnls = [(1 - (1-e.yes_price)) if (not e.is_winner) else -(1-e.yes_price) for e in ts]
        # buy-NO pays (1-yes_price), wins 1 if YES loses
        paid = mean(1-e.yes_price for e in ts)
        wr = sum(1 for e in ts if not e.is_winner)/len(ts)
        print(f"    NO  yes_price ≥ {thresh:.2f}  {len(ts):>4}  {wr:.3f}  {paid:.3f}  {mean(pnls):+.4f}  ${sum(pnls):+7.2f}  {sharpe(pnls):+.2f}")

    # Стратегия 2: buy YES когда yes_price <= thresh
    for thresh in [0.05, 0.10, 0.15, 0.20, 0.30]:
        ts = [e for e in entries if e.yes_price <= thresh]
        if not ts: continue
        pnls = [(1 - e.yes_price) if e.is_winner else -e.yes_price for e in ts]
        paid = mean(e.yes_price for e in ts)
        wr = sum(1 for e in ts if e.is_winner)/len(ts)
        print(f"    YES yes_price ≤ {thresh:.2f}  {len(ts):>4}  {wr:.3f}  {paid:.3f}  {mean(pnls):+.4f}  ${sum(pnls):+7.2f}  {sharpe(pnls):+.2f}")


def section_bucket_type(entries: list[Entry]) -> None:
    print("\n=== Per bucket-type (middle vs tail), NO when yes_price ≥ 0.70 ===")
    groups = {"middle": [e for e in entries if not e.is_tail and e.yes_price >= 0.70],
              "tail":   [e for e in entries if e.is_tail and e.yes_price >= 0.70]}
    for k, ts in groups.items():
        if not ts:
            print(f"  {k}: n=0")
            continue
        pnls = [(1-(1-e.yes_price)) if not e.is_winner else -(1-e.yes_price) for e in ts]
        wr = sum(1 for e in ts if not e.is_winner)/len(ts)
        print(f"  {k:>6}: n={len(ts)} wr={wr:.3f} pnl/$={mean(pnls):+.4f} total=${sum(pnls):+.2f} sharpe={sharpe(pnls):+.2f}")


def section_per_city(entries: list[Entry]) -> None:
    print("\n=== Per-city (NO when yes_price ≥ 0.70) ===")
    by_city = defaultdict(list)
    for e in entries:
        if e.yes_price >= 0.70:
            by_city[e.city].append(e)
    print(f"    {'city':>14}  n    wr     pnl/$   total")
    for city in sorted(by_city):
        ts = by_city[city]
        pnls = [(1-(1-e.yes_price)) if not e.is_winner else -(1-e.yes_price) for e in ts]
        wr = sum(1 for e in ts if not e.is_winner)/len(ts)
        print(f"    {city:>14}  {len(ts):>3}  {wr:.3f}  {mean(pnls):+.4f}  ${sum(pnls):+.2f}")


def main() -> int:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    for lead_h, lab in [(6, "T-6h"), (24, "T-24h"), (48, "T-48h")]:
        entries = load_entries(conn, hours_before=lead_h)
        if lead_h == 6:
            section_coverage(conn, entries)
        section_calibration(entries, lab)
        section_backtest(entries, lab)
        if lead_h == 6:
            section_bucket_type(entries)
            section_per_city(entries)
        print("-" * 70)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
