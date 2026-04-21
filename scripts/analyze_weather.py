"""Честный анализ собранных weather_bot данных.

Использует yes_best_ask для buy-YES и (1 - yes_best_bid) для buy-NO — это реальная
цена fill'а на CLOB. Падает обратно на yes_price/no_price для старых снимков, где
колонок ещё не было (до schema-миграции).

Разделы:
  1. Forecast error diagnostics    (RMSE, bias, per-city, per-lead)
  2. Calibration                    (p_model vs emp hit rate, то же для рынка)
  3. Spread & liquidity profile     (что у нас вообще торгуется)
  4. Backtest first-signal          (1 trade per bucket, по best_ask — честно)
  5. Intraday edge                  (observed_max уже отрезал бакет — «free money»)
  6. Strategy grid-search           (top-N конфигов по PnL на fill)
"""
from __future__ import annotations

import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from statistics import mean
from typing import Iterable

DB_PATH = "data/weather_bot.db"

# Комиссии Polymarket ~0 на CLOB (maker ребейт), в backtest считаем 0.
# Если хочешь учесть - выставь FEE_BPS.
FEE_BPS = 0.0


# ---------- helpers ----------

def rms(xs: Iterable[float]) -> float:
    xs = list(xs)
    if not xs:
        return 0.0
    return (sum(x * x for x in xs) / len(xs)) ** 0.5


def sharpe(pnls: list[float]) -> float:
    if len(pnls) < 2:
        return 0.0
    m = mean(pnls)
    var = sum((p - m) ** 2 for p in pnls) / len(pnls)
    sd = var ** 0.5
    return m / sd if sd > 0 else 0.0


def effective_buy_yes_price(row: sqlite3.Row) -> float:
    """Цена, по которой реально купим YES: best_ask если есть, иначе fallback yes_price."""
    ba = row["yes_best_ask"] if "yes_best_ask" in row.keys() else None
    if ba is not None and ba > 0:
        return float(ba) * (1 + FEE_BPS / 1e4)
    return float(row["yes_price"])


def effective_buy_no_price(row: sqlite3.Row) -> float:
    """Цена, по которой реально купим NO = 1 - yes_best_bid. Fallback: no_price."""
    bb = row["yes_best_bid"] if "yes_best_bid" in row.keys() else None
    if bb is not None and bb > 0:
        return (1.0 - float(bb)) * (1 + FEE_BPS / 1e4)
    return float(row["no_price"])


# ---------- data loading ----------

@dataclass
class Snap:
    ts: str
    bucket_id: int
    forecast_id: int
    p_model: float
    yes_price: float
    no_price: float
    edge_yes: float
    yes_best_ask: float
    yes_best_bid: float
    yes_spread: float
    liquidity_num: float
    volume_24h: float
    observed_max_f: float | None
    hours_remaining: float
    # from joins
    event_id: str
    bucket_title: str
    lo: float
    hi: float
    city_slug: str
    event_date: str
    mu_f: float
    sigma_f: float
    lead_hours: float
    actual_high_f: float
    winning_bucket_id: int
    hit: bool

    @property
    def is_tail(self) -> bool:
        return self.lo <= -1e8 or self.hi >= 1e8

    @property
    def buy_yes_price(self) -> float:
        if self.yes_best_ask and self.yes_best_ask > 0:
            return self.yes_best_ask * (1 + FEE_BPS / 1e4)
        return self.yes_price

    @property
    def buy_no_price(self) -> float:
        if self.yes_best_bid and self.yes_best_bid > 0:
            return (1.0 - self.yes_best_bid) * (1 + FEE_BPS / 1e4)
        return self.no_price

    @property
    def effective_edge_yes(self) -> float:
        return self.p_model - self.buy_yes_price

    @property
    def effective_edge_no(self) -> float:
        """Edge на покупку NO = (1-p_model) - buy_no_price."""
        return (1.0 - self.p_model) - self.buy_no_price

    def observed_cuts_bucket(self) -> bool | None:
        """Точно ли observed_max уже отрезал бакет (impossible hit)?

        Возвращает True если observed_max > hi (rounded) → бакет заведомо проигран.
        None если данных нет / день ещё не начался.
        """
        if self.observed_max_f is None:
            return None
        # бакет выигрывает если rounded actual ∈ [lo, hi]. Если observed_max > hi+0.49
        # то будущий max тоже ≥ observed, тоже > hi → бакет проигран.
        if self.hi < 1e8 and self.observed_max_f > self.hi + 0.49:
            return True
        return False


def load_snaps(db: sqlite3.Connection) -> list[Snap]:
    # detect available columns (graceful если миграция ещё не прошла на БД)
    cols = {r[1] for r in db.execute("PRAGMA table_info(snapshots)").fetchall()}
    def c(col: str, fallback: str) -> str:
        return f"s.{col}" if col in cols else fallback
    sql = f"""
        SELECT
            s.ts, s.bucket_id, s.forecast_id,
            s.p_model, s.yes_price, s.no_price, s.edge_yes,
            {c("yes_best_ask",    "0.0")} AS yes_best_ask,
            {c("yes_best_bid",    "0.0")} AS yes_best_bid,
            {c("yes_spread",      "0.0")} AS yes_spread,
            {c("liquidity_num",   "0.0")} AS liquidity_num,
            {c("volume_24h",      "0.0")} AS volume_24h,
            {c("observed_max_f",  "NULL")} AS observed_max_f,
            {c("hours_remaining", "0.0")} AS hours_remaining,
            b.event_id, b.title AS bucket_title, b.lo, b.hi,
            m.city_slug, m.event_date,
            f.mu_f, f.sigma_f, f.lead_hours,
            o.actual_high_f, o.winning_bucket_id,
            (b.id = o.winning_bucket_id) AS hit
        FROM snapshots s
        JOIN buckets b ON b.id = s.bucket_id
        JOIN markets m ON m.event_id = b.event_id
        JOIN forecasts f ON f.id = s.forecast_id
        JOIN outcomes o ON o.event_id = m.event_id
        WHERE o.winning_bucket_id IS NOT NULL
        ORDER BY s.ts
    """
    out = []
    for r in db.execute(sql):
        out.append(Snap(
            ts=r["ts"], bucket_id=r["bucket_id"], forecast_id=r["forecast_id"],
            p_model=r["p_model"], yes_price=r["yes_price"], no_price=r["no_price"],
            edge_yes=r["edge_yes"],
            yes_best_ask=r["yes_best_ask"] or 0.0,
            yes_best_bid=r["yes_best_bid"] or 0.0,
            yes_spread=r["yes_spread"] or 0.0,
            liquidity_num=r["liquidity_num"] or 0.0,
            volume_24h=r["volume_24h"] or 0.0,
            observed_max_f=r["observed_max_f"],
            hours_remaining=r["hours_remaining"] or 0.0,
            event_id=r["event_id"], bucket_title=r["bucket_title"],
            lo=r["lo"], hi=r["hi"],
            city_slug=r["city_slug"], event_date=r["event_date"],
            mu_f=r["mu_f"], sigma_f=r["sigma_f"], lead_hours=r["lead_hours"],
            actual_high_f=r["actual_high_f"], winning_bucket_id=r["winning_bucket_id"],
            hit=bool(r["hit"]),
        ))
    return out


# ---------- trade model ----------

@dataclass
class Trade:
    side: str          # "YES" | "NO"
    paid: float        # цена входа (cost per $1 notional)
    win: bool
    snap: Snap

    @property
    def pnl(self) -> float:
        return (1.0 - self.paid) if self.win else -self.paid


def take_first_signal(
    snaps: list[Snap], *, side: str, min_edge: float,
    filter_fn=None,
) -> list[Trade]:
    """По одному входу на bucket_id — на первом снимке, где условие выполнено."""
    trades: list[Trade] = []
    seen: set[int] = set()
    for s in snaps:  # уже отсортированы по ts в load_snaps
        if s.bucket_id in seen:
            continue
        if filter_fn and not filter_fn(s):
            continue
        if side == "YES":
            if s.effective_edge_yes < min_edge:
                continue
            paid = s.buy_yes_price
            win = s.hit
        else:
            if s.effective_edge_no < min_edge:
                continue
            paid = s.buy_no_price
            win = not s.hit
        if paid <= 0 or paid >= 1:
            continue
        seen.add(s.bucket_id)
        trades.append(Trade(side, paid, win, s))
    return trades


def summarize(trades: list[Trade]) -> dict:
    if not trades:
        return {"n": 0}
    pnls = [t.pnl for t in trades]
    wins = sum(1 for t in trades if t.win)
    return {
        "n": len(trades),
        "winrate": wins / len(trades),
        "avg_paid": mean(t.paid for t in trades),
        "avg_pnl": mean(pnls),
        "total_pnl": sum(pnls),
        "sharpe": sharpe(pnls),
        "max_drawdown_trade": min(pnls),
    }


# ---------- sections ----------

def section_coverage(snaps: list[Snap]) -> None:
    evs = {(s.city_slug, s.event_date) for s in snaps}
    with_ask = sum(1 for s in snaps if s.yes_best_ask > 0)
    with_obs = sum(1 for s in snaps if s.observed_max_f is not None)
    print("=== Coverage ===")
    print(f"  resolved events: {len(evs)}")
    print(f"  snapshots on resolved: {len(snaps)}")
    print(f"  snapshots with bestAsk: {with_ask}  ({with_ask/max(len(snaps),1)*100:.0f}%)")
    print(f"  snapshots with observed_max: {with_obs}  ({with_obs/max(len(snaps),1)*100:.0f}%)")


def section_forecast(snaps: list[Snap]) -> None:
    seen: dict[int, Snap] = {}
    for s in snaps:
        seen.setdefault(s.forecast_id, s)
    errs = [(s.city_slug, s.lead_hours, s.mu_f - s.actual_high_f) for s in seen.values()]
    all_err = [e for (_, _, e) in errs]
    print("\n=== 1. Forecast error (mu_f − actual) ===")
    print(f"  n_forecasts={len(errs)}  bias={mean(all_err):+.2f}  RMSE={rms(all_err):.2f}  MAE={mean(abs(e) for e in all_err):.2f}")

    by_city: dict[str, list[float]] = defaultdict(list)
    for (city, _, e) in errs:
        by_city[city].append(e)
    print("  per-city:")
    print(f"    {'city':>14}  n  bias   rmse")
    for city in sorted(by_city):
        es = by_city[city]
        print(f"    {city:>14}  {len(es):>3}  {mean(es):>+5.2f}  {rms(es):>5.2f}")

    lead_bins = [(-9999,0,"<0h (intraday)"),(0,6,"0-6h"),(6,12,"6-12h"),(12,24,"12-24h"),(24,48,"24-48h"),(48,72,"48-72h"),(72,9999,">72h")]
    by_lead: dict[str, list[float]] = defaultdict(list)
    for (_, lead, e) in errs:
        for (lo, hi, lab) in lead_bins:
            if lo <= lead <= hi:
                by_lead[lab].append(e)
                break
    print("  per-lead:")
    for (_, _, lab) in lead_bins:
        if lab in by_lead:
            es = by_lead[lab]
            print(f"    {lab:>14}  n={len(es):>4}  bias={mean(es):>+5.2f}  rmse={rms(es):>5.2f}")


def section_calibration(snaps: list[Snap]) -> None:
    bins = [(0,0.05),(0.05,0.15),(0.15,0.30),(0.30,0.50),(0.50,0.70),(0.70,0.85),(0.85,0.95),(0.95,1.001)]

    def show(label: str, pvals: list[tuple[float, bool]]) -> None:
        print(f"\n  {label}:")
        print(f"    {'bin':>12}  n    emp_hit  mid_p  delta")
        for (lo, hi) in bins:
            sub = [hit for (p, hit) in pvals if lo <= p < hi]
            if not sub:
                continue
            hr = sum(sub) / len(sub)
            mid = (lo + hi) / 2
            print(f"    {lo:>4.2f}-{hi:<5.2f}  {len(sub):>5}  {hr:>6.3f}  {mid:>5.2f}  {hr-mid:+.3f}")

    print("\n=== 2. Calibration ===")
    show("p_model", [(s.p_model, s.hit) for s in snaps])
    # Рынок: используем mid (yes_price) и bestAsk отдельно — хотим видеть, насколько они отличаются
    show("yes_price (mid/last из Gamma)", [(s.yes_price, s.hit) for s in snaps])
    sub_ask = [(s.yes_best_ask, s.hit) for s in snaps if s.yes_best_ask > 0]
    if sub_ask:
        show("yes_best_ask (исполняемая YES)", sub_ask)


def section_spread_liquidity(snaps: list[Snap]) -> None:
    asks = [s for s in snaps if s.yes_best_ask > 0]
    if not asks:
        print("\n=== 3. Spread & liquidity ===  (нет данных — migration не пройдёна)")
        return
    print("\n=== 3. Spread & liquidity profile ===")
    spreads = [s.yes_spread for s in asks]
    liqs = [s.liquidity_num for s in asks]
    print(f"  n_with_ask={len(asks)}")
    print(f"  spread: median={sorted(spreads)[len(spreads)//2]:.4f}  "
          f"p90={sorted(spreads)[int(0.9*len(spreads))]:.4f}  max={max(spreads):.4f}")
    print(f"  liq:    median=${sorted(liqs)[len(liqs)//2]:.0f}  "
          f"p90=${sorted(liqs)[int(0.9*len(liqs))]:.0f}")

    # Bid-ask vs midprice: честный edge vs «mid-edge»
    mid_edge = [s.edge_yes for s in asks]
    eff_edge = [s.effective_edge_yes for s in asks]
    print(f"  avg mid-edge: {mean(mid_edge):+.4f}   avg ask-edge: {mean(eff_edge):+.4f}   "
          f"slippage: {mean(mid_edge)-mean(eff_edge):+.4f}")


def print_summary_row(label: str, summary: dict) -> None:
    if summary["n"] == 0:
        return
    print(f"  {label:>32}  n={summary['n']:>4}  wr={summary['winrate']:.3f}  "
          f"paid={summary['avg_paid']:.3f}  pnl/$={summary['avg_pnl']:+.4f}  "
          f"total=${summary['total_pnl']:+.2f}  sharpe={summary['sharpe']:+.2f}")


def section_backtest(snaps: list[Snap]) -> None:
    print("\n=== 4. Backtest first-signal per bucket (fill по best_ask) ===")
    print("  (edge уже посчитан от best_ask, не от mid-price)")
    for thresh in [0.03, 0.05, 0.08, 0.10, 0.12, 0.15, 0.20]:
        for side in ("YES", "NO"):
            trades = take_first_signal(snaps, side=side, min_edge=thresh)
            s = summarize(trades)
            print_summary_row(f"|edge|≥{thresh:.2f} {side}", s)


def section_intraday(snaps: list[Snap]) -> None:
    print("\n=== 5. Intraday edge (observed_max уже отрезал бакет) ===")
    cutters = [s for s in snaps if s.observed_cuts_bucket() is True]
    if not cutters:
        print("  нет снимков, где observed_max > bucket.hi (данных ещё недостаточно или день не начинался)")
        return
    # Если бакет отрезан фактом (observed_max > hi) — он 100% проигран, поэтому:
    #  - YES никогда не должен стоить > 0 (если рынок видит): продаём/шортим
    #  - Эквивалент: buy NO по ~любой цене < 1
    yes_prices = [s.buy_yes_price for s in cutters]
    print(f"  n_snapshots где бакет уже отрезан: {len(cutters)}")
    print(f"  YES-цена на таких снимках: avg={mean(yes_prices):.4f}  max={max(yes_prices):.4f}")
    # Сколько из них торговались по >=1¢ YES — это прямой free-money NO-buy
    free_money_snaps = [s for s in cutters if s.buy_yes_price >= 0.01]
    print(f"  снимков где YES ≥ $0.01 (есть edge на buy-NO): {len(free_money_snaps)}")

    # First-signal buy NO на отрезанных бакетах
    def cut_filter(s: Snap) -> bool:
        return s.observed_cuts_bucket() is True
    trades = take_first_signal(snaps, side="NO", min_edge=0.0, filter_fn=cut_filter)
    # для «отрезанных» edge не информативен (p_model=0), используем только фильтр
    # Поэтому возьмём вручную: first snap per bucket где cut.
    trades = []
    seen: set[int] = set()
    for s in snaps:
        if s.bucket_id in seen:
            continue
        if s.observed_cuts_bucket() is not True:
            continue
        paid = s.buy_no_price
        if paid <= 0 or paid >= 1:
            continue
        seen.add(s.bucket_id)
        trades.append(Trade("NO", paid, not s.hit, s))
    print_summary_row("buy-NO на отрезанных", summarize(trades))


def section_strategy_grid(snaps: list[Snap]) -> None:
    """Grid-search по (side, min_edge, side_filter_spread, side_filter_liq, tail/middle, lead-max)."""
    print("\n=== 6. Strategy grid-search ===")
    print("  ищем стратегии с (n≥15, winrate-adjusted PnL > 0)")
    have_ask = any(s.yes_best_ask > 0 for s in snaps)

    results = []
    for side in ("YES", "NO"):
        for min_edge in [0.05, 0.08, 0.10, 0.12, 0.15, 0.20]:
            for max_spread in [0.10, 0.05, 0.02]:  # <= значит «spread не хуже чем»
                for min_liq in [0.0, 50.0, 200.0]:
                    for bucket_kind in ("any", "middle", "tail"):
                        for max_lead in [999.0, 24.0, 12.0, 6.0]:
                            def flt(s: Snap, max_spread=max_spread, min_liq=min_liq,
                                    bucket_kind=bucket_kind, max_lead=max_lead) -> bool:
                                if have_ask and s.yes_spread > max_spread:
                                    return False
                                if have_ask and s.liquidity_num < min_liq:
                                    return False
                                if bucket_kind == "middle" and s.is_tail:
                                    return False
                                if bucket_kind == "tail" and not s.is_tail:
                                    return False
                                if s.lead_hours > max_lead:
                                    return False
                                return True
                            trades = take_first_signal(snaps, side=side, min_edge=min_edge, filter_fn=flt)
                            if len(trades) < 15:
                                continue
                            s = summarize(trades)
                            results.append((s["avg_pnl"] * (s["n"] ** 0.5), side, min_edge,
                                            max_spread, min_liq, bucket_kind, max_lead, s))

    if not results:
        print("  нет стратегий с n≥15 при текущих данных (мало резолвов)")
        return
    results.sort(reverse=True)
    print(f"\n  top-10 по avg_pnl × √n:")
    print(f"    {'side':>4} {'edge':>5} {'spread':>7} {'liq':>5} {'bucket':>7} {'lead':>5}  "
          f"n   wr     avg_pnl  total   sharpe")
    for (_, side, min_edge, max_spread, min_liq, bk, max_lead, s) in results[:10]:
        print(f"    {side:>4} {min_edge:>5.2f} {max_spread:>7.3f} {min_liq:>4.0f} {bk:>7} "
              f"{max_lead:>5.0f}  {s['n']:>3} {s['winrate']:>5.3f} {s['avg_pnl']:>+7.4f} "
              f"${s['total_pnl']:>+6.2f}  {s['sharpe']:>+5.2f}")


def section_per_city(snaps: list[Snap]) -> None:
    print("\n=== 7. Per-city breakdown (|edge|≥0.10 YES first-signal, fill по bestAsk) ===")
    trades = take_first_signal(snaps, side="YES", min_edge=0.10)
    by_city: dict[str, list[Trade]] = defaultdict(list)
    for t in trades:
        by_city[t.snap.city_slug].append(t)
    print(f"    {'city':>14}  n  wr     avg_pnl  total")
    for city in sorted(by_city):
        ts = by_city[city]
        s = summarize(ts)
        print(f"    {city:>14}  {s['n']:>2}  {s['winrate']:.2f}  {s['avg_pnl']:+.4f}  ${s['total_pnl']:+.2f}")


def main() -> int:
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    snaps = load_snaps(db)
    if not snaps:
        print("no resolved outcomes — run scripts/resolve_weather.py first")
        return 1

    section_coverage(snaps)
    section_forecast(snaps)
    section_calibration(snaps)
    section_spread_liquidity(snaps)
    section_backtest(snaps)
    section_intraday(snaps)
    section_per_city(snaps)
    section_strategy_grid(snaps)

    db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
