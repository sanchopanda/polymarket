"""
research_bot/analyze_correlate.py

Анализирует CSV из pm_correlate.py и выводит готовую сводку.
Запуск: python3 -m research_bot analyze [путь_к_csv]
        python3 -m research_bot analyze  (берёт последний файл автоматически)
"""
from __future__ import annotations

import calendar
import csv
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median, mean

import httpx
import json

_PM_HEADERS = {"User-Agent": "Mozilla/5.0"}
_PM_TIMEOUT = 10
_CACHE_PATH = Path("research_bot/data/ref_price_cache.json")


def _fetch_market_prices(slug: str) -> tuple[float | None, float | None]:
    """
    Парсит openPrice и closePrice с HTML Polymarket.
    Возвращает (open_price, close_price).
    close_price = None если рынок ещё не закрыт.
    """
    url = f"https://polymarket.com/event/{slug}"
    try:
        resp = httpx.get(url, timeout=_PM_TIMEOUT, follow_redirects=True, headers=_PM_HEADERS)
        m_open  = re.search(r'"openPrice"\s*:\s*([0-9.]+)', resp.text)
        m_close = re.search(r'"closePrice"\s*:\s*([0-9.]+)', resp.text)
        open_p  = float(m_open.group(1))  if m_open  else None
        close_p = float(m_close.group(1)) if m_close else None
        return open_p, close_p
    except Exception as exc:
        print(f"[enrich] {slug}: {exc}")
    return None, None


def _date_from_path(path: Path) -> str | None:
    """Извлекает дату YYYYMMDD из имени файла correlate_YYYYMMDD_HHMMSS.csv"""
    m = re.search(r"(\d{8})_", path.stem)
    return m.group(1) if m else None


def _market_start_dt(ts_str: str, date_str: str, minutes_remaining: float,
                     prev_hour: list) -> datetime | None:
    """
    Вычисляет market_start из ts_utc + minutes_remaining.
    market_end = ts + minutes_remaining округлённый до 5 мин,
    market_start = market_end - 5 мин.
    prev_hour — однэлементный список [int] для отслеживания перехода через полночь.
    """
    try:
        # Поддержка двух форматов ts_utc:
        #   старый: "HH:MM:SS.fff"  (дата берётся из date_str)
        #   новый:  "YYYY-MM-DD HH:MM:SS.fff"
        if " " in ts_str and "-" in ts_str[:10]:
            # Новый формат: полный ISO datetime
            date_part, time_part = ts_str.split(" ", 1)
            date_str = date_part.replace("-", "")
            h, mn, s = time_part.split(":")
        else:
            h, mn, s = ts_str.split(":")
        h_int = int(h)
        # Обнаруживаем переход через полночь: час уменьшился (например, 23 → 0)
        if prev_hour[0] is not None and h_int < prev_hour[0] and prev_hour[0] >= 22:
            date_str = (datetime(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]),
                                 tzinfo=timezone.utc) + timedelta(days=1)).strftime("%Y%m%d")
        prev_hour[0] = h_int
        base = datetime(
            int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]),
            h_int, int(mn), int(float(s)), tzinfo=timezone.utc,
        )
        end_approx = base + timedelta(minutes=minutes_remaining)
        total_min = end_approx.hour * 60 + end_approx.minute
        rounded = round(total_min / 5) * 5
        end = end_approx.replace(
            hour=(rounded // 60) % 24,
            minute=rounded % 60,
            second=0, microsecond=0,
        )
        # Если округление перешло через полночь
        if rounded >= 1440:
            end = (end + timedelta(days=1)).replace(hour=0, minute=0)
        return end - timedelta(minutes=5)
    except Exception:
        return None


def enrich_with_ref_price(rows: list[dict], path: Path) -> None:
    """
    Добавляет поля cl_ref_price и cl_vs_ref в каждую строку (in-place).
    Использует fetch_pm_open_price по slug вида btc-updown-5m-{unix_ts}.
    """
    date_str = _date_from_path(path)
    # Для correlate_all.csv и подобных — дату берём из ts_utc каждой строки
    # (новый формат: "YYYY-MM-DD HH:MM:SS.fff")
    if not date_str and rows:
        ts0 = rows[0].get("ts_utc", "")
        if len(ts0) >= 10 and "-" in ts0[:10]:
            date_str = ts0[:10].replace("-", "")  # берём из первой строки как fallback
    if not date_str:
        return

    # Кэш: "sym:unix_ts" → {"o": open_price, "c": close_price}  (персистентный JSON)
    raw_cache: dict[str, dict] = {}
    if _CACHE_PATH.exists():
        try:
            loaded = json.loads(_CACHE_PATH.read_text())
            # Миграция старого формата (plain float → dict)
            for k, v in loaded.items():
                raw_cache[k] = v if isinstance(v, dict) else {"o": v, "c": None}
        except Exception:
            pass
    cache: dict[tuple, dict] = {
        (k.split(":")[0], int(k.split(":")[1])): v
        for k, v in raw_cache.items()
    }

    unique_keys: set[tuple] = set()
    row_keys: list[tuple | None] = []
    prev_hour: list = [None]

    for row in rows:
        sym = row.get("symbol", "")
        ts  = row.get("ts_utc", "")
        mr_f = None
        try:
            mr_f = float(row.get("minutes_remaining", ""))
        except (ValueError, TypeError):
            pass

        if not sym or not ts or mr_f is None:
            row_keys.append(None)
            continue

        start_dt = _market_start_dt(ts, date_str, mr_f, prev_hour)
        if start_dt is None:
            row_keys.append(None)
            continue

        unix_ts = calendar.timegm(start_dt.timetuple())
        key = (sym.lower(), unix_ts)
        row_keys.append(key)
        unique_keys.add(key)

    # Нужно перезапросить те у кого нет closePrice (старый кэш)
    missing = [k for k in unique_keys
               if k not in cache or cache[k].get("c") is None and cache[k].get("o") is not None]
    # Также те кого нет вообще
    missing += [k for k in unique_keys if k not in cache]
    missing = list(set(missing))

    total_keys = len(missing)
    if total_keys:
        print(f"[enrich] Новых/обновляемых рынков: {total_keys} (кэш: {len(unique_keys)-total_keys})", flush=True)
    else:
        print(f"[enrich] Все {len(unique_keys)} рынков из кэша", flush=True)

    for i, (sym_lower, unix_ts) in enumerate(sorted(missing), 1):
        slug = f"{sym_lower}-updown-5m-{unix_ts}"
        open_p, close_p = _fetch_market_prices(slug)
        cache[(sym_lower, unix_ts)] = {"o": open_p, "c": close_p}
        status = f"open={open_p:.4f} close={close_p:.4f}" if open_p and close_p else (
                 f"open={open_p:.4f} close=—" if open_p else "—")
        print(f"  [{i}/{total_keys}] {slug} → {status}", flush=True)

    # Сохраняем в кэш-файл
    if missing:
        raw_cache.update({f"{s}:{t}": v for (s, t), v in cache.items()})
        _CACHE_PATH.write_text(json.dumps(raw_cache))

    for row, key in zip(rows, row_keys):
        if key is None:
            continue
        entry = cache.get(key)
        if not entry or entry.get("o") is None:
            continue
        ref = entry["o"]
        close_p = entry.get("c")

        cl = None
        try:
            cl = float(row.get("cl_price", ""))
        except (ValueError, TypeError):
            pass

        row["cl_ref_price"] = ref
        row["cl_vs_ref"] = "above" if cl and cl > ref else ("below" if cl and cl < ref else "at")
        # Исход рынка по closePrice: YES если close > open, NO если close < open
        if close_p is not None:
            row["market_outcome"] = "yes" if close_p > ref else ("no" if close_p < ref else "flat")


def load(path: Path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def f(val: str) -> float | None:
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def analyze(rows: list[dict]) -> None:
    total = len(rows)
    print(f"\n{'='*60}")
    print(f"Файл: {len(rows)} событий")
    print(f"{'='*60}")

    # Фильтр: только строки с известным CL направлением и PM ценой
    directional = [r for r in rows if r.get("cl_direction") in ("up", "down")
                   and f(r.get("pm_ask_before")) is not None]
    print(f"С известным CL направлением: {len(directional)}/{total}")

    # ── Совпадение направлений ────────────────────────────────────────────────
    print(f"\n── Совпадение направления CL→PM (по pm_end, t30) ──")
    matched    = [r for r in directional if r.get("direction_match") == "Y"]
    mismatched = [r for r in directional if r.get("direction_match") == "N"]
    neutral    = [r for r in directional if r.get("direction_match") == "—"]
    comparable = matched + mismatched
    if comparable:
        pct = len(matched) / len(comparable) * 100
        print(f"  Y={len(matched)}  N={len(mismatched)}  —={len(neutral)}  "
              f"→ {pct:.1f}% совпадений (из {len(comparable)} измеримых)")

    # Альтернатива: направление по pm_t3 (фильтруем спайки ≥0.98 и ≤0.02)
    print(f"\n── Совпадение направления CL→PM (по pm_t3, без спайков) ──")
    early_match = early_mismatch = early_skip = 0
    for r in directional:
        cl_dir = r.get("cl_direction")
        ask0   = f(r.get("pm_ask_before"))
        t3     = f(r.get("pm_t3"))
        if not ask0 or not t3 or t3 >= 0.98 or t3 <= 0.02:
            early_skip += 1
            continue
        pm_moved_up = t3 > ask0 + 0.005
        pm_moved_dn = t3 < ask0 - 0.005
        if not pm_moved_up and not pm_moved_dn:
            early_skip += 1
            continue
        pm_dir = "up" if pm_moved_up else "down"
        if cl_dir == pm_dir:
            early_match += 1
        else:
            early_mismatch += 1
    early_total = early_match + early_mismatch
    if early_total:
        print(f"  Y={early_match}  N={early_mismatch}  —={early_skip}  "
              f"→ {early_match/early_total*100:.1f}% совпадений (из {early_total} измеримых)")

    # По порогу CL движения (все рынки)
    print(f"\n── По порогу |cl_delta_pct| (все рынки) ──")
    for thr in (0.005, 0.010, 0.030, 0.050, 0.100):
        bucket = [r for r in comparable
                  if f(r.get("cl_delta_pct")) is not None
                  and abs(f(r["cl_delta_pct"])) >= thr]
        if not bucket:
            continue
        bm = [r for r in bucket if r.get("direction_match") == "Y"]
        print(f"  ≥{thr:.3f}%: {len(bm)}/{len(bucket)} = {len(bm)/len(bucket)*100:.1f}%")

    # По порогу CL движения (только активная зона pm 0.15–0.85)
    print(f"\n── По порогу |cl_delta_pct| (активная зона pm 0.15–0.85) ──")
    active_comparable = [r for r in comparable
                         if f(r.get("pm_ask_before")) is not None
                         and 0.15 <= f(r["pm_ask_before"]) <= 0.85]
    for thr in (0.005, 0.010, 0.030, 0.050, 0.100):
        bucket = [r for r in active_comparable
                  if f(r.get("cl_delta_pct")) is not None
                  and abs(f(r["cl_delta_pct"])) >= thr]
        if not bucket:
            continue
        bm = [r for r in bucket if r.get("direction_match") == "Y"]
        mf = [f(r["pm_max_fav_cents"]) for r in bm
              if f(r.get("pm_max_fav_cents")) is not None]
        mf_str = f"  max_fav avg={mean(mf):.1f}c median={median(mf):.1f}c" if mf else ""
        print(f"  ≥{thr:.3f}%: match={len(bm)}/{len(bucket)}={len(bm)/len(bucket)*100:.1f}%{mf_str}")

    # ── Зона входа (pm_before) ────────────────────────────────────────────────
    print(f"\n── По зоне входа pm_ask_before ──")
    zones = [
        ("extreme (<0.15 или >0.85)", lambda v: v < 0.15 or v > 0.85),
        ("0.15–0.35",                 lambda v: 0.15 <= v <= 0.35),
        ("0.35–0.65  (центр)",        lambda v: 0.35 < v < 0.65),
        ("0.65–0.85",                 lambda v: 0.65 <= v <= 0.85),
    ]
    for name, pred in zones:
        bucket = [r for r in comparable
                  if f(r.get("pm_ask_before")) is not None
                  and pred(f(r["pm_ask_before"]))]
        if not bucket:
            continue
        bm = [r for r in bucket if r.get("direction_match") == "Y"]
        mf_vals = [f(r["pm_max_fav_cents"]) for r in bucket
                   if f(r.get("pm_max_fav_cents")) is not None]
        mf_str = f"  max_fav avg={mean(mf_vals):.1f}c median={median(mf_vals):.1f}c" if mf_vals else ""
        print(f"  {name}: match={len(bm)}/{len(bucket)}={len(bm)/len(bucket)*100:.0f}%{mf_str}")

    # ── Lag (скорость реакции PM) ─────────────────────────────────────────────
    print(f"\n── Лаг первой PM реакции ──")
    lags = [f(r["first_lag_ms"]) for r in directional
            if f(r.get("first_lag_ms")) is not None]
    if lags:
        print(f"  n={len(lags)}  avg={mean(lags):.0f}ms  median={median(lags):.0f}ms  "
              f"min={min(lags):.0f}ms  max={max(lags):.0f}ms")
        for thr in (100, 500, 1000, 3000):
            n = sum(1 for l in lags if l < thr)
            print(f"  < {thr}ms: {n}/{len(lags)} = {n/len(lags)*100:.0f}%")

    # ── Слиппаж t0 → t1 ───────────────────────────────────────────────────────
    print(f"\n── Слиппаж: ask_t0 → entry_t1 (реальная цена входа) ──")
    slippage = []
    for r in directional:
        ask0 = f(r.get("pm_ask_before"))
        t1   = f(r.get("pm_entry_t1"))
        if ask0 and t1:
            cl_dir = r.get("cl_direction")
            # Для CL UP: хотим купить YES, слиппаж = насколько выросла цена за 1s
            # Для CL DOWN: хотим купить NO (= low YES), слиппаж = насколько упала YES цена
            slip = (t1 - ask0) * 100 if cl_dir == "up" else (ask0 - t1) * 100
            slippage.append(slip)
    if slippage:
        pos = [s for s in slippage if s > 0]
        neg = [s for s in slippage if s < 0]
        print(f"  n={len(slippage)}  avg={mean(slippage):+.1f}c  median={median(slippage):+.1f}c")
        print(f"  Цена ухудшилась за 1s: {len(pos)}/{len(slippage)} ({len(pos)/len(slippage)*100:.0f}%)")
        print(f"  Цена улучшилась за 1s: {len(neg)}/{len(slippage)} ({len(neg)/len(slippage)*100:.0f}%)")
        if pos:
            print(f"  Среднее ухудшение: {mean(pos):.1f}c  max: {max(pos):.1f}c")

    # ── Max favorable (потенциальный profit) ──────────────────────────────────
    print(f"\n── Потенциальный profit (max_fav от t+1s) ──")
    mf_all = [f(r["pm_max_fav_cents"]) for r in matched
              if f(r.get("pm_max_fav_cents")) is not None]
    if mf_all:
        print(f"  n={len(mf_all)}  avg={mean(mf_all):.1f}c  median={median(mf_all):.1f}c  "
              f"max={max(mf_all):.1f}c")
        for thr in (3, 5, 10, 20):
            n = sum(1 for v in mf_all if v >= thr)
            print(f"  ≥{thr}c: {n}/{len(mf_all)} = {n/len(mf_all)*100:.0f}%")

    # ── Спред bid-ask ──────────────────────────────────────────────────────────
    print(f"\n── Спред bid-ask в момент тика ──")
    spreads = [f(r["spread_before"]) for r in directional
               if f(r.get("spread_before")) is not None and f(r["spread_before"]) > 0]
    if spreads:
        print(f"  n={len(spreads)}  avg={mean(spreads):.1f}c  median={median(spreads):.1f}c  "
              f"min={min(spreads):.1f}c  max={max(spreads):.1f}c")

    # ── По символам ───────────────────────────────────────────────────────────
    print(f"\n── По символам ──")
    for sym in sorted(set(r["symbol"] for r in rows)):
        se = [r for r in comparable if r["symbol"] == sym]
        if not se:
            continue
        sm = [r for r in se if r.get("direction_match") == "Y"]
        mf = [f(r["pm_max_fav_cents"]) for r in sm if f(r.get("pm_max_fav_cents")) is not None]
        mf_str = f"  max_fav avg={mean(mf):.1f}c" if mf else ""
        print(f"  {sym}: match={len(sm)}/{len(se)}={len(sm)/len(se)*100:.0f}%{mf_str}")

    # ── По минутам до закрытия рынка ──────────────────────────────────────────
    print(f"\n── По minutes_remaining (активность рынка) ──")
    mr_buckets = [
        (">4m  (начало)",   lambda v: v > 4),
        ("2–4m (середина)", lambda v: 2 < v <= 4),
        ("1–2m",            lambda v: 1 < v <= 2),
        ("<1m  (конец)",    lambda v: v <= 1),
    ]
    for name, pred in mr_buckets:
        bucket = [r for r in comparable
                  if f(r.get("minutes_remaining")) is not None
                  and pred(f(r["minutes_remaining"]))]
        if not bucket:
            continue
        bm = [r for r in bucket if r.get("direction_match") == "Y"]
        ask_vals = [f(r["pm_ask_before"]) for r in bucket if f(r.get("pm_ask_before"))]
        active = [v for v in ask_vals if 0.15 <= v <= 0.85]
        mf = [f(r["pm_max_fav_cents"]) for r in bm if f(r.get("pm_max_fav_cents")) is not None]
        mf_str = f"  max_fav avg={mean(mf):.1f}c" if mf else ""
        print(f"  {name}: match={len(bm)}/{len(bucket)}={len(bm)/len(bucket)*100:.0f}%"
              f"  активных(pm 0.15-0.85)={len(active)}/{len(bucket)}{mf_str}")

    # ── Только активные рынки (pm 0.15–0.85) ──────────────────────────────────
    print(f"\n── АКТИВНАЯ ЗОНА pm 0.15–0.85 (фильтр по ask_before) ──")
    active_rows = [r for r in comparable
                   if f(r.get("pm_ask_before")) is not None
                   and 0.15 <= f(r["pm_ask_before"]) <= 0.85]
    if active_rows:
        am = [r for r in active_rows if r.get("direction_match") == "Y"]
        pct = len(am) / len(active_rows) * 100
        mf = [f(r["pm_max_fav_cents"]) for r in am if f(r.get("pm_max_fav_cents")) is not None]
        spr = [f(r["spread_before"]) for r in active_rows
               if f(r.get("spread_before")) is not None and f(r["spread_before"]) < 20]
        print(f"  Событий: {len(active_rows)}  match={len(am)}/{len(active_rows)}={pct:.1f}%")
        if mf:
            print(f"  max_fav: avg={mean(mf):.1f}c  median={median(mf):.1f}c  max={max(mf):.1f}c")
            for thr in (3, 5, 10, 20):
                n = sum(1 for v in mf if v >= thr)
                print(f"    ≥{thr}c: {n}/{len(mf)} = {n/len(mf)*100:.0f}%")
        if spr:
            print(f"  Реальный спред (без 98c): avg={mean(spr):.1f}c  median={median(spr):.1f}c")
    else:
        print("  Нет данных")

    # ── Траектория цены по временным срезам (активная зона) ──────────────────
    # Показывает: растёт ли цена РАВНОМЕРНО по срезам или сразу прыгает?
    # Нормализуем: 0 = цена в момент тика, положительное = движение в сторону CL
    print(f"\n── Траектория PM цены после тика (активная зона pm 0.15–0.85) ──")
    sample_cols = ["pm_t1", "pm_t3", "pm_t6", "pm_t9", "pm_t12",
                   "pm_t15", "pm_t18", "pm_t21", "pm_t24", "pm_t27", "pm_t30"]
    for cl_dir, label in [("up", "CL вверх (UP)"), ("down", "CL вниз (DOWN)")]:
        subset = [r for r in directional
                  if r.get("cl_direction") == cl_dir
                  and f(r.get("pm_ask_before")) is not None
                  and 0.15 <= f(r["pm_ask_before"]) <= 0.85]
        if not subset:
            continue
        print(f"\n  {label} — {len(subset)} событий:")
        print(f"  {'время':>6}  {'avg движение':>14}  {'% событий с данными':>20}")
        for col in sample_cols:
            deltas = []
            for r in subset:
                ask0 = f(r.get("pm_ask_before"))
                val  = f(r.get(col))
                if ask0 is None or val is None or val >= 0.98 or val <= 0.02:
                    continue
                # Движение в сторону CL: для up = val - ask0, для down = ask0 - val
                delta = (val - ask0) * 100 if cl_dir == "up" else (ask0 - val) * 100
                deltas.append(delta)
            if deltas:
                t = col.replace("pm_t", "t+") + "s"
                pct_data = len(deltas) / len(subset) * 100
                print(f"  {t:>6}  {mean(deltas):>+13.1f}c  {pct_data:>19.0f}%")

    # ── Окно (насколько часто следующий тик обрезает) ─────────────────────────
    print(f"\n── Длина окна ──")
    windows = [f(r["window_s"]) for r in directional if f(r.get("window_s")) is not None]
    if windows:
        full = sum(1 for w in windows if w >= 29.9)
        cut  = sum(1 for w in windows if w < 29.9)
        print(f"  avg={mean(windows):.1f}s  min={min(windows):.1f}s")
        print(f"  Полные (30s): {full}/{len(windows)} | Обрезанные следующим тиком: {cut}/{len(windows)}")

    print(f"\n{'='*60}\n")


def analyze_ref_price(rows: list[dict]) -> None:
    """Анализ по позиции CL относительно референс-цены рынка."""
    enriched = [r for r in rows if r.get("cl_ref_price") is not None]
    if not enriched:
        print("\n── CL vs референс-цена: нет данных (БД не найдена или нет совпадений) ──\n")
        return

    total = len(enriched)
    above = [r for r in enriched if r.get("cl_vs_ref") == "above"]
    below = [r for r in enriched if r.get("cl_vs_ref") == "below"]
    matched_pct = sum(1 for r in enriched
                      if r.get("cl_vs_ref") == "above" and r.get("pm_ask_before") is not None
                      and f(r["pm_ask_before"]) > 0.5) + \
                  sum(1 for r in enriched
                      if r.get("cl_vs_ref") == "below" and r.get("pm_ask_before") is not None
                      and f(r["pm_ask_before"]) <= 0.5)

    print(f"\n{'='*60}")
    print(f"── CL vs референс-цена рынка ──")
    print(f"{'='*60}")
    print(f"  Событий с референс-ценой из БД: {total}/{len(rows)}")
    print(f"  CL выше референса: {len(above)} ({len(above)/total*100:.0f}%)")
    print(f"  CL ниже референса: {len(below)} ({len(below)/total*100:.0f}%)")
    print(f"  PM согласен с CL позицией (ask>0.5 ↔ CL above): {matched_pct}/{total} = {matched_pct/total*100:.0f}%")

    # Только активная зона
    active = [r for r in enriched
              if f(r.get("pm_ask_before")) is not None
              and 0.15 <= f(r["pm_ask_before"]) <= 0.85]
    print(f"\n── По позиции CL (активная зона pm 0.15–0.85) ──")
    print(f"  Активных событий: {len(active)}")

    for vs_ref, label in [("above", "CL выше референса → YES должен победить"),
                           ("below", "CL ниже референса → NO должен победить")]:
        bucket = [r for r in active if r.get("cl_vs_ref") == vs_ref]
        if not bucket:
            continue
        comparable = [r for r in bucket if r.get("direction_match") in ("Y", "N")]
        # Для "above": тик вверх = укрепляет YES = правильно, тик вниз = неправильно
        # Для "below": тик вниз = укрепляет NO = правильно, тик вверх = неправильно
        # direction_match уже считает tick-to-tick — покажем просто как сигнал
        tick_consistent = [r for r in bucket if
                           (vs_ref == "above" and r.get("cl_direction") == "up") or
                           (vs_ref == "below" and r.get("cl_direction") == "down")]
        tick_contra = [r for r in bucket if
                       (vs_ref == "above" and r.get("cl_direction") == "down") or
                       (vs_ref == "below" and r.get("cl_direction") == "up")]
        mf_consist = [f(r["pm_max_fav_cents"]) for r in tick_consistent
                      if r.get("direction_match") == "Y" and f(r.get("pm_max_fav_cents")) is not None]
        mf_contra  = [f(r["pm_max_fav_cents"]) for r in tick_contra
                      if r.get("direction_match") == "Y" and f(r.get("pm_max_fav_cents")) is not None]
        print(f"\n  {label} (n={len(bucket)}):")
        print(f"    Тики В сторону CL позиции: {len(tick_consistent)}  "
              f"max_fav avg={mean(mf_consist):.1f}c" if mf_consist else
              f"    Тики В сторону CL позиции: {len(tick_consistent)}")
        print(f"    Тики ПРОТИВ CL позиции:   {len(tick_contra)}  "
              f"max_fav avg={mean(mf_contra):.1f}c" if mf_contra else
              f"    Тики ПРОТИВ CL позиции:   {len(tick_contra)}")

    # Пересечение референса: тик который меняет cl_vs_ref (нужны соседние строки)
    print(f"\n── Тики-пересечения референса (смена above↔below) ──")
    crossings = []
    prev_vs = {}
    for r in sorted(enriched, key=lambda x: (x["symbol"], x["ts_utc"])):
        sym = r["symbol"]
        cur = r.get("cl_vs_ref")
        prev = prev_vs.get(sym)
        if prev and cur and prev != cur and cur != "at":
            crossings.append(r)
        prev_vs[sym] = cur

    if crossings:
        active_cross = [r for r in crossings
                        if f(r.get("pm_ask_before")) is not None
                        and 0.15 <= f(r["pm_ask_before"]) <= 0.85]
        comp = [r for r in active_cross if r.get("direction_match") in ("Y", "N")]
        matched = [r for r in comp if r.get("direction_match") == "Y"]
        mf = [f(r["pm_max_fav_cents"]) for r in matched
              if f(r.get("pm_max_fav_cents")) is not None]
        print(f"  Всего пересечений: {len(crossings)}  активных (pm 0.15-0.85): {len(active_cross)}")
        if comp:
            print(f"  Direction match при пересечении: {len(matched)}/{len(comp)} = {len(matched)/len(comp)*100:.0f}%")
        if mf:
            print(f"  max_fav при пересечении: avg={mean(mf):.1f}c  median={median(mf):.1f}c")
    else:
        print("  Пересечений не обнаружено (нужно несколько тиков подряд по одному символу)")

    # ── Проверка исходов рынков ───────────────────────────────────────────────
    with_outcome = [r for r in enriched if r.get("market_outcome")]
    if with_outcome:
        print(f"\n── Реальные исходы рынков (по closePrice) ──")
        yes_rows = [r for r in with_outcome if r["market_outcome"] == "yes"]
        no_rows  = [r for r in with_outcome if r["market_outcome"] == "no"]
        print(f"  Событий с известным исходом: {len(with_outcome)}/{len(enriched)}")
        print(f"  YES победил: {len(yes_rows)} ({len(yes_rows)/len(with_outcome)*100:.0f}%)")
        print(f"  NO победил:  {len(no_rows)}  ({len(no_rows)/len(with_outcome)*100:.0f}%)")

        # Насколько PM предсказывал исход в момент тика
        pm_correct = [r for r in with_outcome
                      if (r["market_outcome"] == "yes" and f(r.get("pm_ask_before")) is not None
                          and f(r["pm_ask_before"]) > 0.5)
                      or (r["market_outcome"] == "no"  and f(r.get("pm_ask_before")) is not None
                          and f(r["pm_ask_before"]) <= 0.5)]
        print(f"  PM ask правильно предсказывал исход (в момент тика): "
              f"{len(pm_correct)}/{len(with_outcome)} = {len(pm_correct)/len(with_outcome)*100:.0f}%")

    # ── Ключевой кейс: PM < 0.5 но CL уже выше референса ────────────────────
    print(f"\n── Кейс: PM ask < 0.5, но CL выше референса (рынок недооценивает YES) ──")
    mispriced_yes = [r for r in enriched
                     if r.get("cl_vs_ref") == "above"
                     and f(r.get("pm_ask_before")) is not None
                     and f(r["pm_ask_before"]) < 0.5
                     and f(r["pm_ask_before"]) >= 0.15]
    if mispriced_yes:
        # Реальный исход: в скольких случаях YES действительно победил?
        with_out = [r for r in mispriced_yes if r.get("market_outcome")]
        yes_won  = [r for r in with_out if r["market_outcome"] == "yes"]
        no_won   = [r for r in with_out if r["market_outcome"] == "no"]
        if with_out:
            print(f"  Реальный исход (n={len(with_out)}):")
            print(f"    YES победил (CL удержался выше ref): {len(yes_won)}/{len(with_out)} = {len(yes_won)/len(with_out)*100:.0f}%")
            print(f"    NO победил  (CL упал ниже ref):      {len(no_won)}/{len(with_out)}  = {len(no_won)/len(with_out)*100:.0f}%")

        # Считаем: цена выросла (pm_end > pm_ask_before) = PM скорректировал вверх
        went_up   = [r for r in mispriced_yes
                     if f(r.get("pm_end")) is not None and f(r["pm_end"]) > f(r["pm_ask_before"]) + 0.005]
        went_down = [r for r in mispriced_yes
                     if f(r.get("pm_end")) is not None and f(r["pm_end"]) < f(r["pm_ask_before"]) - 0.005]
        flat      = [r for r in mispriced_yes if r not in went_up and r not in went_down]
        comparable = went_up + went_down
        mf_up   = [f(r["pm_max_fav_cents"]) for r in went_up   if f(r.get("pm_max_fav_cents")) is not None]
        mf_down = [f(r["pm_max_fav_cents"]) for r in went_down if f(r.get("pm_max_fav_cents")) is not None]
        gaps = [(0.5 - f(r["pm_ask_before"])) * 100 for r in mispriced_yes]
        print(f"  Событий: {len(mispriced_yes)}")
        print(f"  Средняя недооценка YES: {mean(gaps):.1f}c  (насколько ниже 0.5)")
        if comparable:
            print(f"  PM вырос  (→ YES): {len(went_up)}/{len(comparable)} = {len(went_up)/len(comparable)*100:.0f}%"
                  + (f"  max_fav avg={mean(mf_up):.1f}c" if mf_up else ""))
            print(f"  PM упал   (→ NO):  {len(went_down)}/{len(comparable)} = {len(went_down)/len(comparable)*100:.0f}%"
                  + (f"  max_fav avg={mean(mf_down):.1f}c" if mf_down else ""))
        print(f"  Нет данных / flat:  {len(flat)}")
        # По глубине недооценки
        # По дистанции CL от референса (насколько устойчиво выше)
        print(f"  По дистанции CL от референса (cl_price - ref) / ref * 100:")
        for lo, hi in [(0, 0.05), (0.05, 0.2), (0.2, 0.5), (0.5, 999)]:
            bucket = [r for r in mispriced_yes
                      if f(r.get("cl_ref_price")) is not None and f(r.get("cl_price")) is not None
                      and lo <= (f(r["cl_price"]) - f(r["cl_ref_price"])) / f(r["cl_ref_price"]) * 100 < hi]
            if not bucket:
                continue
            bu = [r for r in bucket if f(r.get("pm_end")) is not None
                  and f(r["pm_end"]) > f(r["pm_ask_before"]) + 0.005]
            bd = [r for r in bucket if f(r.get("pm_end")) is not None
                  and f(r["pm_end"]) < f(r["pm_ask_before"]) - 0.005]
            bc = bu + bd
            mf_u = [f(r["pm_max_fav_cents"]) for r in bu if f(r.get("pm_max_fav_cents")) is not None]
            mf_d = [f(r["pm_max_fav_cents"]) for r in bd if f(r.get("pm_max_fav_cents")) is not None]
            up_str = f"{len(bu)/len(bc)*100:.0f}% вверх" if bc else "—"
            hi_str = f"{hi:.2f}" if hi < 999 else "∞"
            mf_str = f"  max_fav(↑)={mean(mf_u):.1f}c (↓)={mean(mf_d):.1f}c" if mf_u and mf_d else ""
            print(f"    +{lo:.2f}%..+{hi_str}%: n={len(bucket)}  {up_str}{mf_str}")
    else:
        print("  Таких событий не найдено")

    # ── Зеркальный кейс: PM > 0.5 но CL ниже референса ──────────────────────
    print(f"\n── Кейс: PM ask > 0.5, но CL ниже референса (рынок переоценивает YES) ──")
    mispriced_no = [r for r in enriched
                    if r.get("cl_vs_ref") == "below"
                    and f(r.get("pm_ask_before")) is not None
                    and f(r["pm_ask_before"]) > 0.5
                    and f(r["pm_ask_before"]) <= 0.85]
    if mispriced_no:
        went_down = [r for r in mispriced_no
                     if f(r.get("pm_end")) is not None and f(r["pm_end"]) < f(r["pm_ask_before"]) - 0.005]
        went_up   = [r for r in mispriced_no
                     if f(r.get("pm_end")) is not None and f(r["pm_end"]) > f(r["pm_ask_before"]) + 0.005]
        comparable = went_up + went_down
        mf_down = [f(r["pm_max_fav_cents"]) for r in went_down if f(r.get("pm_max_fav_cents")) is not None]
        gaps = [(f(r["pm_ask_before"]) - 0.5) * 100 for r in mispriced_no]
        print(f"  Событий: {len(mispriced_no)}")
        print(f"  Средняя переоценка YES: {mean(gaps):.1f}c  (насколько выше 0.5)")
        if comparable:
            print(f"  PM упал   (→ NO):  {len(went_down)}/{len(comparable)} = {len(went_down)/len(comparable)*100:.0f}%"
                  + (f"  max_fav avg={mean(mf_down):.1f}c" if mf_down else ""))
            print(f"  PM вырос  (→ YES): {len(went_up)}/{len(comparable)} = {len(went_up)/len(comparable)*100:.0f}%")
    else:
        print("  Таких событий не найдено")

    print()


def analyze_binance_delta(rows: list[dict]) -> None:
    """Анализ binance_cl_delta_pct — насколько Binance опережает CL в момент тика."""
    has_bnb = [r for r in rows
               if r.get("binance_cl_delta_pct") not in (None, "", "None", "nan")]
    if not has_bnb:
        print("\n── Binance delta: нет данных (старый CSV без binance колонок) ──\n")
        return

    directional = [r for r in has_bnb if r.get("cl_direction") in ("up", "down")]

    print(f"\n{'='*60}")
    print(f"── Binance vs Chainlink delta ──")
    print(f"{'='*60}")
    print(f"  Событий с Binance данными: {len(has_bnb)}/{len(rows)}")

    deltas = [f(r["binance_cl_delta_pct"]) for r in has_bnb
              if f(r.get("binance_cl_delta_pct")) is not None]
    if deltas:
        pos = sum(1 for d in deltas if d > 0)
        neg = sum(1 for d in deltas if d < 0)
        print(f"  binance_cl_delta_pct: avg={mean(deltas):+.4f}%  median={median(deltas):+.4f}%")
        print(f"  Binance выше CL (Δ>0): {pos}/{len(deltas)} = {pos/len(deltas)*100:.0f}%")
        print(f"  Binance ниже CL (Δ<0): {neg}/{len(deltas)} = {neg/len(deltas)*100:.0f}%")

    # Совпадает ли знак дельты с направлением тика?
    # Подтверждающий: CL вверх + Binance выше CL, или CL вниз + Binance ниже CL
    print(f"\n── Binance направление vs CL тик ──")

    def _is_confirming(r: dict) -> bool:
        d = f(r.get("binance_cl_delta_pct"))
        if d is None:
            return False
        return (r["cl_direction"] == "up" and d > 0) or (r["cl_direction"] == "down" and d < 0)

    def _is_contradicting(r: dict) -> bool:
        d = f(r.get("binance_cl_delta_pct"))
        if d is None:
            return False
        return (r["cl_direction"] == "up" and d < 0) or (r["cl_direction"] == "down" and d > 0)

    confirming    = [r for r in directional if _is_confirming(r)]
    contradicting = [r for r in directional if _is_contradicting(r)]

    for group, label in [(confirming, "Binance подтверждает тик (Δ в сторону тика)"),
                         (contradicting, "Binance противоречит тику (Δ против тика)")]:
        if not group:
            continue
        comp    = [r for r in group if r.get("direction_match") in ("Y", "N")]
        matched = [r for r in comp  if r.get("direction_match") == "Y"]
        mf      = [f(r["pm_max_fav_cents"]) for r in matched
                   if f(r.get("pm_max_fav_cents")) is not None]
        pct_str = f"{len(matched)/len(comp)*100:.0f}%" if comp else "—"
        mf_str  = f"  max_fav avg={mean(mf):.1f}c" if mf else ""
        print(f"  {label}:")
        print(f"    n={len(group)}  match={len(matched)}/{len(comp)}={pct_str}{mf_str}")

    # По величине дельты
    print(f"\n── По величине |binance_cl_delta_pct| ──")
    for lo, hi in [(0, 0.01), (0.01, 0.03), (0.03, 0.05), (0.05, 0.1), (0.1, 999)]:
        bucket  = [r for r in directional
                   if f(r.get("binance_cl_delta_pct")) is not None
                   and lo <= abs(f(r["binance_cl_delta_pct"])) < hi]
        if not bucket:
            continue
        comp    = [r for r in bucket if r.get("direction_match") in ("Y", "N")]
        matched = [r for r in comp   if r.get("direction_match") == "Y"]
        mf      = [f(r["pm_max_fav_cents"]) for r in matched
                   if f(r.get("pm_max_fav_cents")) is not None]
        pct_str = f"{len(matched)/len(comp)*100:.0f}%" if comp else "—"
        mf_str  = f"  max_fav avg={mean(mf):.1f}c" if mf else ""
        hi_str  = f"{hi:.2f}" if hi < 999 else "∞"
        print(f"  {lo:.2f}%–{hi_str}%: n={len(bucket)}  match={pct_str}{mf_str}")

    # Активная зона (0.15–0.85): confirming vs contradicting
    print(f"\n── Binance сигнал (активная зона pm 0.15–0.85) ──")
    active_dir = [r for r in directional
                  if f(r.get("pm_ask_before")) is not None
                  and 0.15 <= f(r["pm_ask_before"]) <= 0.85]
    conf_act = [r for r in active_dir if _is_confirming(r)]
    cont_act = [r for r in active_dir if _is_contradicting(r)]

    for group, label in [(conf_act, "Binance подтверждает"),
                         (cont_act, "Binance противоречит")]:
        if not group:
            continue
        comp    = [r for r in group if r.get("direction_match") in ("Y", "N")]
        matched = [r for r in comp  if r.get("direction_match") == "Y"]
        mf      = [f(r["pm_max_fav_cents"]) for r in matched
                   if f(r.get("pm_max_fav_cents")) is not None]
        pct_str = f"{len(matched)/len(comp)*100:.0f}%" if comp else "—"
        mf_str  = f"  max_fav avg={mean(mf):.1f}c" if mf else ""
        print(f"  {label}: n={len(group)}  match={len(matched)}/{len(comp)}={pct_str}{mf_str}")

    print()


def main(argv: list[str] = sys.argv[1:]) -> None:
    if argv:
        path = Path(argv[0])
    else:
        data_dir = Path("research_bot/data")
        csvs = sorted(data_dir.glob("correlate_*.csv"), key=lambda p: p.stat().st_mtime)
        if not csvs:
            print("Нет CSV файлов в research_bot/data/")
            return
        path = csvs[-1]
        print(f"Авто-выбор последнего файла: {path}")

    rows = load(path)
    if not rows:
        print("Файл пустой")
        return
    enrich_with_ref_price(rows, path)
    analyze(rows)
    analyze_ref_price(rows)
    analyze_binance_delta(rows)


if __name__ == "__main__":
    main()
