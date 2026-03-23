"""Проверка резолюции рынков 12:00-12:15 ET, March 22."""
import os, base64, httpx, sys
from dotenv import load_dotenv
from datetime import datetime
from urllib.parse import urlparse
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

load_dotenv()

api_key = os.environ["KALSHI_API_KEY_ID"]
with open(os.environ["KALSHI_PRIVATE_KEY_PATH"], "rb") as f:
    pk = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
BASE = "https://api.elections.kalshi.com/trade-api/v2"

def kalshi_get(path):
    url = BASE + path
    ts = str(int(datetime.now().timestamp() * 1000))
    p = urlparse(url).path
    sig = base64.b64encode(pk.sign(f"{ts}GET{p}".encode(), padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())).decode()
    return httpx.get(url, headers={"KALSHI-ACCESS-KEY": api_key, "KALSHI-ACCESS-SIGNATURE": sig, "KALSHI-ACCESS-TIMESTAMP": ts}, timeout=60).json()

# ── Шаг 1: Kalshi — все рынки серий (без фильтра status) ──
print("=== Kalshi: все рынки серий ===")
for series in ["KXBTC15M", "KXXRP15M", "KXSOL15M"]:
    print(f"\n{series}:")
    try:
        data = kalshi_get(f"/markets?series_ticker={series}&limit=20")
        for m in data.get("markets", []):
            close = m.get("close_time", "")
            if "2026-03-22" in close:
                print(f"  {m['ticker']} | status={m.get('status')} | result={m.get('result','')} | close={close}")
        if not any("2026-03-22" in m.get("close_time","") for m in data.get("markets",[])):
            # Покажем что есть
            for m in data.get("markets", [])[:3]:
                print(f"  {m['ticker']} | status={m.get('status')} | close={m.get('close_time','')}")
            print(f"  ... (всего {len(data.get('markets',[]))} рынков)")
    except Exception as e:
        print(f"  ERROR: {e}")

# ── Шаг 2: PM — последние crypto рынки ──
print("\n=== Polymarket: последние crypto рынки ===")
try:
    resp = httpx.get("https://gamma-api.polymarket.com/markets", params={
        "limit": 30, "closed": "true",
    }, timeout=30).json()
    for m in resp:
        q = m.get("question", "")
        if "Up or Down" in q:
            print(f"  {q} | prices={m.get('outcomePrices','')} | id={m.get('id','')}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── Шаг 3: PM — ищем конкретные ID из нашего бота ──
print("\n=== Polymarket: поиск по condition_id из feeds ===")
try:
    from cross_arb_bot.polymarket_feed import PolymarketFeed
    import yaml
    with open("real_arb_bot/config.yaml") as f:
        config = yaml.safe_load(f)
    pf = PolymarketFeed(
        base_url=config["polymarket"]["gamma_base_url"],
        page_size=config["polymarket"]["page_size"],
        request_delay_ms=config["polymarket"]["request_delay_ms"],
        market_filter=config["market_filter"],
    )
    markets = pf.fetch_markets()
    for m in markets:
        if "12:00" in m.title or "11:45" in m.title or "12:15" in m.title:
            print(f"  {m.title} | yes={m.yes_ask} no={m.no_ask} | id={m.market_id}")
    if not any("12:00" in m.title or "12:15" in m.title for m in markets):
        print("  (нет 12:00/12:15 рынков в текущих active)")
        # Покажем что есть
        for m in markets[:5]:
            print(f"  {m.title} | id={m.market_id}")
except Exception as e:
    print(f"  ERROR: {e}")
