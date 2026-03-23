"""Сырой дебаг API ответов."""
import os, base64, json, httpx
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
    return httpx.get(url, headers={"KALSHI-ACCESS-KEY": api_key, "KALSHI-ACCESS-SIGNATURE": sig, "KALSHI-ACCESS-TIMESTAMP": ts}, timeout=30).json()

# 1. Kalshi: просто берём любые рынки серии KXBTC15M
print("=== Kalshi KXBTC15M (no filters) ===")
data = kalshi_get("/markets?series_ticker=KXBTC15M&limit=5")
print(f"cursor: {data.get('cursor')}")
markets = data.get("markets", [])
print(f"count: {len(markets)}")
for m in markets[:3]:
    print(json.dumps({k: m[k] for k in ["ticker", "status", "result", "close_time", "title", "yes_sub_title"] if k in m}, indent=2))
print()

# 2. PM: просто берём последние закрытые рынки
print("=== PM last closed markets ===")
try:
    resp = httpx.get("https://gamma-api.polymarket.com/markets", params={"limit": 5, "closed": "true", "order": "endDate", "ascending": "false"}, timeout=15).json()
    for m in resp[:3]:
        print(f"  {m.get('question','')} | id={m.get('id','')} | end={m.get('endDate','')}")
except Exception as e:
    print(f"  error: {e}")
print()

# 3. PM: ищем через events API
print("=== PM events search ===")
try:
    resp = httpx.get("https://gamma-api.polymarket.com/events", params={"limit": 5, "tag": "crypto", "closed": "true", "order": "endDate", "ascending": "false"}, timeout=15).json()
    for ev in resp[:3]:
        print(f"  {ev.get('title','')} | id={ev.get('id','')}")
        for m in ev.get("markets", [])[:2]:
            print(f"    {m.get('question','')} | prices={m.get('outcomePrices','')} | id={m.get('id','')}")
except Exception as e:
    print(f"  error: {e}")
