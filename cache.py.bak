import time
import httpx
from config import CACHE_TTL

CACHE: dict = {}


def cache_get(key: str):
    entry = CACHE.get(key)
    if entry and time.time() - entry["ts"] < CACHE_TTL:
        return entry["data"]
    return None


def cache_set(key: str, data):
    CACHE[key] = {"data": data, "ts": time.time()}


async def api_get_async(url: str, token: str, client: httpx.AsyncClient):
    cached = cache_get(url)
    if cached is not None:
        return cached
    resp = await client.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    cache_set(url, data)
    return data
