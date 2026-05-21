import asyncio
import json
import time
import httpx
import redis.asyncio as aioredis

from config import REDIS_URL, CACHE_TTL, CACHE_TTL_BY_TYPE

# ── Redis client (shared across the process) ──────────────────────────────────

_redis: aioredis.Redis = aioredis.from_url(REDIS_URL, decode_responses=True)

# ── L1: in-memory cache (per-process, ultra-fast) ─────────────────────────────

_L1: dict = {}


def _ttl_for(url: str) -> int:
    for key, ttl in CACHE_TTL_BY_TYPE.items():
        if key in url:
            return ttl
    return CACHE_TTL


def _l1_get(url: str):
    entry = _L1.get(url)
    if entry and time.monotonic() - entry["ts"] < entry["ttl"]:
        return entry["data"]
    return None


def _l1_set(url: str, data, ttl: int) -> None:
    _L1[url] = {"data": data, "ts": time.monotonic(), "ttl": ttl}


# ── In-flight dedup (per-process) ─────────────────────────────────────────────

_INFLIGHT: dict[str, asyncio.Future] = {}
_CACHE_LOCK = asyncio.Lock()


# ── Main API fetch ─────────────────────────────────────────────────────────────

async def api_get_async(url: str, token: str, client: httpx.AsyncClient):
    """
    Fetch *url* with two-level caching + in-process request coalescing.

    L1 = in-memory (nanoseconds, per-process)
    L2 = Redis (milliseconds, shared across all workers and restarts)

    TTL varies by endpoint type — stable data like summaries/themes gets
    60 min, frequently-changing data like students gets 5 min.
    """
    # ── L1 fast path ──
    cached = _l1_get(url)
    if cached is not None:
        return cached

    ttl = _ttl_for(url)
    redis_key = f"api:{url}"

    # ── L2 Redis path ──
    try:
        r_val = await _redis.get(redis_key)
        if r_val:
            data = json.loads(r_val)
            _l1_set(url, data, ttl)
            return data
    except Exception:
        pass  # Redis unavailable — fall through to HTTP

    loop = asyncio.get_event_loop()

    async with _CACHE_LOCK:
        # Re-check L1 after lock (another coroutine may have fetched while we waited)
        cached = _l1_get(url)
        if cached is not None:
            return cached

        if url in _INFLIGHT:
            fut = _INFLIGHT[url]
        else:
            fut = loop.create_future()
            _INFLIGHT[url] = fut
            fut = None  # signal: we are the fetcher

    if fut is not None:
        return await asyncio.shield(fut)

    # ── We are the fetcher ──
    inflight_future = _INFLIGHT[url]
    try:
        resp = await client.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        _l1_set(url, data, ttl)

        # Write to Redis (best-effort, don't fail the request if Redis is down)
        try:
            await _redis.setex(redis_key, ttl, json.dumps(data, ensure_ascii=False))
        except Exception:
            pass

        inflight_future.set_result(data)
        return data
    except Exception as exc:
        inflight_future.set_exception(exc)
        raise
    finally:
        _INFLIGHT.pop(url, None)
