import asyncio
import json
import time
import httpx
import redis.asyncio as aioredis

from config import REDIS_URL, CACHE_TTL, CACHE_TTL_BY_TYPE
from concurrency import API_SEM

# ── Redis client (shared across the process) ──────────────────────────────────

_redis: aioredis.Redis = aioredis.from_url(REDIS_URL, decode_responses=True)

# ── Shared HTTP client for light/interactive endpoints ────────────────────────
# Endpoints like /course-months, /filter-courses each fire 1-3 small requests.
# Creating a fresh httpx.AsyncClient per request meant TLS handshake every time
# (≈100-300ms of pure overhead on each UI click). The shared client with
# keepalive reuses connections so subsequent calls only pay the round-trip.

_SHARED_CLIENT_LIMITS = httpx.Limits(
    max_connections=80,
    max_keepalive_connections=40,
    keepalive_expiry=60,
)
_shared_client: httpx.AsyncClient | None = None


def get_shared_client() -> httpx.AsyncClient:
    global _shared_client
    if _shared_client is None or _shared_client.is_closed:
        _shared_client = httpx.AsyncClient(
            limits=_SHARED_CLIENT_LIMITS,
            timeout=30,
        )
    return _shared_client

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
        # Retry ONLY hard timeouts/connection errors. 5xx is not retried.
        # 4 attempts total with gentle backoff so we actually pull the data
        # for slow endpoints (some lesson summary URLs on juz40-edu.kz are
        # genuinely slow for heavy subjects like History — without enough
        # retries we'd drop the data and show "-" in the report).
        BACKOFF = [0.3, 0.8, 2.0]  # 3 sleeps between 4 attempts
        REQUEST_TIMEOUT = 60       # 30s wasn't enough for heavy themes
        for attempt in range(4):
            try:
                async with API_SEM:
                    resp = await client.get(
                        url,
                        headers={"Authorization": f"Bearer {token}"},
                        timeout=REQUEST_TIMEOUT,
                    )
                resp.raise_for_status()
                data = resp.json()
                break
            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.PoolTimeout,
                    httpx.RemoteProtocolError) as exc:
                if attempt == 3:
                    raise
                await asyncio.sleep(BACKOFF[attempt])

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
        # If nobody was waiting on this future (we were the lone fetcher),
        # the exception we just stashed inside it never gets read, and Python
        # logs "Future exception was never retrieved" when the future is GC'd.
        # Calling .exception() marks it as retrieved without re-raising.
        try:
            inflight_future.exception()
        except (asyncio.InvalidStateError, asyncio.CancelledError):
            pass
        raise
    finally:
        _INFLIGHT.pop(url, None)
