import asyncio
import random
import time
from collections import OrderedDict

import httpx
import orjson
import redis.asyncio as aioredis

from config import REDIS_URL, CACHE_TTL, CACHE_TTL_BY_TYPE
from concurrency import API_SEM, GLOBAL_API_LIMIT

# ── Redis client (shared across the process) ──────────────────────────────────

_redis: aioredis.Redis = aioredis.from_url(REDIS_URL, decode_responses=False)

# ── Shared HTTP client for light/interactive endpoints ────────────────────────

# Pool size is tied to GLOBAL_API_LIMIT so the two can't drift apart: API_SEM
# admits up to that many concurrent requests, and a smaller pool here would
# silently become the real ceiling (requests queueing on the pool, not the
# semaphore — at one point the semaphore said 250 while the pool said 80).
_SHARED_CLIENT_LIMITS = httpx.Limits(
    max_connections=GLOBAL_API_LIMIT,
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

# ── L1: in-memory LRU cache (per-process) ────────────────────────────────────
#
# Entries are stored as serialized orjson bytes, not live Python objects.
# Every hit deserializes a fresh copy, so a caller that mutates a response
# (filters a list in place, etc.) can't corrupt the cache for everyone else.
# orjson round-trips are orders of magnitude cheaper than the Redis/API trip
# this cache avoids, and bytes take less memory than the object graphs did.

_L1_MAX_SIZE = 4096

_L1: OrderedDict = OrderedDict()


def _ttl_for(url: str) -> int:
    for key, ttl in CACHE_TTL_BY_TYPE.items():
        if key in url:
            return ttl
    return CACHE_TTL


def _l1_get(url: str):
    entry = _L1.get(url)
    if entry is None:
        return None
    if time.monotonic() - entry[1] < entry[2]:
        _L1.move_to_end(url)
        return orjson.loads(entry[0])
    del _L1[url]
    return None


def _l1_set(url: str, blob: bytes, ttl: int) -> None:
    """*blob* is orjson-serialized payload bytes."""
    _L1[url] = (blob, time.monotonic(), ttl)
    _L1.move_to_end(url)
    while len(_L1) > _L1_MAX_SIZE:
        _L1.popitem(last=False)


# ── In-flight dedup (per-URL locks instead of one global lock) ────────────────

_INFLIGHT: dict[str, asyncio.Future] = {}
_URL_LOCKS: dict[str, asyncio.Lock] = {}
_LOCKS_LOCK = asyncio.Lock()

_URL_LOCK_HIGH_WATER = 2048
_URL_LOCK_LOW_WATER = 1024


async def _get_url_lock(url: str) -> asyncio.Lock:
    lock = _URL_LOCKS.get(url)
    if lock is not None:
        return lock
    async with _LOCKS_LOCK:
        lock = _URL_LOCKS.get(url)
        if lock is None:
            if len(_URL_LOCKS) > _URL_LOCK_HIGH_WATER:
                to_remove = list(_URL_LOCKS.keys())[:len(_URL_LOCKS) - _URL_LOCK_LOW_WATER]
                for k in to_remove:
                    lk = _URL_LOCKS[k]
                    if not lk.locked():
                        del _URL_LOCKS[k]
            lock = asyncio.Lock()
            _URL_LOCKS[url] = lock
        return lock


# ── Main API fetch ─────────────────────────────────────────────────────────────

async def api_get_async(url: str, token: str, client: httpx.AsyncClient):
    cached = _l1_get(url)
    if cached is not None:
        return cached

    ttl = _ttl_for(url)
    redis_key = f"api:{url}"

    try:
        r_val = await _redis.get(redis_key)
        if r_val:
            _l1_set(url, r_val, ttl)
            return orjson.loads(r_val)
    except Exception:
        pass

    url_lock = await _get_url_lock(url)

    async with url_lock:
        cached = _l1_get(url)
        if cached is not None:
            return cached

        if url in _INFLIGHT:
            fut = _INFLIGHT[url]
        else:
            loop = asyncio.get_event_loop()
            fut = loop.create_future()
            _INFLIGHT[url] = fut
            fut = None

    if fut is not None:
        # The future resolves to serialized bytes — every waiter deserializes
        # its own copy so concurrent callers never share a mutable object.
        return orjson.loads(await asyncio.shield(fut))

    inflight_future = _INFLIGHT[url]
    try:
        # Retried failures: network timeouts AND 429/5xx responses. Under load
        # the upstream answers 502/503 long before it stops answering at all —
        # without retrying those, every load spike turns into reports built
        # from missing data. 4xx (except 429) are NOT retried: they mean
        # "wrong request / expired token", repeating won't change the answer.
        BACKOFF = [0.3, 0.8, 2.0, 4.0]
        ATTEMPTS = len(BACKOFF) + 1
        RETRYABLE_STATUS = {429, 500, 502, 503, 504}
        REQUEST_TIMEOUT = 60

        def _jittered(delay: float) -> float:
            # Jitter spreads the retries of hundreds of concurrent coroutines
            # so they don't re-hit the struggling API in one synchronized wave.
            return delay * random.uniform(0.8, 1.3)

        for attempt in range(ATTEMPTS):
            try:
                async with API_SEM:
                    resp = await client.get(
                        url,
                        headers={"Authorization": f"Bearer {token}"},
                        timeout=REQUEST_TIMEOUT,
                    )
                if resp.status_code in RETRYABLE_STATUS and attempt < ATTEMPTS - 1:
                    delay = BACKOFF[attempt]
                    if resp.status_code == 429:
                        try:
                            delay = max(delay, float(resp.headers.get("Retry-After", 0)))
                        except (TypeError, ValueError):
                            pass
                    await asyncio.sleep(_jittered(delay))
                    continue
                resp.raise_for_status()
                data = resp.json()
                break
            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.PoolTimeout,
                    httpx.RemoteProtocolError) as exc:
                if attempt == ATTEMPTS - 1:
                    raise
                await asyncio.sleep(_jittered(BACKOFF[attempt]))

        blob = orjson.dumps(data)
        _l1_set(url, blob, ttl)

        try:
            await _redis.setex(redis_key, ttl, blob)
        except Exception:
            pass

        inflight_future.set_result(blob)
        return data
    except Exception as exc:
        inflight_future.set_exception(exc)
        try:
            inflight_future.exception()
        except (asyncio.InvalidStateError, asyncio.CancelledError):
            pass
        raise
    finally:
        _INFLIGHT.pop(url, None)
