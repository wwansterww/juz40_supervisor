import asyncio
import time
from collections import OrderedDict

import httpx
import orjson
import redis.asyncio as aioredis

from config import REDIS_URL, CACHE_TTL, CACHE_TTL_BY_TYPE
from concurrency import API_SEM

# ── Redis client (shared across the process) ──────────────────────────────────

_redis: aioredis.Redis = aioredis.from_url(REDIS_URL, decode_responses=False)

# ── Shared HTTP client for light/interactive endpoints ────────────────────────

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

# ── L1: in-memory LRU cache (per-process) ────────────────────────────────────

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
        return entry[0]
    del _L1[url]
    return None


def _l1_set(url: str, data, ttl: int) -> None:
    _L1[url] = (data, time.monotonic(), ttl)
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
            data = orjson.loads(r_val)
            _l1_set(url, data, ttl)
            return data
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
        return await asyncio.shield(fut)

    inflight_future = _INFLIGHT[url]
    try:
        BACKOFF = [0.3, 0.8, 2.0]
        REQUEST_TIMEOUT = 60
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

        try:
            await _redis.setex(redis_key, ttl, orjson.dumps(data))
        except Exception:
            pass

        inflight_future.set_result(data)
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
