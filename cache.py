import asyncio
import logging
import random
import time
from collections import OrderedDict

import httpx
import orjson

logger = logging.getLogger("juz40.cache")

from config import (
    CACHE_TTL, CACHE_TTL_BY_TYPE, L1_CACHE_MAX,
    EMPTY_CACHE_TTL, EMPTY_RETRY_ATTEMPTS,
)
from concurrency import API_SEM, GLOBAL_API_LIMIT
from redis_client import redis_client as _redis

# ── Shared HTTP client for light/interactive endpoints ────────────────────────

# Pool size is tied to GLOBAL_API_LIMIT so the two can't drift apart: API_SEM
# admits up to that many concurrent requests, and a smaller pool here would
# silently become the real ceiling (requests queueing on the pool, not the
# semaphore — at one point the semaphore said 250 while the pool said 80).
#
# The external API speaks HTTP/1.1 (no HTTP/2 multiplexing), so every request
# needs its own connection and a cold connection pays a full TLS handshake. We
# therefore keep ALL allowed connections warm (max_keepalive == max_connections):
# API_SEM already caps live requests at GLOBAL_API_LIMIT, so we never hold more
# sockets than we actually use, and a burst right after another reuses warm
# connections instead of re-handshaking. Idle sockets are dropped after the
# expiry window.
_SHARED_CLIENT_LIMITS = httpx.Limits(
    max_connections=GLOBAL_API_LIMIT,
    max_keepalive_connections=GLOBAL_API_LIMIT,
    keepalive_expiry=90,
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

_L1_MAX_SIZE = L1_CACHE_MAX

_L1: OrderedDict = OrderedDict()


def _ttl_for(url: str) -> int:
    # Classify by the MOST SPECIFIC endpoint token. The previous plain
    # "key in url" over the dict matched "groups" inside every
    # /groups/{id}/... URL (themes / summary / progresses), so they all
    # collapsed into the 900s "groups" bucket instead of their intended
    # longer TTLs — effectively capping almost everything at 15 min. Order
    # below is by specificity; the group-list endpoint is matched by suffix so
    # it doesn't swallow the sub-resources hanging off /groups/{id}/.
    if "/students" in url:
        return CACHE_TTL_BY_TYPE["students"]       # 300  (5 min)  — changes during month
    if "/progresses" in url:
        return CACHE_TTL_BY_TYPE["progresses"]     # 1800 (30 min) — drives grade freshness
    if "summary" in url:
        return CACHE_TTL_BY_TYPE["summary"]        # 3600 (1 h)   — counts re-derived from progresses
    if "/themes" in url:
        return CACHE_TTL_BY_TYPE["themes"]         # 3600 (1 h)   — structure, doesn't change
    if url.rstrip("/").endswith("/groups"):
        return CACHE_TTL_BY_TYPE["groups"]         # 900  (15 min)
    if "/courses" in url:
        return CACHE_TTL_BY_TYPE["courses"]        # 1800 (30 min)
    return CACHE_TTL


def _looks_empty(data) -> bool:
    """True when a 200 response carries no payload — the shape the upstream
    returns on a momentary glitch ("data didn't take"). Conservative: only the
    known envelope shapes count as empty, an unrecognized dict is assumed full
    so we never short-cache something we don't understand."""
    if not data:                       # None, {}, [], ""
        return True
    if isinstance(data, list):         # summary / progresses / groups list
        return len(data) == 0
    if isinstance(data, dict):         # students / themes / courses envelopes
        for key in ("content", "students", "themes"):
            if key in data:
                return not data.get(key)
        return False
    return False


def _suspicious_empty(url: str, data) -> bool:
    """An empty response from an endpoint that should virtually always have data
    when the thing it describes exists (a real group has students, a listed
    lesson has progresses/summary, a real course has groups). themes/courses are
    excluded — an empty week or product page there is often legitimate, and we
    don't want to slow every report down retrying those."""
    if not _looks_empty(data):
        return False
    if url.endswith("/groups"):                 # a course's group list
        return True
    return ("/students" in url) or ("/progresses" in url) or ("summary" in url)


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
    except Exception as exc:
        logger.debug("redis get failed for %s — %s", redis_key, exc)

    url_lock = await _get_url_lock(url)

    async with url_lock:
        cached = _l1_get(url)
        if cached is not None:
            return cached

        if url in _INFLIGHT:
            fut = _INFLIGHT[url]
        else:
            loop = asyncio.get_running_loop()
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
                # A 200-with-empty body from a should-have-data endpoint almost
                # always means the upstream momentarily "didn't take" the data —
                # the root cause of reports coming back empty. Give it a couple
                # of quick retries before accepting it instead of freezing the
                # emptiness into the cache.
                if (attempt < min(EMPTY_RETRY_ATTEMPTS, ATTEMPTS - 1)
                        and _suspicious_empty(url, data)):
                    await asyncio.sleep(_jittered(BACKOFF[attempt]))
                    continue
                break
            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.PoolTimeout,
                    httpx.RemoteProtocolError) as exc:
                if attempt == ATTEMPTS - 1:
                    raise
                await asyncio.sleep(_jittered(BACKOFF[attempt]))

        # Empty responses are cached only briefly (EMPTY_CACHE_TTL) instead of
        # the full per-type TTL (themes/summary are an HOUR), so a transient
        # empty self-heals within a minute and a re-run actually re-fetches.
        if _looks_empty(data):
            logger.info("upstream returned empty payload, short-caching %ss: %s",
                        EMPTY_CACHE_TTL, url)
            cache_ttl = EMPTY_CACHE_TTL
        else:
            cache_ttl = ttl
        blob = orjson.dumps(data)
        _l1_set(url, blob, cache_ttl)

        try:
            await _redis.setex(redis_key, cache_ttl, blob)
        except Exception as exc:
            logger.debug("redis setex failed for %s — %s", redis_key, exc)

        inflight_future.set_result(blob)
        return data
    except Exception as exc:
        logger.warning("upstream GET failed after retries: %s — %s", url, exc)
        inflight_future.set_exception(exc)
        try:
            inflight_future.exception()
        except (asyncio.InvalidStateError, asyncio.CancelledError):
            pass
        raise
    finally:
        _INFLIGHT.pop(url, None)
