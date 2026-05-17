import asyncio
import time
import httpx
from config import CACHE_TTL

# { url -> {"data": ..., "ts": float} }
CACHE: dict = {}

# In-flight lock: prevents sending the same request twice while the first is pending
_INFLIGHT: dict[str, asyncio.Future] = {}
_CACHE_LOCK = asyncio.Lock()


def cache_get(key: str):
    entry = CACHE.get(key)
    if entry and time.monotonic() - entry["ts"] < CACHE_TTL:
        return entry["data"]
    return None


def cache_set(key: str, data) -> None:
    CACHE[key] = {"data": data, "ts": time.monotonic()}


async def api_get_async(url: str, token: str, client: httpx.AsyncClient):
    """
    Fetch *url* with caching + request coalescing.

    If two coroutines request the same URL simultaneously only ONE actual
    HTTP request is made; the second waits for the first and reuses its result.
    """
    # Fast path: cache hit (no lock needed for reads)
    cached = cache_get(url)
    if cached is not None:
        return cached

    loop = asyncio.get_event_loop()

    async with _CACHE_LOCK:
        # Re-check cache after acquiring lock
        cached = cache_get(url)
        if cached is not None:
            return cached

        # Another coroutine is already fetching this URL — wait for it
        if url in _INFLIGHT:
            fut = _INFLIGHT[url]
        else:
            fut = loop.create_future()
            _INFLIGHT[url] = fut
            fut = None  # signal that WE are the fetcher

    if fut is not None:
        # We are a waiter
        return await asyncio.shield(fut)

    # We are the fetcher
    inflight_future = _INFLIGHT[url]
    try:
        resp = await client.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        cache_set(url, data)
        inflight_future.set_result(data)
        return data
    except Exception as exc:
        inflight_future.set_exception(exc)
        raise
    finally:
        _INFLIGHT.pop(url, None)
