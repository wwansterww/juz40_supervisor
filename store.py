import asyncio

import orjson
import redis.asyncio as aioredis

from config import REDIS_URL


_redis: aioredis.Redis = aioredis.from_url(REDIS_URL, decode_responses=False)


class _Proxy(dict):
    """
    A dict that syncs every write back to the parent _WriteThrough store.
    Used so that PROGRESS[job_id]["done"] = N transparently persists to Redis.
    """

    _ready = False  # class-level sentinel; overridden by instance attribute

    def __init__(self, parent: "_WriteThrough", key: str, data: dict):
        # Initialize dict contents BEFORE setting _ready so that super().__init__
        # (which may call __setitem__ internally) doesn't trigger a premature sync.
        super().__init__(data)
        self._parent = parent
        self._key = key
        self._ready = True

    def __setitem__(self, field, value):
        super().__setitem__(field, value)
        if self._ready:
            merged = dict(self)
            self._parent._local[self._key] = merged
            self._parent._fire(self._key, merged)


class _WriteThrough:
    """
    Dict-like store backed by Redis.

    Writes go to in-memory (L1) synchronously and fire an async write to
    Redis (L2) in the background so other workers can read the latest state.

    Reads use .get() for L1-only (fast, works within the same worker) or
    .aget() for L1 + Redis fallback (needed for cross-worker visibility).
    """

    def __init__(self, prefix: str, ttl: int):
        self._local: dict = {}
        self._prefix = prefix
        self._ttl = ttl

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _fire(self, key: str, data: dict) -> None:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self._async_write(key, data))
        except Exception:
            pass

    async def _async_write(self, key: str, data: dict) -> None:
        try:
            payload = orjson.dumps(data, default=str)
            await _redis.setex(f"{self._prefix}:{key}", self._ttl, payload)
        except Exception:
            pass

    # ── Dict interface ─────────────────────────────────────────────────────────

    def __setitem__(self, key: str, value: dict) -> None:
        self._local[key] = value
        self._fire(key, value)

    def __getitem__(self, key: str) -> _Proxy:
        return _Proxy(self, key, self._local[key])

    def get(self, key: str, default=None):
        """Sync read from L1 only. Works when the job ran on this worker."""
        data = self._local.get(key)
        if data is not None:
            return data
        return default

    async def aget(self, key: str, default=None):
        """
        Async read: L1 first, then Redis.
        Use this in route handlers for cross-worker visibility.
        """
        data = self._local.get(key)
        if data is not None:
            return data
        try:
            val = await _redis.get(f"{self._prefix}:{key}")
            if val:
                data = orjson.loads(val)
                self._local[key] = data   # warm L1 for subsequent reads
                return data
        except Exception:
            pass
        return default


# ── Shared stores ──────────────────────────────────────────────────────────────

# job_id -> {"total": N, "done": M, "status": "running"|"done", "results": [...]}
PROGRESS = _WriteThrough("progress", ttl=7200)

# report_key -> {"tables": [...], "title": "..."}
REPORT_STORE = _WriteThrough("report", ttl=14400)
