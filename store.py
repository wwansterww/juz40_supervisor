import asyncio
import time

import logging

import orjson

from concurrency import spawn
from redis_client import redis_client as _redis

logger = logging.getLogger("juz40.store")


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
            self._parent._local_set(self._key, merged)
            self._parent._fire(self._key, merged)


class _WriteThrough:
    """
    Dict-like store backed by Redis.

    Writes go to in-memory (L1) synchronously and fire an async write to
    Redis (L2) in the background. The deployment is single-worker (see
    concurrency.py) — the Redis layer is here so progress/results survive a
    process restart, and so a future multi-worker migration doesn't need a
    storage rewrite.

    L1 entries expire after the same TTL as Redis. Without that the process
    would keep every report ever built in memory until restart.

    Reads use .get() for L1-only (fast path) or .aget() for L1 + Redis
    fallback (restart survival).
    """

    def __init__(self, prefix: str, ttl: int):
        # key -> (data, monotonic timestamp of last write)
        self._local: dict = {}
        self._prefix = prefix
        self._ttl = ttl

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _local_set(self, key: str, data: dict) -> None:
        self._local[key] = (data, time.monotonic())
        self._prune()

    def _local_get(self, key: str):
        entry = self._local.get(key)
        if entry is None:
            return None
        data, ts = entry
        if time.monotonic() - ts > self._ttl:
            del self._local[key]
            return None
        return data

    def _prune(self) -> None:
        # O(number of live jobs) per write — tens of entries at most, cheap.
        now = time.monotonic()
        stale = [k for k, (_, ts) in self._local.items() if now - ts > self._ttl]
        for k in stale:
            del self._local[k]

    def _fire(self, key: str, data: dict) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # No running loop (called from sync context) — L1 already holds the
            # write; the Redis copy is best-effort and can be skipped here.
            return
        spawn(self._async_write(key, data))

    async def _async_write(self, key: str, data: dict) -> None:
        try:
            payload = orjson.dumps(data, default=str)
            await _redis.setex(f"{self._prefix}:{key}", self._ttl, payload)
        except Exception as exc:
            # Best-effort persistence: a Redis hiccup must not fail the request,
            # but it should leave a trace (it means restart-survival is degraded).
            logger.warning("redis write failed for %s:%s — %s", self._prefix, key, exc)

    # ── Dict interface ─────────────────────────────────────────────────────────

    def __setitem__(self, key: str, value: dict) -> None:
        self._local_set(key, value)
        self._fire(key, value)

    def __getitem__(self, key: str) -> _Proxy:
        data = self._local_get(key)
        if data is None:
            raise KeyError(key)
        return _Proxy(self, key, data)

    def get(self, key: str, default=None):
        """Sync read from L1 only. Works when the job ran on this worker."""
        data = self._local_get(key)
        if data is not None:
            return data
        return default

    async def aget(self, key: str, default=None):
        """
        Async read: L1 first, then Redis.
        Use this in route handlers so results survive a process restart.
        """
        data = self._local_get(key)
        if data is not None:
            return data
        try:
            val = await _redis.get(f"{self._prefix}:{key}")
            if val:
                data = orjson.loads(val)
                self._local_set(key, data)   # warm L1 for subsequent reads
                return data
        except Exception:
            pass
        return default


# ── Shared stores ──────────────────────────────────────────────────────────────

# job_id -> {"total": N, "done": M, "status": "running"|"done", "results": [...]}
PROGRESS = _WriteThrough("progress", ttl=7200)

# report_key -> {"tables": [...], "title": "..."}
REPORT_STORE = _WriteThrough("report", ttl=14400)

# job_id -> {"course_name": ..., "study_month": ..., "week_filter": ...}
# Display metadata for a job, keyed by job_id, so the result page can be
# rendered from the ?job=... query param alone. Storing this in the session
# (the old way) meant one shared slot per browser: starting a second report
# in another tab silently overwrote the first one's metadata.
JOB_META = _WriteThrough("jobmeta", ttl=14400)
