"""
Process-wide concurrency primitives.

These coordinate load across ALL users and ALL reports on this worker:

  • API_SEM       — caps total parallel HTTP requests to juz40-edu.kz.
                    Without it, N concurrent reports × 50 in-report concurrency
                    would hammer the external API with N×50 requests at once.

  • REPORT_SEM    — caps concurrent report generations. Excess users wait
                    in a FIFO queue and see their position via get_queue_position().

If you ever go multi-worker, replace these with Redis-backed counters
(redis.asyncio.Redis.incr + a small Lua script). For a single-worker
deployment this is enough.
"""

import asyncio

# ── Tunables ──────────────────────────────────────────────────────────────────

# Max simultaneous HTTP requests to the external API across the entire process.
# Set generously so light/interactive endpoints (course-months, filter-courses)
# never queue behind a single report's bulk fetches. The external API tolerates
# ~250-300 parallel requests comfortably; at 100 we were artificially
# serializing UI clicks.
GLOBAL_API_LIMIT = 250

# Max simultaneous reports being built. Each report internally uses up to
# GLOBAL_SEMAPHORE_LIMIT (50) parallel requests, all of which still pass
# through API_SEM, so the real ceiling on API load is GLOBAL_API_LIMIT.
REPORT_SLOT_LIMIT = 10

# ── Primitives ────────────────────────────────────────────────────────────────

API_SEM = asyncio.Semaphore(GLOBAL_API_LIMIT)

_REPORT_SEM = asyncio.Semaphore(REPORT_SLOT_LIMIT)

# FIFO of job_ids currently holding a slot OR waiting for one. We use a plain
# list because we need to find positions by job_id, which dict-backed structures
# don't help with at this scale (worst case ~hundreds of entries).
_queue_order: list[str] = []
_queue_lock = asyncio.Lock()


# ── Public API ────────────────────────────────────────────────────────────────

def get_queue_position(job_id: str) -> int:
    """
    Returns 0 if the job is running (or unknown — caller should still treat
    that as "running" once status flips). Returns N >= 1 if the job is waiting
    with N people ahead of it.
    """
    try:
        idx = _queue_order.index(job_id)
    except ValueError:
        return 0
    # First REPORT_SLOT_LIMIT entries are holding slots; the rest wait.
    return max(0, idx - REPORT_SLOT_LIMIT + 1)


class _ReportSlot:
    def __init__(self, job_id: str):
        self.job_id = job_id

    async def __aenter__(self):
        async with _queue_lock:
            _queue_order.append(self.job_id)
        try:
            await _REPORT_SEM.acquire()
        except BaseException:
            async with _queue_lock:
                try:
                    _queue_order.remove(self.job_id)
                except ValueError:
                    pass
            raise
        return self

    async def __aexit__(self, *exc):
        _REPORT_SEM.release()
        async with _queue_lock:
            try:
                _queue_order.remove(self.job_id)
            except ValueError:
                pass


def report_slot(job_id: str) -> _ReportSlot:
    """
    Use as `async with report_slot(job_id):` around the heavy report-building
    code. Acquires one of REPORT_SLOT_LIMIT concurrent slots; while waiting,
    the job_id is in the queue and visible to get_queue_position().
    """
    return _ReportSlot(job_id)
