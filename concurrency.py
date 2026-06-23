"""
Process-wide concurrency primitives.

These coordinate load across ALL users and ALL reports on this worker:

  • API_SEM       — caps total parallel HTTP requests to juz40-edu.kz.
                    Without it, N concurrent reports × 50 in-report concurrency
                    would hammer the external API with N×50 requests at once.

  • REPORT_SEM    — caps concurrent report generations. Excess users wait
                    in a FIFO queue and see their position via get_queue_position().

  • spawn()       — create_task + a strong reference until the task finishes,
                    so background jobs can't be garbage-collected mid-run.

MULTI-WORKER MODEL: the semaphores and FIFO queue below are per-process. To
run several uvicorn workers safely WITHOUT a fragile distributed lock, each
worker self-limits to its fair share of the deployment-wide budget — config.py
computes GLOBAL_API_LIMIT = API_LIMIT_TOTAL // WEB_CONCURRENCY (and likewise for
report slots). N workers × per-worker-share = the total, so the combined load on
the external API stays bounded no matter how many workers you start.

Job progress, metadata and results live in Redis (see store.py), and every
polling/result route reads them via .aget(), so those work cross-worker already.
The one thing that stays per-worker is get_queue_position()'s exact number: a
progress poll that lands on a different worker than the one running the job sees
position 0 (status still correctly shows "queued"/"running"/"done"). Making the
position itself cross-worker would need a Redis-backed queue; it's cosmetic, so
it's intentionally left in-process.
"""

import asyncio

from config import GLOBAL_API_LIMIT, REPORT_SLOT_LIMIT

# ── Tunables (per-worker shares, computed in config.py from the *_TOTAL env) ───
# GLOBAL_API_LIMIT  — max simultaneous HTTP requests to the external API for
#                     THIS worker. Set generously so light/interactive endpoints
#                     never queue behind a single report's bulk fetches.
# REPORT_SLOT_LIMIT — max simultaneous reports being built on THIS worker. Each
#                     report internally uses up to GLOBAL_SEMAPHORE_LIMIT (50)
#                     parallel requests, all of which still pass through API_SEM,
#                     so the real ceiling on API load is GLOBAL_API_LIMIT.
# Both are re-exported here so existing `from concurrency import ...` imports
# keep working unchanged.

# ── Primitives ────────────────────────────────────────────────────────────────

API_SEM = asyncio.Semaphore(GLOBAL_API_LIMIT)

# asyncio.create_task only keeps a weak reference to the task — without a
# strong one a long-running report job can be garbage-collected mid-run
# (symptom: progress freezes at N% with no error anywhere). Every fire-and-
# forget task in the app must go through spawn().
_BACKGROUND_TASKS: set[asyncio.Task] = set()


def spawn(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_TASKS.discard)
    return task



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
