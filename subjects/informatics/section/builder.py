import asyncio
import httpx

from config import BASE_URL
from cache import api_get_async
from store import PROGRESS
from subjects.informatics.builder import build_group_all_weeks, CLIENT_LIMITS, GLOBAL_SEMAPHORE_LIMIT
from subjects.informatics.metrics import merge_metrics_info as merge_metrics, metrics_to_row, compute_avg_row_info as compute_avg_row


async def _process_course(
    course: dict,
    token: str,
    study_month: int,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
):
    """Returns a single metrics row for one course (all its groups aggregated)."""
    course_id = course["id"]
    course_name = course["name"]
    try:
        groups = await api_get_async(
            f"{BASE_URL}/v1/headteacher/courses/{course_id}/groups",
            token, client,
        )
        active_flags = await asyncio.gather(
            *[_is_group_active(g["id"], study_month, token, client) for g in groups]
        )
        groups = [g for g, active in zip(groups, active_flags) if active]
        if not groups:
            return None

        # All groups in parallel — semaphore limits actual HTTP concurrency
        group_results_raw = await asyncio.gather(
            *[build_group_all_weeks(g, token, study_month, client, semaphore) for g in groups],
            return_exceptions=True,
        )
        group_results = [r for r in group_results_raw if not isinstance(r, Exception) and r is not None]

        if not group_results:
            return None

        course_avg = merge_metrics([gr["monthly"] for gr in group_results])
        total_students = sum(gr["base"].get("Оқушы саны", 0) or 0 for gr in group_results)
        return metrics_to_row(
            {"Поток": course_name, "Оқушы саны": total_students},
            course_avg,
        )
    except Exception:
        return None


async def build_sliding_section_report_job(
    job_id: str,
    stream_courses: list,
    token: str,
):
    """
    Sliding section report.
    stream_courses: [{"stream_month": int, "study_month": int, "courses": [...]}, ...]
    Result stored in PROGRESS[job_id]["results"]: [{"stream_month", "study_month", "rows", "avg_row"}, ...]
    """
    total = sum(len(s["courses"]) for s in stream_courses)
    PROGRESS[job_id] = {"total": total, "done": 0, "status": "running", "results": []}

    semaphore = asyncio.Semaphore(GLOBAL_SEMAPHORE_LIMIT)
    done_count = 0
    results = []

    async with httpx.AsyncClient(limits=CLIENT_LIMITS) as client:
        for stream_info in stream_courses:
            stream_month = stream_info["stream_month"]
            study_month = stream_info["study_month"]
            courses = stream_info["courses"]

            async def _process_and_track(c, _done=done_count):
                nonlocal done_count
                try:
                    return await _process_course(c, token, study_month, client, semaphore)
                finally:
                    done_count += 1
                    PROGRESS[job_id]["done"] = done_count

            # All courses for this stream in parallel
            stream_results_raw = await asyncio.gather(
                *[_process_and_track(c) for c in courses],
                return_exceptions=True,
            )
            stream_rows = [r for r in stream_results_raw if not isinstance(r, Exception) and r is not None]

            stream_avg = compute_avg_row(stream_rows) if stream_rows else None
            for r in stream_rows:
                r.pop("Куратор", None)
            if stream_avg:
                stream_avg.pop("Куратор", None)
                stream_avg["Поток"] = "⌀ Орта көрсеткіш"

            if stream_rows:
                results.append({
                    "stream_month": stream_month,
                    "study_month": study_month,
                    "rows": stream_rows,
                    "avg_row": stream_avg,
                })

    PROGRESS[job_id]["status"] = "done"
    PROGRESS[job_id]["results"] = results
