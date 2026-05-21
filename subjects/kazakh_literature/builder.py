import asyncio
import httpx

from subjects.base_builder import make_builder, CLIENT_LIMITS, _is_group_active
from subjects.kazakh_literature.metrics import (
    empty_metrics_kazakh_literature,
    extract_metrics,
    merge_metrics_kazakh_literature,
    metrics_to_row,
)

from config import BASE_URL
from cache import api_get_async
from store import PROGRESS
from subjects.base_builder import GLOBAL_SEMAPHORE_LIMIT


_fetch_week_metrics, build_group_all_weeks, _build_report_job = make_builder(
    extract_metrics_fn=extract_metrics,
    merge_metrics_fn=merge_metrics_kazakh_literature,
    empty_metrics_fn=empty_metrics_kazakh_literature,
    metrics_to_row_fn=metrics_to_row,
)


async def _process_single_course(course, token, study_month, client, semaphore):
    course_id = course["id"]
    course_name = course["name"]

    try:
        groups = await api_get_async(
            f"{BASE_URL}/v1/headteacher/courses/{course_id}/groups",
            token,
            client,
        )

        active_flags = await asyncio.gather(
            *[_is_group_active(g["id"], study_month, token, client) for g in groups]
        )
        groups = [g for g, active in zip(groups, active_flags) if active]

        if not groups:
            return None

        group_results_raw = await asyncio.gather(
            *[
                build_group_all_weeks(g, token, study_month, client, semaphore)
                for g in groups
            ],
            return_exceptions=True,
        )

        group_results = [
            r for r in group_results_raw
            if not isinstance(r, Exception) and r is not None
        ]

        if not group_results:
            return None

        course_avg = merge_metrics_kazakh_literature(
            [gr["monthly"] for gr in group_results]
        )

        total_students = sum(
            gr["base"].get("Оқушы саны", 0) or 0
            for gr in group_results
        )

        return metrics_to_row(
            {
                "Поток": course_name,
                "Оқушы саны": total_students,
            },
            course_avg,
        )

    except Exception:
        return None


async def _build_section_report_job(job_id, courses, token, study_month):
    total = len(courses)

    PROGRESS[job_id] = {
        "total": total,
        "done": 0,
        "status": "running",
        "results": [],
    }

    semaphore = asyncio.Semaphore(GLOBAL_SEMAPHORE_LIMIT)
    done_count = 0

    async with httpx.AsyncClient(limits=CLIENT_LIMITS) as client:

        async def _process_and_track(c):
            nonlocal done_count

            try:
                return await _process_single_course(
                    c,
                    token,
                    study_month,
                    client,
                    semaphore,
                )
            finally:
                done_count += 1
                PROGRESS[job_id]["done"] = done_count

        all_results = await asyncio.gather(
            *[_process_and_track(c) for c in courses],
            return_exceptions=True,
        )

    results = [
        r for r in all_results
        if not isinstance(r, Exception) and r is not None
    ]

    PROGRESS[job_id]["status"] = "done"
    PROGRESS[job_id]["results"] = results