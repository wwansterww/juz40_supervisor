from subjects.base_builder import make_builder, CLIENT_LIMITS
from subjects.history.metrics import (
    empty_metrics_history,
    extract_metrics,
    merge_metrics_history,
    metrics_to_row,
)

import asyncio
import httpx

from config import BASE_URL
from cache import api_get_async
from store import PROGRESS


_fetch_week_metrics, build_group_all_weeks, _build_report_job = make_builder(
    extract_metrics_fn=extract_metrics,
    merge_metrics_fn=merge_metrics_history,
    empty_metrics_fn=empty_metrics_history,
)


async def _process_single_course(course, token, study_month, client):
    course_id = course["id"]
    course_name = course["name"]

    try:
        groups = await api_get_async(
            f"{BASE_URL}/v1/headteacher/courses/{course_id}/groups",
            token,
            client,
        )

        groups = [
            g for g in groups
            if g.get("prolongCount", 0) >= 3
        ]

        if not groups:
            return None

        group_results = []

        for j in range(0, len(groups), 10):
            batch = groups[j:j + 10]

            batch_results = await asyncio.gather(
                *[
                    build_group_all_weeks(
                        g,
                        token,
                        study_month,
                        client,
                    )
                    for g in batch
                ],
                return_exceptions=True,
            )

            for r in batch_results:
                if not isinstance(r, Exception) and r is not None:
                    group_results.append(r)

        if not group_results:
            return None

        course_avg = merge_metrics_history([
            gr["monthly"]
            for gr in group_results
        ])

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

    async with httpx.AsyncClient(limits=CLIENT_LIMITS) as client:
        results = []

        for i in range(0, total, 5):
            batch = courses[i:i + 5]

            batch_results = await asyncio.gather(
                *[
                    _process_single_course(
                        c,
                        token,
                        study_month,
                        client,
                    )
                    for c in batch
                ],
                return_exceptions=True,
            )

            for r in batch_results:
                if not isinstance(r, Exception) and r is not None:
                    results.append(r)

            PROGRESS[job_id]["done"] = min(i + 5, total)

    PROGRESS[job_id]["status"] = "done"
    PROGRESS[job_id]["results"] = results