import asyncio
import httpx

from config import BASE_URL
from cache import api_get_async
from store import PROGRESS
from subjects.informatics.builder import build_group_all_weeks, CLIENT_LIMITS
from subjects.informatics.metrics import merge_metrics, metrics_to_row, compute_avg_row


async def _process_course(
    course: dict,
    token: str,
    study_month: int,
    client: httpx.AsyncClient,
):
    """Бір курстың барлық топтары бойынша орта метрика қатарын қайтарады."""
    course_id = course["id"]
    course_name = course["name"]
    try:
        groups = await api_get_async(
            f"{BASE_URL}/v1/headteacher/courses/{course_id}/groups",
            token, client,
        )
        groups = [g for g in groups if g.get("prolongCount", 0) > 0]
        if not groups:
            return None

        batch_size = 10
        group_results = []
        for j in range(0, len(groups), batch_size):
            batch = groups[j: j + batch_size]
            batch_results = await asyncio.gather(
                *[build_group_all_weeks(g, token, study_month, client) for g in batch],
                return_exceptions=True,
            )
            for r in batch_results:
                if not isinstance(r, Exception) and r is not None:
                    group_results.append(r)

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
    Скользящий раздел отчёт.
    stream_courses: [{stream_month, study_month, courses: [...]}, ...]
    Нәтиже: [{stream_month, study_month, rows, avg_row}, ...]
    """
    total = sum(len(s["courses"]) for s in stream_courses)
    PROGRESS[job_id] = {"total": total, "done": 0, "status": "running", "results": []}

    done_count = 0
    results = []

    async with httpx.AsyncClient(limits=CLIENT_LIMITS) as client:
        for stream_info in stream_courses:
            stream_month = stream_info["stream_month"]
            study_month = stream_info["study_month"]
            courses = stream_info["courses"]

            stream_rows = []
            batch_size = 5
            for i in range(0, len(courses), batch_size):
                batch = courses[i: i + batch_size]
                batch_results = await asyncio.gather(
                    *[_process_course(c, token, study_month, client) for c in batch],
                    return_exceptions=True,
                )
                for r in batch_results:
                    if not isinstance(r, Exception) and r is not None:
                        stream_rows.append(r)
                done_count += len(batch)
                PROGRESS[job_id]["done"] = done_count

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
