import asyncio
import httpx

from config import BASE_URL
from cache import api_get_async
from store import PROGRESS

from subjects.geometry.metrics import (
    empty_metrics,
    extract_metrics,
    merge_metrics,
    metrics_to_row,
)

CLIENT_LIMITS = httpx.Limits(
    max_connections=100,
    max_keepalive_connections=30,
    keepalive_expiry=30,
)


async def _fetch_week_metrics(
    group_id: str,
    week: int,
    study_month: int,
    token: str,
    client: httpx.AsyncClient,
) -> dict:
    try:
        resp = await api_get_async(
            f"{BASE_URL}/v1/headteacher/groups/{group_id}/themes?week={week}&month={study_month}",
            token, client,
        )
    except Exception:
        return empty_metrics()

    themes = resp.get("themes", [])

    valid_themes = [t for t in themes if t.get("themeId")]
    if not valid_themes:
        return empty_metrics()

    summary_responses = await asyncio.gather(
        *[
            api_get_async(
                f"{BASE_URL}/v3/headteacher/groups/{group_id}/themes/{t['themeId']}/lessons/summary",
                token, client,
            )
            for t in valid_themes
        ],
        return_exceptions=True,
    )

    week_theme_metrics = [
        extract_metrics(sr, (t.get("themeName") or "").upper())
        for t, sr in zip(valid_themes, summary_responses)
        if not isinstance(sr, Exception)
    ]

    return merge_metrics(week_theme_metrics) if week_theme_metrics else empty_metrics()


async def build_group_all_weeks(
    group: dict,
    token: str,
    study_month: int,
    client: httpx.AsyncClient,
):
    group_id = group["id"]
    curator = group.get("curator", {})
    curator_name = f"{curator.get('lastname', '')} {curator.get('firstname', '')}".strip()
    course_name = group.get("courseName", "")

    all_results = await asyncio.gather(
        api_get_async(
            f"{BASE_URL}/v3/headteacher/groups/{group_id}/students?month={study_month}",
            token, client,
        ),
        _fetch_week_metrics(group_id, 1, study_month, token, client),
        _fetch_week_metrics(group_id, 2, study_month, token, client),
        _fetch_week_metrics(group_id, 3, study_month, token, client),
        _fetch_week_metrics(group_id, 4, study_month, token, client),
        return_exceptions=True,
    )

    students_resp = all_results[0]

    if isinstance(students_resp, Exception):
        student_count = group.get("studentCount", 0)
    else:
        student_count = len(students_resp.get("students", []))

    if not student_count:
        return None

    base = {
        "Поток": course_name,
        "Куратор": curator_name,
        "Оқушы саны": student_count,
    }

    weeks_data = {}
    all_week_metrics = []

    for i, wm in enumerate(all_results[1:], 1):
        if isinstance(wm, Exception):
            weeks_data[i] = empty_metrics()
        else:
            weeks_data[i] = wm
            if any(v is not None for v in wm.values()):
                all_week_metrics.append(wm)

    monthly = merge_metrics(all_week_metrics) if all_week_metrics else empty_metrics()

    return {
        "base": base,
        "weeks": weeks_data,
        "monthly": monthly,
    }


async def _build_report_job(job_id: str, groups: list, token: str, month_num: int):
    total = len(groups)

    PROGRESS[job_id] = {
        "total": total,
        "done": 0,
        "status": "running",
        "results": [],
    }

    batch_size = 10

    async with httpx.AsyncClient(limits=CLIENT_LIMITS) as client:
        results = []

        for i in range(0, total, batch_size):
            batch = groups[i: i + batch_size]

            batch_results = await asyncio.gather(
                *[
                    build_group_all_weeks(g, token, month_num, client)
                    for g in batch
                ],
                return_exceptions=True,
            )

            for r in batch_results:
                if not isinstance(r, Exception) and r is not None:
                    results.append(r)

            PROGRESS[job_id]["done"] = min(i + batch_size, total)

    PROGRESS[job_id]["status"] = "done"
    PROGRESS[job_id]["results"] = results