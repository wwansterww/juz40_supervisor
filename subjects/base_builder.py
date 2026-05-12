import asyncio
import httpx
from typing import Callable
from config import BASE_URL
from cache import api_get_async
from store import PROGRESS

CLIENT_LIMITS = httpx.Limits(
    max_connections=100,
    max_keepalive_connections=30,
    keepalive_expiry=30,
)


async def _fetch_student_count_from_progresses(
    group_id: str,
    themes: list,
    token: str,
    client: httpx.AsyncClient,
) -> int:
    """
    Берём первый урок из первой валидной темы и считаем len(progresses).
    Это и есть реальное число учеников текущей недели/месяца.
    """
    for theme in themes:
        theme_id = theme.get("themeId")
        if not theme_id:
            continue
        # Берём список уроков темы чтобы найти первый lessonId
        try:
            summary = await api_get_async(
                f"{BASE_URL}/v3/headteacher/groups/{group_id}/themes/{theme_id}/lessons/summary",
                token, client,
            )
        except Exception:
            continue

        if not summary or not isinstance(summary, list):
            continue

        # Берём первый урок у которого есть id
        lesson_id = None
        for item in summary:
            lid = item.get("lessonId") or item.get("id")
            if lid:
                lesson_id = lid
                break

        if not lesson_id:
            continue

        try:
            progresses = await api_get_async(
                f"{BASE_URL}/v2/headteacher/groups/{group_id}/lessons/{lesson_id}/progresses",
                token, client,
            )
            if isinstance(progresses, list) and len(progresses) > 0:
                return len(progresses)
        except Exception:
            continue

    return 0


def make_builder(extract_metrics_fn, merge_metrics_fn, empty_metrics_fn):
    async def _fetch_week_metrics(group_id, week, study_month, token, client):
        try:
            resp = await api_get_async(
                f"{BASE_URL}/v1/headteacher/groups/{group_id}/themes?week={week}&month={study_month}",
                token, client,
            )
        except Exception:
            return empty_metrics_fn(), 0

        themes = resp.get("themes", [])
        valid_themes = [t for t in themes if t.get("themeId")]
        if not valid_themes:
            return empty_metrics_fn(), 0

        # Получаем summary для всех тем параллельно
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

        # Считаем число учеников из progresses первого урока первой рабочей темы
        student_count = 0
        for t, sr in zip(valid_themes, summary_responses):
            if isinstance(sr, Exception) or not isinstance(sr, list) or not sr:
                continue
            lesson_id = None
            for item in sr:
                lid = item.get("lessonId") or item.get("id")
                if lid:
                    lesson_id = lid
                    break
            if not lesson_id:
                continue
            try:
                progresses = await api_get_async(
                    f"{BASE_URL}/v2/headteacher/groups/{group_id}/lessons/{lesson_id}/progresses",
                    token, client,
                )
                if isinstance(progresses, list) and len(progresses) > 0:
                    student_count = len(progresses)
                    break
            except Exception:
                continue

        from utils import normalize
        week_theme_metrics = [
            extract_metrics_fn(sr, normalize(t.get("themeName") or ""))
            for t, sr in zip(valid_themes, summary_responses)
            if not isinstance(sr, Exception)
        ]
        metrics = merge_metrics_fn(week_theme_metrics) if week_theme_metrics else empty_metrics_fn()
        return metrics, student_count

    async def build_group_all_weeks(group, token, study_month, client):
        group_id = group["id"]
        curator = group.get("curator", {})
        curator_name = f"{curator.get('lastname', '')} {curator.get('firstname', '')}".strip()
        course_name = group.get("courseName", "")

        # Запускаем все 4 недели параллельно
        week_results = await asyncio.gather(
            _fetch_week_metrics(group_id, 1, study_month, token, client),
            _fetch_week_metrics(group_id, 2, study_month, token, client),
            _fetch_week_metrics(group_id, 3, study_month, token, client),
            _fetch_week_metrics(group_id, 4, study_month, token, client),
            return_exceptions=True,
        )

        # Берём число учеников из первой недели где оно ненулевое
        student_count = 0
        for wr in week_results:
            if isinstance(wr, Exception):
                continue
            _, cnt = wr
            if cnt > 0:
                student_count = cnt
                break

        # Если ни одна неделя не дала учеников — пропускаем группу
        if not student_count:
            return None

        base = {"Поток": course_name, "Куратор": curator_name, "Оқушы саны": student_count}

        weeks_data = {}
        all_week_metrics = []
        for i, wr in enumerate(week_results, 1):
            if isinstance(wr, Exception):
                weeks_data[i] = empty_metrics_fn()
            else:
                metrics, _ = wr
                weeks_data[i] = metrics
                if any(v is not None for v in metrics.values()):
                    all_week_metrics.append(metrics)

        monthly = merge_metrics_fn(all_week_metrics) if all_week_metrics else empty_metrics_fn()
        return {"base": base, "weeks": weeks_data, "monthly": monthly}

    async def _build_report_job(job_id, groups, token, month_num):
        groups = [g for g in groups if g.get("prolongCount", 0) >= 3]
        total = len(groups)
        PROGRESS[job_id] = {"total": total, "done": 0, "status": "running", "results": []}
        batch_size = 10
        async with httpx.AsyncClient(limits=CLIENT_LIMITS) as client:
            results = []
            for i in range(0, total, batch_size):
                batch = groups[i: i + batch_size]
                batch_results = await asyncio.gather(
                    *[build_group_all_weeks(g, token, month_num, client) for g in batch],
                    return_exceptions=True,
                )
                for r in batch_results:
                    if not isinstance(r, Exception) and r is not None:
                        results.append(r)
                PROGRESS[job_id]["done"] = min(i + batch_size, total)
        PROGRESS[job_id]["status"] = "done"
        PROGRESS[job_id]["results"] = results

    return _fetch_week_metrics, build_group_all_weeks, _build_report_job