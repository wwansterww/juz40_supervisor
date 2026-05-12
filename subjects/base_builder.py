import asyncio
import httpx
from config import BASE_URL
from cache import api_get_async
from store import PROGRESS

CLIENT_LIMITS = httpx.Limits(
    max_connections=100,
    max_keepalive_connections=30,
    keepalive_expiry=30,
)


def is_left_course(progress: dict) -> bool:
    texts = []

    for comment in progress.get("comments") or []:
        texts.append((comment.get("commentText") or "").lower())

    for comment in progress.get("parentComments") or []:
        texts.append((comment.get("commentText") or "").lower())

    comment = progress.get("comment")
    if comment:
        texts.append(str(comment).lower())

    full_text = " ".join(texts)

    left_phrases = [
        "шыққан оқушы",
        "курстан шықты",
        "оқудан шықты",
        "шығып кетті",
        "- курс",

    ]

    return any(phrase in full_text for phrase in left_phrases)


def is_submitted(progress: dict) -> bool:
    """
    Считаем задание сданным только если оно реально отправлено/завершено.
    Просто открыть или начать тест недостаточно.
    """

    if progress.get("finished") is True:
        return True

    if progress.get("finishTime"):
        return True

    if progress.get("submissionTime"):
        return True

    submissions = progress.get("submissions")
    if submissions and len(submissions) > 0:
        return True

    submission_text = progress.get("submissionText")
    if submission_text is not None and str(submission_text).strip() != "":
        return True

    return False


def to_int(value) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def get_student_id(progress: dict) -> str:
    return (
        progress.get("studentId")
        or progress.get("username")
        or f"{progress.get('studentFirstname', '')}_{progress.get('studentLastname', '')}"
    )


async def _fetch_student_count_from_progresses(
    group_id: str,
    themes: list,
    token: str,
    client: httpx.AsyncClient,
) -> int:
    max_students_count = 0
    left_students = set()

    for theme in themes:
        theme_id = theme.get("themeId")
        if not theme_id:
            continue

        try:
            summary = await api_get_async(
                f"{BASE_URL}/v3/headteacher/groups/{group_id}/themes/{theme_id}/lessons/summary",
                token,
                client,
            )
        except Exception:
            continue

        if not summary or not isinstance(summary, list):
            continue

        lesson_ids = []

        for item in summary:
            students_count = to_int(
                item.get("studentsCount")
                or item.get("totalStudentsCount")
                or 0
            )

            max_students_count = max(max_students_count, students_count)

            lesson_id = item.get("lessonId") or item.get("id")
            if lesson_id:
                lesson_ids.append(lesson_id)

            for child in item.get("children") or []:
                child_students_count = to_int(
                    child.get("studentsCount")
                    or child.get("totalStudentsCount")
                    or 0
                )

                max_students_count = max(
                    max_students_count,
                    child_students_count,
                )

                child_lesson_id = child.get("lessonId") or child.get("id")
                if child_lesson_id:
                    lesson_ids.append(child_lesson_id)

        for lesson_id in set(lesson_ids):
            try:
                progresses = await api_get_async(
                    f"{BASE_URL}/v2/headteacher/groups/{group_id}/lessons/{lesson_id}/progresses",
                    token,
                    client,
                )
            except Exception:
                continue

            if not isinstance(progresses, list):
                continue

            for p in progresses:
                if not is_left_course(p):
                    continue

                student_id = get_student_id(p)

                if student_id:
                    left_students.add(student_id)

    final_count = max_students_count - len(left_students)

    if final_count < 0:
        final_count = 0

    return int(final_count)


def recalc_summary_item_by_progresses(
    item: dict,
    progresses: list,
    forced_students_count: int = None,
) -> dict:
    left_students = set()

    for p in progresses:
        if not is_left_course(p):
            continue

        student_id = get_student_id(p)

        if student_id:
            left_students.add(student_id)

    old_students_count = to_int(
        item.get("studentsCount")
        or item.get("totalStudentsCount")
        or 0
    )

    if forced_students_count is not None:
        new_students_count = int(forced_students_count)
    else:
        new_students_count = old_students_count - len(left_students)

    if new_students_count < 0:
        new_students_count = 0

    active_progresses = [
        p for p in progresses
        if not is_left_course(p)
    ]

    submitted_progresses = [
        p for p in active_progresses
        if is_submitted(p)
    ]

    submitted_count = len(submitted_progresses)

    scores = [
        p.get("score")
        for p in submitted_progresses
        if p.get("score") is not None
    ]

    average_score = None
    if scores:
        average_score = sum(scores) / len(scores)

    new_item = dict(item)

    # Главное:
    # studentsCount / totalStudentsCount теперь всегда = все активные ученики
    # Поэтому любой процент считается как:
    # submittedCount / барлық актив оқушы
    new_item["studentsCount"] = int(new_students_count)
    new_item["totalStudentsCount"] = int(new_students_count)

    # submittedCount = только те, кто реально сделал именно это задание
    new_item["submittedCount"] = int(submitted_count)
    new_item["reviewedCount"] = int(submitted_count)

    not_submitted = int(new_students_count) - int(submitted_count)
    if not_submitted < 0:
        not_submitted = 0

    new_item["notSubmittedCount"] = int(not_submitted)
    new_item["averageScore"] = average_score

    return new_item


async def recalc_summary_with_active_students(
    group_id: str,
    summary: list,
    token: str,
    client: httpx.AsyncClient,
) -> list:
    new_summary = []

    for item in summary:
        lesson_id = item.get("lessonId") or item.get("id")

        if not lesson_id:
            new_summary.append(item)
            continue

        try:
            progresses = await api_get_async(
                f"{BASE_URL}/v2/headteacher/groups/{group_id}/lessons/{lesson_id}/progresses",
                token,
                client,
            )
        except Exception:
            new_summary.append(item)
            continue

        if not isinstance(progresses, list):
            new_summary.append(item)
            continue

        new_item = recalc_summary_item_by_progresses(item, progresses)

        parent_students_count = int(
            new_item.get("studentsCount")
            or new_item.get("totalStudentsCount")
            or 0
        )

        children = item.get("children") or []
        new_children = []

        for child in children:
            child_lesson_id = child.get("lessonId") or child.get("id")

            if not child_lesson_id:
                new_children.append(child)
                continue

            try:
                child_progresses = await api_get_async(
                    f"{BASE_URL}/v2/headteacher/groups/{group_id}/lessons/{child_lesson_id}/progresses",
                    token,
                    client,
                )
            except Exception:
                new_children.append(child)
                continue

            if not isinstance(child_progresses, list):
                new_children.append(child)
                continue

            # ВАЖНО:
            # ҚЖ сияқты child тапсырмалар да parent_students_count бойынша есептеледі.
            # Яғни ҚЖ % = ҚЖ тапсырғандар / барлық актив оқушы.
            new_child = recalc_summary_item_by_progresses(
                child,
                child_progresses,
                forced_students_count=parent_students_count,
            )

            new_children.append(new_child)

        new_item["children"] = new_children
        new_summary.append(new_item)

    return new_summary


def make_builder(extract_metrics_fn, merge_metrics_fn, empty_metrics_fn):
    async def _fetch_week_metrics(group_id, week, study_month, token, client):
        try:
            resp = await api_get_async(
                f"{BASE_URL}/v1/headteacher/groups/{group_id}/themes?week={week}&month={study_month}",
                token,
                client,
            )
        except Exception:
            return empty_metrics_fn(), 0

        themes = resp.get("themes", [])
        valid_themes = [t for t in themes if t.get("themeId")]

        if not valid_themes:
            return empty_metrics_fn(), 0

        summary_responses = await asyncio.gather(
            *[
                api_get_async(
                    f"{BASE_URL}/v3/headteacher/groups/{group_id}/themes/{t['themeId']}/lessons/summary",
                    token,
                    client,
                )
                for t in valid_themes
            ],
            return_exceptions=True,
        )

        fixed_summary_responses = []

        for sr in summary_responses:
            if isinstance(sr, Exception) or not isinstance(sr, list):
                fixed_summary_responses.append(sr)
                continue

            fixed_sr = await recalc_summary_with_active_students(
                group_id,
                sr,
                token,
                client,
            )

            fixed_summary_responses.append(fixed_sr)

        student_count = await _fetch_student_count_from_progresses(
            group_id,
            valid_themes,
            token,
            client,
        )

        from utils import normalize

        week_theme_metrics = [
            extract_metrics_fn(sr, normalize(t.get("themeName") or ""))
            for t, sr in zip(valid_themes, fixed_summary_responses)
            if not isinstance(sr, Exception)
        ]

        metrics = (
            merge_metrics_fn(week_theme_metrics)
            if week_theme_metrics
            else empty_metrics_fn()
        )

        return metrics, int(student_count)

    async def build_group_all_weeks(group, token, study_month, client):
        group_id = group["id"]

        curator = group.get("curator", {})
        curator_name = f"{curator.get('lastname', '')} {curator.get('firstname', '')}".strip()

        course_name = group.get("courseName", "")

        week_results = await asyncio.gather(
            _fetch_week_metrics(group_id, 1, study_month, token, client),
            _fetch_week_metrics(group_id, 2, study_month, token, client),
            _fetch_week_metrics(group_id, 3, study_month, token, client),
            _fetch_week_metrics(group_id, 4, study_month, token, client),
            return_exceptions=True,
        )

        student_count = 0

        for wr in week_results:
            if isinstance(wr, Exception):
                continue

            _, cnt = wr

            if cnt > 0:
                student_count = int(cnt)
                break

        if student_count <= 0:
            return None

        base = {
            "Поток": course_name,
            "Куратор": curator_name,
            "Оқушы саны": int(student_count),
        }

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

        monthly = (
            merge_metrics_fn(all_week_metrics)
            if all_week_metrics
            else empty_metrics_fn()
        )

        return {
            "base": base,
            "weeks": weeks_data,
            "monthly": monthly,
        }

    async def _build_report_job(job_id, groups, token, month_num):
        groups = [
            g for g in groups
            if g.get("prolongCount", 0) >= 3
        ]

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
                batch = groups[i:i + batch_size]

                batch_results = await asyncio.gather(
                    *[
                        build_group_all_weeks(
                            g,
                            token,
                            month_num,
                            client,
                        )
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

    return _fetch_week_metrics, build_group_all_weeks, _build_report_job