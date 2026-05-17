import asyncio
import httpx
from config import BASE_URL
from cache import api_get_async
from store import PROGRESS

CLIENT_LIMITS = httpx.Limits(
    max_connections=100,
    max_keepalive_connections=40,
    keepalive_expiry=30,
)

# Global semaphore limit — controls max concurrent HTTP requests across all coroutines
GLOBAL_SEMAPHORE_LIMIT = 50

EXCLUDE_PHRASES = [
    "шыққан оқушы",
    "курстан шықты",
    "оқудан шықты",
    "шығып кетті",
    "- курс",
    "шыққан",
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def is_left_course(progress: dict) -> bool:
    texts = []
    for comment in (progress.get("comments") or []):
        texts.append((comment.get("commentText") or "").lower())
    for comment in (progress.get("parentComments") or []):
        texts.append((comment.get("commentText") or "").lower())
    comment = progress.get("comment")
    if comment:
        texts.append(str(comment).lower())
    full_text = " ".join(texts)
    return any(phrase in full_text for phrase in EXCLUDE_PHRASES)


def is_submitted(progress: dict) -> bool:
    if progress.get("finished") is True:
        return True
    if progress.get("finishTime") or progress.get("submissionTime"):
        return True
    submissions = progress.get("submissions")
    if submissions and len(submissions) > 0:
        return True
    sub_text = progress.get("submissionText")
    if sub_text is not None and str(sub_text).strip():
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


# ── Cached fetchers ────────────────────────────────────────────────────────────

async def _fetch_summary(group_id, theme_id, token, client, semaphore):
    async with semaphore:
        try:
            data = await api_get_async(
                f"{BASE_URL}/v3/headteacher/groups/{group_id}/themes/{theme_id}/lessons/summary",
                token, client,
            )
            return data if isinstance(data, list) else []
        except Exception:
            return []


async def _fetch_progresses(group_id, lesson_id, token, client, semaphore):
    async with semaphore:
        try:
            data = await api_get_async(
                f"{BASE_URL}/v2/headteacher/groups/{group_id}/lessons/{lesson_id}/progresses",
                token, client,
            )
            return data if isinstance(data, list) else []
        except Exception:
            return []


# ── Progress recalc ────────────────────────────────────────────────────────────

def _recalc_item(item: dict, progresses: list, forced_count: int = None) -> dict:
    left_ids = {
        get_student_id(p) for p in progresses
        if is_left_course(p) and get_student_id(p)
    }

    old_count = to_int(item.get("studentsCount") or item.get("totalStudentsCount") or 0)
    new_count = max(0, (forced_count if forced_count is not None else old_count) - len(left_ids))

    submitted = 0
    scores = []
    for p in progresses:
        if is_left_course(p):
            continue
        if is_submitted(p):
            submitted += 1
            score = p.get("score")
            if score is not None:
                scores.append(score)

    new_item = dict(item)
    new_item["studentsCount"] = new_count
    new_item["totalStudentsCount"] = new_count
    new_item["submittedCount"] = submitted
    new_item["reviewedCount"] = submitted
    new_item["notSubmittedCount"] = max(0, new_count - submitted)
    new_item["averageScore"] = (sum(scores) / len(scores)) if scores else None
    return new_item


def _count_active_from_progresses(all_progresses: list[list], max_students: int) -> int:
    left_ids: set = set()
    for progresses in all_progresses:
        for p in progresses:
            if is_left_course(p):
                sid = get_student_id(p)
                if sid:
                    left_ids.add(sid)
    return max(0, max_students - len(left_ids))


# ── Parallel paginated course loader ──────────────────────────────────────────

async def fetch_all_pages(base_url: str, token: str, client: httpx.AsyncClient) -> list:
    """
    Fetches the first page of *base_url* (must include ?page=0 or &page=0),
    then fetches remaining pages in parallel.
    """
    first = await api_get_async(base_url, token, client)
    content = first.get("content", [])
    total_pages = first.get("totalPages", 1)

    if total_pages <= 1:
        return content

    # Replace page=0 with page=N for remaining pages
    rest_urls = [base_url.replace("page=0", f"page={p}") for p in range(1, total_pages)]
    results = await asyncio.gather(
        *[api_get_async(u, token, client) for u in rest_urls],
        return_exceptions=True,
    )
    for r in results:
        if not isinstance(r, Exception):
            content.extend(r.get("content", []))
    return content


# ── Core builder ───────────────────────────────────────────────────────────────

def make_builder(extract_metrics_fn, merge_metrics_fn, empty_metrics_fn, metrics_to_row_fn=None):

    async def _fetch_week_metrics(group_id, week, study_month, token, client, semaphore):
        # 1. Load week themes
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

        # 2. Load all summaries in parallel
        summary_responses: list[list] = await asyncio.gather(
            *[_fetch_summary(group_id, t["themeId"], token, client, semaphore)
              for t in valid_themes],
            return_exceptions=True,
        )
        summary_responses = [
            sr if isinstance(sr, list) else []
            for sr in summary_responses
        ]

        # 3. Collect all lesson_ids, load progresses once in parallel
        lesson_id_map: dict[str, list] = {}
        max_students = 0

        for sr in summary_responses:
            for item in sr:
                sc = to_int(item.get("studentsCount") or item.get("totalStudentsCount") or 0)
                max_students = max(max_students, sc)
                lid = item.get("lessonId") or item.get("id")
                if lid:
                    lesson_id_map.setdefault(lid, [])
                for child in (item.get("children") or []):
                    c_sc = to_int(child.get("studentsCount") or child.get("totalStudentsCount") or 0)
                    max_students = max(max_students, c_sc)
                    clid = child.get("lessonId") or child.get("id")
                    if clid:
                        lesson_id_map.setdefault(clid, [])

        all_lesson_ids = list(lesson_id_map.keys())

        progress_lists: list[list] = await asyncio.gather(
            *[_fetch_progresses(group_id, lid, token, client, semaphore)
              for lid in all_lesson_ids],
            return_exceptions=True,
        )
        progress_lists = [
            pl if isinstance(pl, list) else []
            for pl in progress_lists
        ]
        progress_cache: dict[str, list] = dict(zip(all_lesson_ids, progress_lists))

        # 4. Count active students
        student_count = _count_active_from_progresses(progress_lists, max_students)

        # 5. Recalc summaries
        fixed_summaries = []
        for sr in summary_responses:
            new_sr = []
            for item in sr:
                lid = item.get("lessonId") or item.get("id")
                progresses = progress_cache.get(lid, []) if lid else []
                new_item = _recalc_item(item, progresses)
                parent_count = to_int(new_item.get("studentsCount") or 0)
                new_children = []
                for child in (item.get("children") or []):
                    clid = child.get("lessonId") or child.get("id")
                    c_progresses = progress_cache.get(clid, []) if clid else []
                    new_children.append(_recalc_item(child, c_progresses, forced_count=parent_count))
                new_item["children"] = new_children
                new_sr.append(new_item)
            fixed_summaries.append(new_sr)

        # 6. Extract metrics
        from utils import normalize
        week_theme_metrics = [
            extract_metrics_fn(sr, normalize(t.get("themeName") or ""))
            for t, sr in zip(valid_themes, fixed_summaries)
        ]
        metrics = merge_metrics_fn(week_theme_metrics) if week_theme_metrics else empty_metrics_fn()
        return metrics, int(student_count)

    async def build_group_all_weeks(group, token, study_month, client, semaphore):
        group_id = group["id"]
        curator = group.get("curator", {})
        curator_name = f"{curator.get('lastname', '')} {curator.get('firstname', '')}".strip()
        course_name = group.get("courseName", "")

        # All 4 weeks in parallel
        week_results = await asyncio.gather(
            _fetch_week_metrics(group_id, 1, study_month, token, client, semaphore),
            _fetch_week_metrics(group_id, 2, study_month, token, client, semaphore),
            _fetch_week_metrics(group_id, 3, study_month, token, client, semaphore),
            _fetch_week_metrics(group_id, 4, study_month, token, client, semaphore),
            return_exceptions=True,
        )

        student_count = 0
        for wr in week_results:
            if isinstance(wr, tuple) and wr[1] > 0:
                student_count = int(wr[1])
                break

        if student_count <= 0:
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

        semaphore = asyncio.Semaphore(GLOBAL_SEMAPHORE_LIMIT)
        done_count = 0

        async with httpx.AsyncClient(limits=CLIENT_LIMITS) as client:

            async def _process_and_track(g):
                nonlocal done_count
                try:
                    result = await build_group_all_weeks(g, token, month_num, client, semaphore)
                finally:
                    done_count += 1
                    PROGRESS[job_id]["done"] = done_count
                return result

            # Launch ALL groups at once — semaphore controls actual concurrency
            all_results = await asyncio.gather(
                *[_process_and_track(g) for g in groups],
                return_exceptions=True,
            )

        results = [r for r in all_results if not isinstance(r, Exception) and r is not None]
        PROGRESS[job_id]["status"] = "done"
        PROGRESS[job_id]["results"] = results

    async def _process_single_course(course, token, study_month, client, semaphore):
        course_id = course["id"]
        course_name = course["name"]
        try:
            groups = await api_get_async(
                f"{BASE_URL}/v1/headteacher/courses/{course_id}/groups",
                token, client,
            )
            groups = [g for g in groups if g.get("prolongCount", 0) >= 3]
            if not groups:
                return None

            # All groups in parallel (semaphore limits concurrency)
            group_results_raw = await asyncio.gather(
                *[build_group_all_weeks(g, token, study_month, client, semaphore) for g in groups],
                return_exceptions=True,
            )
            group_results = [r for r in group_results_raw if not isinstance(r, Exception) and r is not None]

            if not group_results:
                return None
            course_avg = merge_metrics_fn([gr["monthly"] for gr in group_results])
            total_students = sum(gr["base"].get("Оқушы саны", 0) or 0 for gr in group_results)
            return metrics_to_row_fn({"Поток": course_name, "Оқушы саны": total_students}, course_avg)
        except Exception:
            return None

    async def _build_section_report_job(job_id, courses, token, study_month):
        total = len(courses)
        PROGRESS[job_id] = {"total": total, "done": 0, "status": "running", "results": []}
        semaphore = asyncio.Semaphore(GLOBAL_SEMAPHORE_LIMIT)
        done_count = 0

        async with httpx.AsyncClient(limits=CLIENT_LIMITS) as client:

            async def _process_and_track_course(c):
                nonlocal done_count
                try:
                    return await _process_single_course(c, token, study_month, client, semaphore)
                finally:
                    done_count += 1
                    PROGRESS[job_id]["done"] = done_count

            all_results = await asyncio.gather(
                *[_process_and_track_course(c) for c in courses],
                return_exceptions=True,
            )

        results = [r for r in all_results if not isinstance(r, Exception) and r is not None]
        PROGRESS[job_id]["status"] = "done"
        PROGRESS[job_id]["results"] = results

    return _fetch_week_metrics, build_group_all_weeks, _build_report_job
