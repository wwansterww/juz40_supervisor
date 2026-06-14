import asyncio
import httpx
from config import BASE_URL
from cache import api_get_async, get_shared_client
from store import PROGRESS
from concurrency import report_slot


class DataFetchError(Exception):
    """Required report data could not be fetched (after HTTP-level retries).

    Separates "the API failed" from "the API said there is no data". The
    former must surface as a failed report — silently rendering it as zeros
    produces numbers that look plausible and are wrong, which is worse than
    an error the user can retry.
    """

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
    "қолхат",
]

# Score that curators use as a "student left the course" marker. A student who
# got sick or dropped out keeps platform access (so the group still shows e.g.
# 50 students), but the curator marks them with 0.1 балл — usually together
# with a "курстан шыққан" / "қолхат" comment. Such students must be excluded
# from BOTH the numerator and the denominator: 40 submitted of 50 students
# with 3 marked 0.1 → 40/47, not 40/50.
LEFT_MARKER_SCORE = 0.1


# ── Helpers ────────────────────────────────────────────────────────────────────

def _has_left_marker_score(progress: dict) -> bool:
    score = progress.get("score")
    if score is None:
        return False
    try:
        # The API may return 0.1 as a float or "0.1"/"0,1" as a string.
        val = float(str(score).replace(",", "."))
    except (ValueError, TypeError):
        return False
    return abs(val - LEFT_MARKER_SCORE) < 1e-9


def is_left_course(progress: dict) -> bool:
    if _has_left_marker_score(progress):
        return True
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


def is_submitted(progress: dict, include_zero_score: bool = False) -> bool:
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
    # Curator-graded rows: the student didn't trip any of the submission
    # markers above, but a curator manually entered a score. We treat that
    # as "submitted" so our percentages match what curators see on the
    # platform UI ("Бағаланды" column). Score == 0 is excluded by default
    # because the platform uses 0 as a placeholder for "no work" — except
    # in themes like САБАҚ ТАПСЫРУ / ҚАЙТАЛАУ ТЕСТ where 0 is a real grade
    # (caller passes include_zero_score=True for those).
    score = progress.get("score")
    if score is not None and (include_zero_score or score != 0):
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
# A 404 is legitimate "this data doesn't exist" → empty list. Anything else
# (timeouts, 5xx after all retries in api_get_async) raises DataFetchError —
# returning [] there would silently count every student as "didn't submit".

async def _fetch_summary(group_id, theme_id, token, client, semaphore):
    async with semaphore:
        try:
            data = await api_get_async(
                f"{BASE_URL}/v3/headteacher/groups/{group_id}/themes/{theme_id}/lessons/summary",
                token, client,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return []
            raise DataFetchError(f"summary theme={theme_id}") from exc
        except Exception as exc:
            raise DataFetchError(f"summary theme={theme_id}") from exc
        return data if isinstance(data, list) else []


async def _fetch_progresses(group_id, lesson_id, token, client, semaphore):
    async with semaphore:
        try:
            data = await api_get_async(
                f"{BASE_URL}/v2/headteacher/groups/{group_id}/lessons/{lesson_id}/progresses",
                token, client,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return []
            raise DataFetchError(f"progresses lesson={lesson_id}") from exc
        except Exception as exc:
            raise DataFetchError(f"progresses lesson={lesson_id}") from exc
        return data if isinstance(data, list) else []


# ── Progress recalc ────────────────────────────────────────────────────────────

def _lesson_left_ids(progresses: list, week_left_ids: set) -> set:
    """Students of THIS lesson who are marked as left — either directly on
    this lesson (comment / 0.1-балл marker) or anywhere else this week.

    A curator usually marks a left student on ONE lesson, not on every
    lesson. week_left_ids carries those students across the whole week so
    they're excluded from every lesson's numerator and denominator, not
    just the lesson that has the marker."""
    out = set()
    for p in progresses:
        sid = get_student_id(p)
        if not sid:
            continue
        if sid in week_left_ids or is_left_course(p):
            out.add(sid)
    return out


def _recalc_item(item: dict, progresses: list, left_ids: set,
                 forced_count: int = None,
                 include_zero_score: bool = False,
                 already_excluded: set | None = None) -> dict:
    # When forced_count comes from a parent lesson it has the parent's left
    # students already subtracted. Subtract only the ones the parent didn't
    # know about, otherwise the same student is removed twice.
    newly_left = left_ids - (already_excluded or set())

    old_count = to_int(item.get("studentsCount") or item.get("totalStudentsCount") or 0)
    new_count = max(0, (forced_count if forced_count is not None else old_count) - len(newly_left))

    submitted = 0
    scores = []
    for p in progresses:
        if get_student_id(p) in left_ids or is_left_course(p):
            continue
        if is_submitted(p, include_zero_score=include_zero_score):
            submitted += 1
            score = p.get("score")
            if score is not None and (include_zero_score or score != 0):
                scores.append(score)

    new_item = dict(item)
    new_item["studentsCount"] = new_count
    new_item["totalStudentsCount"] = new_count
    new_item["submittedCount"] = submitted
    new_item["reviewedCount"] = submitted
    new_item["notSubmittedCount"] = max(0, new_count - submitted)
    new_item["averageScore"] = (sum(scores) / len(scores)) if scores else None
    return new_item


def _collect_left_ids(all_progresses: list[list]) -> set:
    """All students marked as left (comment phrase or 0.1-балл marker) on
    ANY lesson in the given progress lists."""
    left_ids: set = set()
    for progresses in all_progresses:
        for p in progresses:
            if is_left_course(p):
                sid = get_student_id(p)
                if sid:
                    left_ids.add(sid)
    return left_ids


def _count_active_from_progresses(all_progresses: list[list], max_students: int) -> int:
    return max(0, max_students - len(_collect_left_ids(all_progresses)))


# ── Parallel paginated course loader ──────────────────────────────────────────

async def fetch_all_pages(base_url: str, token: str, client: httpx.AsyncClient) -> list:
    """
    Fetches the first page of *base_url* (must include ?page=0 or &page=0),
    then fetches remaining pages in parallel.

    Raises if any page fails: a silently shorter list is indistinguishable
    from a complete one, and everything downstream would be quietly wrong.
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
    )
    for r in results:
        content.extend(r.get("content", []))
    return content


# ── Theme classification ───────────────────────────────────────────────────────
# Themes where a score of 0 is a legitimate grade (zero counts toward the
# class average — e.g. САБАҚ ТАПСЫРУ where 0/30 means "submitted and got 0",
# not "didn't submit"). For everything else, 0 is treated as a placeholder
# and excluded from averages.
#
# Both spellings of "ҚАЙТАЛАУ ТЕСТ" are listed — curators sometimes type the
# theme name with a typo (ҚАЙТАЛУ, missing the second А), and the system has
# to handle both forms identically so the wrong spelling doesn't silently
# break score handling.
_ZERO_SCORE_THEME_KEYWORDS = frozenset({
    "САБАҚ ТАПСЫРУ", "ҚАЙТАЛАУ ТЕСТ", "ҚАЙТАЛУ ТЕСТ",
})

# NOTE: dead detection helpers (_is_quiz_theme, _is_homework_theme,
# _QUIZ_KEYWORDS, _HOMEWORK_KEYWORDS) were removed — they had no callers
# anywhere in the codebase. Quiz-theme matching now lives in
# subjects.common.is_quiz_theme (which also handles the Latin/Cyrillic
# normalization correctly).


# ── Group activity check ───────────────────────────────────────────────────────

async def _is_group_active(group_id: str, month: int, token: str, client: httpx.AsyncClient) -> bool:
    try:
        data = await api_get_async(
            f"{BASE_URL}/v3/headteacher/groups/{group_id}/students?month={month}",
            token, client,
        )
        students = data.get("students", []) if isinstance(data, dict) else []
        return len(students) > 1
    except Exception:
        return True


# ── Core builder ───────────────────────────────────────────────────────────────

def make_builder(extract_metrics_fn, merge_metrics_fn, empty_metrics_fn, metrics_to_row_fn=None):

    async def _fetch_week_metrics(group_id, week, study_month, token, client, semaphore):
        # 1. Load week themes. 404 = "the week doesn't exist for this group"
        #    (legitimately empty); any other failure must NOT be rendered as
        #    an empty week — it propagates and fails the group loudly.
        try:
            resp = await api_get_async(
                f"{BASE_URL}/v1/headteacher/groups/{group_id}/themes?week={week}&month={study_month}",
                token, client,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return empty_metrics_fn(), 0
            raise DataFetchError(f"themes group={group_id} week={week}") from exc
        except DataFetchError:
            raise
        except Exception as exc:
            raise DataFetchError(f"themes group={group_id} week={week}") from exc

        themes = resp.get("themes", [])
        valid_themes = [t for t in themes if t.get("themeId")]
        if not valid_themes:
            return empty_metrics_fn(), 0

        # 2. Load all summaries in parallel. A failed summary raises
        #    DataFetchError out of the gather — no silent empty lists.
        summary_responses: list[list] = await asyncio.gather(
            *[_fetch_summary(group_id, t["themeId"], token, client, semaphore)
              for t in valid_themes],
        )

        # 3. Collect all lesson_ids across all themes
        max_students = 0
        all_lesson_ids: list[str] = []
        seen_ids: set[str] = set()

        for sr in summary_responses:
            for item in sr:
                sc = to_int(item.get("studentsCount") or item.get("totalStudentsCount") or 0)
                max_students = max(max_students, sc)
                lid = item.get("lessonId") or item.get("id")
                if lid and lid not in seen_ids:
                    seen_ids.add(lid)
                    all_lesson_ids.append(lid)
                for child in (item.get("children") or []):
                    c_sc = to_int(child.get("studentsCount") or child.get("totalStudentsCount") or 0)
                    max_students = max(max_students, c_sc)
                    clid = child.get("lessonId") or child.get("id")
                    if clid and clid not in seen_ids:
                        seen_ids.add(clid)
                        all_lesson_ids.append(clid)

        # 4. Fetch progresses for ALL lessons.
        #    Redis caches results for 30 min, so after the first run subsequent
        #    users hit cache and this is fast. Full fetch ensures accurate
        #    submitted counts with left students properly excluded everywhere.
        #    A failed fetch raises DataFetchError — a lesson with silently
        #    missing progresses would show every student as not submitted.
        progress_lists: list[list] = await asyncio.gather(
            *[_fetch_progresses(group_id, lid, token, client, semaphore)
              for lid in all_lesson_ids],
        )
        progress_cache: dict[str, list] = dict(zip(all_lesson_ids, progress_lists))

        # 4b. Collect students who left (0.1-балл marker or comment on ANY
        #     lesson this week) and count the remaining active students.
        week_left_ids = _collect_left_ids(progress_lists)
        student_count = max(0, max_students - len(week_left_ids))

        # 5. Recalc all summaries with full progress data.
        #    _recalc_item corrects both studentsCount and submittedCount,
        #    excluding "шыққан оқушы" from numerator AND denominator.
        fixed_summaries = []
        for t, sr in zip(valid_themes, summary_responses):
            theme_upper = (t.get("themeName") or "").upper()
            inc_zero = any(kw in theme_upper for kw in _ZERO_SCORE_THEME_KEYWORDS)
            new_sr = []
            for item in sr:
                lid = item.get("lessonId") or item.get("id")
                progresses = progress_cache.get(lid, []) if lid else []
                parent_left = _lesson_left_ids(progresses, week_left_ids)
                new_item = _recalc_item(item, progresses, parent_left,
                                        include_zero_score=inc_zero)
                parent_count = to_int(new_item.get("studentsCount") or 0)
                new_children = []
                for child in (item.get("children") or []):
                    clid = child.get("lessonId") or child.get("id")
                    c_progresses = progress_cache.get(clid, []) if clid else []
                    c_left = _lesson_left_ids(c_progresses, week_left_ids)
                    new_children.append(_recalc_item(child, c_progresses, c_left,
                                                     forced_count=parent_count,
                                                     include_zero_score=inc_zero,
                                                     already_excluded=parent_left))
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

    async def build_group_all_weeks(group, token, study_month, client, semaphore, week_filter=None):
        """Build per-week + monthly metrics for one group.

        If ``week_filter`` is None (default) all 4 weeks are fetched and a
        monthly aggregate is computed — the original behaviour. If a single
        week number (1..4) is passed, only that week's API calls are made,
        and the unfetched weeks are filled with empty metrics. This makes
        single-week reports ~4× faster: each week's themes/summaries/
        progresses are an independent fan-out, so skipping 3 of them
        proportionally cuts the network work.
        """
        group_id = group["id"]
        curator = group.get("curator", {})
        curator_name = f"{curator.get('lastname', '')} {curator.get('firstname', '')}".strip()
        course_name = group.get("courseName", "")

        # Try the month-scoped students endpoint first (authoritative for the
        # study month). Some groups (especially fresh VIP courses) return an
        # empty list here even though the group definitely has students —
        # which used to drop the entire group from the report, making the
        # whole VIP section disappear. Fall back to the `studentCount` that
        # the groups-list endpoint already gave us so the curator still shows
        # up with their real student count.
        try:
            students_data = await api_get_async(
                f"{BASE_URL}/v3/headteacher/groups/{group_id}/students?month={study_month}",
                token, client,
            )
            student_count = len(students_data.get("students", [])) if isinstance(students_data, dict) else 0
        except Exception:
            student_count = 0

        if student_count <= 0:
            # Fallback: trust the parent /groups response if it included a count.
            fallback = group.get("studentCount") or group.get("studentsCount") or 0
            try:
                student_count = int(fallback)
            except Exception:
                student_count = 0

        if student_count <= 0:
            return None

        # Decide which weeks to actually hit the API for. When the caller
        # only needs one week we skip the others entirely — that's the
        # whole point of the week_filter speed-up.
        weeks_to_fetch = [week_filter] if week_filter in (1, 2, 3, 4) else [1, 2, 3, 4]

        async def _week_with_retry(w):
            # api_get_async already retried each individual request; reaching
            # here means a sustained failure. One more pass over the whole
            # week (cache holds everything that DID succeed, so the retry
            # only re-fetches what failed) rides out short API outages.
            try:
                return await _fetch_week_metrics(group_id, w, study_month, token, client, semaphore)
            except DataFetchError:
                await asyncio.sleep(1.0)
                return await _fetch_week_metrics(group_id, w, study_month, token, client, semaphore)

        # No return_exceptions: a week that still fails after retries must
        # fail the whole group — rendering it as an empty week would show
        # plausible-looking wrong percentages.
        week_results = await asyncio.gather(
            *[_week_with_retry(w) for w in weeks_to_fetch],
        )

        base = {"Поток": course_name, "Куратор": curator_name, "Оқушы саны": student_count}

        weeks_data = {}
        all_week_metrics = []
        for w, wr in zip(weeks_to_fetch, week_results):
            metrics, _ = wr
            weeks_data[w] = metrics
            if any(v is not None for v in metrics.values()):
                all_week_metrics.append(metrics)

        # Weeks we deliberately skipped still need a slot in the dict so the
        # template loops don't KeyError. They'll just be all-None metrics
        # and the result handler can decide whether to render them at all.
        for w in (1, 2, 3, 4):
            weeks_data.setdefault(w, empty_metrics_fn())

        monthly = merge_metrics_fn(all_week_metrics) if all_week_metrics else empty_metrics_fn()
        return {"base": base, "weeks": weeks_data, "monthly": monthly}

    async def _build_report_job(job_id, groups, token, month_num, week_filter=None):
        # Seed progress IMMEDIATELY (no awaits before this!) so the client's
        # first poll never 404s.
        PROGRESS[job_id] = {"total": 0, "done": 0, "status": "queued", "results": []}

        try:
            async with report_slot(job_id):
                PROGRESS[job_id]["status"] = "running"

                # One shared keep-alive client for everything (see cache.py):
                # per-job clients paid the TLS-handshake storm on every report,
                # which under many concurrent users is what knocks APIs over.
                client = get_shared_client()

                active_flags = await asyncio.gather(
                    *[_is_group_active(g["id"], month_num, token, client) for g in groups],
                    return_exceptions=True,
                )
                groups_active = [
                    g for g, active in zip(groups, active_flags)
                    if active is True
                ]

                total = len(groups_active)
                PROGRESS[job_id]["total"] = total

                semaphore = asyncio.Semaphore(GLOBAL_SEMAPHORE_LIMIT)
                done_count = 0

                async def _process_and_track(g):
                    nonlocal done_count
                    try:
                        return await build_group_all_weeks(
                            g, token, month_num, client, semaphore,
                            week_filter=week_filter,
                        )
                    finally:
                        done_count += 1
                        PROGRESS[job_id]["done"] = done_count

                # Launch ALL groups at once — semaphore controls actual concurrency
                outcomes = list(await asyncio.gather(
                    *[_process_and_track(g) for g in groups_active],
                    return_exceptions=True,
                ))

                # Second pass over failed groups only. Everything that DID
                # succeed sits in cache, so this re-fetches just the broken
                # parts — cheap, and it rides out transient API trouble.
                failed_idx = [i for i, r in enumerate(outcomes) if isinstance(r, BaseException)]
                if failed_idx:
                    retried = await asyncio.gather(
                        *[build_group_all_weeks(
                              groups_active[i], token, month_num, client,
                              semaphore, week_filter=week_filter)
                          for i in failed_idx],
                        return_exceptions=True,
                    )
                    for i, r in zip(failed_idx, retried):
                        outcomes[i] = r

                still_failed = sum(1 for r in outcomes if isinstance(r, BaseException))
                if still_failed:
                    # All-or-nothing: a report missing N groups looks complete
                    # and lies. Fail loudly — a re-run is cheap because all
                    # successfully fetched data is already cached.
                    PROGRESS[job_id]["error"] = (
                        f"{still_failed} топ бойынша деректер жүктелмеді. "
                        f"Қайталап көріңіз — жүктелген бөлігі кэште сақталды."
                    )
                    PROGRESS[job_id]["status"] = "failed"
                    return

                results = [r for r in outcomes if r is not None]
                PROGRESS[job_id]["status"] = "done"
                PROGRESS[job_id]["results"] = results
        except Exception:
            # Don't crash the asyncio task with an unhandled exception — leave
            # a failed status so the client UI can show an error and move on.
            PROGRESS[job_id]["status"] = "failed"
            raise

    async def _process_single_course(course, token, study_month, client, semaphore):
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
        # Seed progress IMMEDIATELY (no awaits before this) — avoids 404s.
        PROGRESS[job_id] = {"total": len(courses), "done": 0, "status": "queued", "results": []}

        try:
            async with report_slot(job_id):
                PROGRESS[job_id]["status"] = "running"
                semaphore = asyncio.Semaphore(GLOBAL_SEMAPHORE_LIMIT)
                done_count = 0

                async with httpx.AsyncClient(limits=CLIENT_LIMITS) as client:

                    async def _process_and_track_course(c):
                        nonlocal done_count
                        try:
                            return await _process_single_course(c, token, study_month, client, semaphore)
                        except Exception:
                            return None
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
        except Exception:
            PROGRESS[job_id]["status"] = "failed"
            raise

    return _fetch_week_metrics, build_group_all_weeks, _build_report_job
