"""
СМАРТ айлық СТ есебі — builder.

For a chosen category (esep / auyzsha) and a set of (stream_month, study_month)
selections, fetch the САБАҚ ТАПСЫРУ average score per subject, per week (1..4),
per stream — aggregated across all SMART/EXPRESS/INTENSIVE courses and groups.

The numbers are the platform's raw СТ averageScore (no rescaling): weeks 1–3
are graded out of 15, week 4 out of 20, and we report exactly what the API
returns. Assembly into the ОРТАҚ / БӨЛЕК tables happens in routes.py — the
builder only produces, per (subject, stream), the four weekly scores and the
total student count.
"""

import asyncio

import httpx

from config import BASE_URL
from cache import api_get_async, get_shared_client
from store import PROGRESS
from concurrency import report_slot
from subjects.base_builder import GLOBAL_SEMAPHORE_LIMIT, DataFetchError, to_int
from subjects.common import is_kaitalau_test
from utils import normalize
from subjects.smart_monthly.constants import (
    SMART_PRODUCTS, subjects_for_category, stream_position,
)

# This report needs exactly ONE metric — the САБАҚ ТАПСЫРУ average score — so
# it does NOT reuse build_group_all_weeks (which fetches every theme and the
# per-lesson progresses for all of them: dozens-to-hundreds of calls a group).
# Instead we fetch only the СТ theme's summary and read its averageScore. That
# is ~10× fewer requests. Trade-off: this uses the platform's raw averageScore,
# which still includes any 0.1-балл "курстан шыққан" students — the heavyweight
# path excluded those. For an aggregate monthly score the effect is small; if
# exact exclusion is needed later, fetch progresses for the СТ lessons only.

# Theme-name marker (compared after utils.normalize, so it's pure Cyrillic).
_SABAK_MARKER = "САБАҚ ТАПСЫРУ"


# ── Course discovery ──────────────────────────────────────────────────────────

async def _list_smart_courses(subject_id, stream_month, token, client):
    """All SMART-umbrella courses for one subject at one stream month.

    404 on a product → that product simply has no courses; any other failure
    raises DataFetchError so the job fails loudly instead of silently missing
    a chunk of courses.
    """
    urls = [
        f"{BASE_URL}/v2/headteacher/subjects/{subject_id}/courses"
        f"?size=200&page=0&searchWord=&sort=year,DESC&sort=month,DESC"
        f"&product={p}&month={stream_month}"
        for p in SMART_PRODUCTS
    ]
    responses = await asyncio.gather(
        *[api_get_async(u, token, client) for u in urls],
    )
    courses = []
    for resp in responses:
        courses.extend(resp.get("content", []))

    if not courses:
        return []

    # Keep only the most recent year, drop copies — matches the раздел report.
    latest_year = max((c.get("year") or 0) for c in courses)
    return [
        c for c in courses
        if (c.get("year") or 0) == latest_year
        and "(КОПИЯ" not in (c.get("name") or "").upper()
    ]


async def _fetch_groups(course_id, token, client):
    data = await api_get_async(
        f"{BASE_URL}/v1/headteacher/courses/{course_id}/groups",
        token, client,
    )
    return data if isinstance(data, list) else []


# ── Lightweight СТ-only fetch (no progresses, no other themes) ─────────────────

async def _get(url, token, client, semaphore):
    """One cached GET. 404 → None (legitimately absent); anything else after
    api_get_async's own retries → DataFetchError so the job fails loudly."""
    async with semaphore:
        try:
            return await api_get_async(url, token, client)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None
            raise DataFetchError(url) from exc
        except DataFetchError:
            raise
        except Exception as exc:
            raise DataFetchError(url) from exc


def _theme_sabak_score(items: list, is_sabak: bool):
    """Average САБАҚ ТАПСЫРУ score for one theme's summary, replicating the
    informatics extract_metrics logic: for a real СТ theme skip ҚЖ items and
    child rows (parentId set); for a ҚАЙТАЛАУ ТЕСТ fallback take every item."""
    scores = []
    for it in items:
        if is_sabak:
            name = (it.get("name") or "").upper()
            if "ҚЖ" in name or it.get("parentId") is not None:
                continue
        score = it.get("averageScore")
        if score is not None:
            scores.append(score)
    if not scores:
        return None
    return sum(scores) / len(scores)


async def _group_sabak_weeks(group_id, study_month, token, client, semaphore):
    """СТ score for weeks 1..4 of one group + that group's СТ class size.

    Per week: fetch the week's theme list, keep only the САБАҚ ТАПСЫРУ theme
    (or ҚАЙТАЛАУ ТЕСТ as fallback), fetch ONLY those themes' summaries, read
    averageScore. No progresses, no other themes.
    """
    max_students = 0

    async def _week(week):
        nonlocal max_students
        themes_resp = await _get(
            f"{BASE_URL}/v1/headteacher/groups/{group_id}/themes?week={week}&month={study_month}",
            token, client, semaphore,
        )
        themes = (themes_resp or {}).get("themes", []) if isinstance(themes_resp, dict) else []

        targets = []  # (theme_id, is_sabak)
        for t in themes:
            tid = t.get("themeId")
            if not tid:
                continue
            name = normalize(t.get("themeName") or "")
            if _SABAK_MARKER in name:
                targets.append((tid, True))
            elif is_kaitalau_test(name):
                targets.append((tid, False))
        if not targets:
            return None
        # Prefer real СТ themes; only fall back to ҚАЙТАЛАУ ТЕСТ if none exist.
        if any(is_s for _, is_s in targets):
            targets = [(tid, is_s) for tid, is_s in targets if is_s]

        summaries = await asyncio.gather(*[
            _get(f"{BASE_URL}/v3/headteacher/groups/{group_id}/themes/{tid}/lessons/summary",
                 token, client, semaphore)
            for tid, _ in targets
        ])

        per_theme = []
        for (tid, is_sabak), summ in zip(targets, summaries):
            items = summ if isinstance(summ, list) else []
            for it in items:
                max_students = max(max_students, to_int(
                    it.get("studentsCount") or it.get("totalStudentsCount") or 0))
            score = _theme_sabak_score(items, is_sabak)
            if score is not None:
                per_theme.append(score)
        if not per_theme:
            return None
        return sum(per_theme) / len(per_theme)

    week_vals = await asyncio.gather(*[_week(w) for w in (1, 2, 3, 4)])
    weeks = {w: (round(v, 2) if v is not None else None)
             for w, v in zip((1, 2, 3, 4), week_vals)}
    return weeks, max_students


# ── Per (subject, stream) aggregation ─────────────────────────────────────────

def _weighted(scores_weights) -> float | None:
    """Student-count-weighted mean of (score, weight) pairs, skipping None
    scores. Weight falls back to 1 so a group with an unknown count still
    counts once rather than vanishing."""
    num = 0.0
    den = 0.0
    for score, weight in scores_weights:
        if score is None:
            continue
        w = weight if weight and weight > 0 else 1
        num += float(score) * w
        den += w
    if den == 0:
        return None
    return round(num / den, 2)


async def _subject_stream(subject, stream_month, study_month, token, client, semaphore):
    """СТ scores for one subject in one stream, weeks 1..4 + total students."""
    courses = await _list_smart_courses(subject.subject_id, stream_month, token, client)

    group_lists = await asyncio.gather(
        *[_fetch_groups(c["id"], token, client) for c in courses],
    )
    all_groups = [g for groups in group_lists for g in groups]

    group_results = await asyncio.gather(
        *[_group_sabak_weeks(g["id"], study_month, token, client, semaphore)
          for g in all_groups],
    )
    # Each result is (weeks_dict, student_count); keep groups with any data.
    group_results = [gr for gr in group_results if gr is not None and gr[1] > 0]

    weeks: dict[int, float | None] = {}
    for w in (1, 2, 3, 4):
        pairs = [(gw.get(w), students) for gw, students in group_results]
        weeks[w] = _weighted(pairs)

    total_students = sum(students for _, students in group_results)

    return {
        "abbr":         subject.abbr,
        "slug":         subject.slug,
        "subgroup":     subject.subgroup,
        "stream_month": stream_month,
        "study_month":  study_month,
        "position":     stream_position(stream_month),
        "weeks":        weeks,           # {1:score|None, ... 4:...}
        "students":     total_students,
    }


# ── Job entry point ───────────────────────────────────────────────────────────

async def build_smart_monthly_job(job_id, category, selections, token):
    """
    selections: [{"stream_month": int, "study_month": int}, ...]
    Stores PROGRESS[job_id]["results"] = [ _subject_stream(...) dicts ].
    Progress ticks once per (subject, selection) pair.
    """
    subjects = subjects_for_category(category)

    PROGRESS[job_id] = {
        "total":      len(subjects) * len(selections),
        "done":       0,
        "status":     "queued",
        "results":    [],
        "category":   category,
        "selections": selections,
    }

    try:
        async with report_slot(job_id):
            PROGRESS[job_id]["status"] = "running"

            client = get_shared_client()
            semaphore = asyncio.Semaphore(GLOBAL_SEMAPHORE_LIMIT)
            done = 0

            # (subject, selection) work items
            work = [
                (subj, sel)
                for subj in subjects
                for sel in selections
            ]

            async def _track(subj, sel):
                nonlocal done
                try:
                    # One retry: everything that loaded is cached, so the retry
                    # only re-fetches the broken part. Still failing → propagate.
                    try:
                        return await _subject_stream(
                            subj, sel["stream_month"], sel["study_month"],
                            token, client, semaphore,
                        )
                    except DataFetchError:
                        await asyncio.sleep(1.0)
                        return await _subject_stream(
                            subj, sel["stream_month"], sel["study_month"],
                            token, client, semaphore,
                        )
                finally:
                    done += 1
                    PROGRESS[job_id]["done"] = done

            outcomes = list(await asyncio.gather(
                *[_track(s, sel) for (s, sel) in work],
                return_exceptions=True,
            ))

            failed = [i for i, r in enumerate(outcomes) if isinstance(r, BaseException)]
            if failed:
                # Second pass over only the failed items (warm cache makes it cheap).
                retried = await asyncio.gather(
                    *[_track(work[i][0], work[i][1]) for i in failed],
                    return_exceptions=True,
                )
                for i, r in zip(failed, retried):
                    outcomes[i] = r

            still_failed = sum(1 for r in outcomes if isinstance(r, BaseException))
            if still_failed:
                PROGRESS[job_id]["error"] = (
                    f"{still_failed} блок бойынша деректер жүктелмеді. "
                    f"Қайталап көріңіз — жүктелген бөлігі кэште сақталды."
                )
                PROGRESS[job_id]["status"] = "failed"
                return

            PROGRESS[job_id]["results"] = [r for r in outcomes if r is not None]
            PROGRESS[job_id]["status"] = "done"
    except Exception:
        PROGRESS[job_id].setdefault(
            "error",
            "Деректерді жүктеу кезінде қате шықты. Қайталап көріңіз.",
        )
        PROGRESS[job_id]["status"] = "failed"
        raise
