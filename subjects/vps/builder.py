"""
VPS combined-report builder.

A "VPS pack" (e.g. ИНФО-МАТ) is a multi-subject cohort. For each pack we
fetch data across **all three тариф levels** (VIP / PREM / STAN) and across
**all five constituent subjects** (МАТ, ИНФО, ГЕО, МС, ТАРИХ) — that's up
to 15 separate juz40-edu.kz courses, fetched in parallel.

The result is grouped two ways:
  • by week 1..4 (and a monthly summary) — used for tabs in the UI;
  • by subject suffix, then by тариф — used for the table sections.

Per-subject metric computation is delegated to the existing subject builders,
so the report layer stays a thin orchestrator.
"""

import asyncio
import httpx

from config import (
    BASE_URL,
    VPS_SUFFIX_TO_SUBJECT,
    VPS_PACKS,
    VPS_PRODUCTS,
    VPS_DEFAULT_MONTH,
)
from cache import api_get_async, get_shared_client
from store import PROGRESS
from concurrency import report_slot
from subjects.base_builder import GLOBAL_SEMAPHORE_LIMIT, DataFetchError

# VPS reports do ~15x the work of a single-subject report (5 subjects × 3
# tariffs in one job), but they still pass through the same process-wide
# API_SEM (GLOBAL_API_LIMIT = 250 in concurrency.py). The per-report cap
# was the bottleneck: 50 parallel calls meant a job that needs ~5-8k calls
# took 100+ "waves" to drain. Bumping this to 200 lets a single VPS job
# use most of the process-wide budget when it's the only one running,
# while still leaving headroom for other reports / UI calls.
VPS_SEMAPHORE_LIMIT = 200

# Reuse each subject's build_group_all_weeks. They are async functions that
# take one group dict and return {"base": {...}, "weeks": {1..4: metrics},
# "monthly": {...}}. We delegate per-subject metric extraction to them.
from subjects.math.builder        import build_group_all_weeks as _build_math
from subjects.informatics.builder import build_group_all_weeks as _build_info
from subjects.geometry.builder    import build_group_all_weeks as _build_geom
from subjects.ms.builder          import build_group_all_weeks as _build_ms
from subjects.history.builder     import build_group_all_weeks as _build_history


BUILDER_BY_SUFFIX = {
    "МАТ":   _build_math,
    "ИНФО":  _build_info,
    "ГЕО":   _build_geom,
    "МС":    _build_ms,
    "ТАРИХ": _build_history,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _list_subject_courses(subject_id, product_key, month_num, token, client):
    """Raw course list for one (subject, product, month).

    404 → [] (legitimately nothing there); any other failure raises
    DataFetchError — an empty list on error would silently drop a whole
    subject section from the report."""
    url = (
        f"{BASE_URL}/v2/headteacher/subjects/{subject_id}/courses"
        f"?size=100&page=0&searchWord=&sort=year,DESC&sort=month,DESC"
        f"&product={product_key}&month={month_num}"
    )
    try:
        data = await api_get_async(url, token, client)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return []
        raise DataFetchError(f"vps courses {product_key}") from exc
    except Exception as exc:
        raise DataFetchError(f"vps courses {product_key}") from exc
    return data.get("content") or []


async def _fetch_groups(course_id, token, client):
    """Raw group list for a course. 404 → []; other failures raise."""
    try:
        data = await api_get_async(
            f"{BASE_URL}/v1/headteacher/courses/{course_id}/groups",
            token, client,
        )
        return data if isinstance(data, list) else []
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return []
        raise DataFetchError(f"vps groups course={course_id}") from exc
    except Exception as exc:
        raise DataFetchError(f"vps groups course={course_id}") from exc


async def _build_one_subject_product(
    suffix, product, pack_name, stream_month, study_month, token, client, semaphore,
    week_filter=None,
):
    """Fetch + build data for one (suffix, product) combination of a pack.

    Two distinct month concepts are involved (matches how SMART works):

    • ``stream_month`` — the cohort/stream month (when students enrolled).
      This filters the courses-list endpoint to find the right pack course
      (e.g. SMART VIP ИНФО-МАТ МАТ exists at stream_month=2). The platform
      currently only has VPS cohorts at VPS_DEFAULT_MONTH, but keeping this
      a parameter makes it easy to support future cohorts.

    • ``study_month`` — which month of study to pull metrics for (1..5).
      This is what the user picks on the dashboard. Passed straight through
      to each subject's per-week builder, where it ends up as ``?month=N``
      on the themes/lessons endpoints.

    Returns a dict shaped like:
        {
          "suffix":        "МАТ",
          "label":         "МАТ",
          "product_key":   "SMART_VIP",
          "product_label": "VIP",
          "course_name":   "SMART VIP ИНФО-МАТ МАТ",
          "stream_name":   "SMART VIP ИНФО-МАТ 2026",
          "groups": [...],
        }
    Always returns something — `groups` will be empty if the course / data
    couldn't be fetched, so the UI can still render an empty section.
    """
    base_skeleton = {
        "suffix":        suffix,
        "product_key":   product["key"],
        "product_label": product["label"],
        "label":         VPS_SUFFIX_TO_SUBJECT.get(suffix, {}).get("label", suffix),
        "course_name":   "",
        "stream_name":   "",
        "groups":        [],
    }

    suffix_info = VPS_SUFFIX_TO_SUBJECT.get(suffix)
    builder_fn  = BUILDER_BY_SUFFIX.get(suffix)
    if not suffix_info or not builder_fn:
        return base_skeleton

    # Course discovery uses stream_month (when the cohort enrolled), NOT the
    # user's study_month pick. Mixing them would silently make the report
    # empty whenever the user picks a study month different from the cohort
    # month — which is the normal case (Feb cohort studying in April).
    courses = await _list_subject_courses(
        suffix_info["subject_id"], product["key"], stream_month, token, client,
    )

    # Find the course whose name contains the pack tag (ИНФО-МАТ, ГЕО-МАТ, …).
    # Course names look like "SMART VIP ИНФО-МАТ МАТ" — uppercased substring match.
    pack_upper = pack_name.upper()
    course = None
    for c in courses:
        if pack_upper in (c.get("name") or "").upper():
            course = c
            break
    if not course:
        return base_skeleton

    base_skeleton["course_name"] = course.get("name", "")
    base_skeleton["stream_name"] = course.get("streamName", "")

    groups_raw = await _fetch_groups(course["id"], token, client)
    # Drop groups without a real curator (system/default stream groups have
    # curator.id == None).
    groups_raw = [g for g in groups_raw if g.get("curator", {}).get("id")]

    # Reuse the subject's existing builder for each group. Internal API_SEM +
    # the local semaphore cap parallel HTTP load. When week_filter is set,
    # each group's builder only fetches that one week (≈4× faster).
    # study_month (not stream_month) is what the per-week builder needs.
    # No return_exceptions: a group whose data failed to load must fail the
    # job loudly, not silently vanish from its тариф section.
    results = await asyncio.gather(
        *[builder_fn(g, token, study_month, client, semaphore, week_filter=week_filter)
          for g in groups_raw],
    )
    base_skeleton["groups"] = [r for r in results if r is not None]
    return base_skeleton


# ── Main entry point ──────────────────────────────────────────────────────────

async def build_vps_report_job(
    job_id, pack_name, study_month, token,
    week_filter=None, stream_month=None,
):
    """Async task: build combined VPS report for a pack across all 3 тарифs.

    Progress ticks once per (subject, product) pair processed — so for the
    ИНФО-МАТ pack the counter goes 0 → 15 (5 subjects × 3 products).

    ``study_month`` (1..5) is the user's "оқу айы" pick — it controls which
    month's lesson data is fetched. ``stream_month`` (defaults to
    VPS_DEFAULT_MONTH) is the cohort/intake month used to find the right
    courses on the platform.

    When ``week_filter`` is 1..4, every per-group fetch only hits that one
    week's API endpoints. The result is the same shape as a full report —
    other weeks' metrics are just all-None — and the route layer is
    responsible for hiding those empty tabs in the rendered view.
    """
    if stream_month is None:
        stream_month = VPS_DEFAULT_MONTH

    suffixes = VPS_PACKS.get(pack_name, [])
    products = VPS_PRODUCTS

    # Seed progress synchronously so polling never 404s.
    PROGRESS[job_id] = {
        "total":        len(suffixes) * len(products),
        "done":         0,
        "status":       "queued",
        "results":      [],
        "pack_name":    pack_name,
        "month_num":    study_month,   # kept for template back-compat
        "study_month":  study_month,
        "stream_month": stream_month,
        "week_filter":  week_filter,
    }

    try:
        async with report_slot(job_id):
            PROGRESS[job_id]["status"] = "running"

            semaphore  = asyncio.Semaphore(VPS_SEMAPHORE_LIMIT)
            done_count = 0

            # Shared keep-alive client (pool = GLOBAL_API_LIMIT, see cache.py)
            # instead of a per-job client — connection reuse matters most on
            # exactly this, the heaviest job in the app.
            client = get_shared_client()

            async def _track(suffix, product):
                nonlocal done_count
                try:
                    # One retry per (subject × тариф): everything that DID
                    # load is cached, so the retry only re-fetches what
                    # failed. If it still fails, the error propagates and
                    # the job is marked failed — an empty section would
                    # silently misreport a whole subject.
                    try:
                        return await _build_one_subject_product(
                            suffix, product, pack_name,
                            stream_month, study_month,
                            token, client, semaphore,
                            week_filter=week_filter,
                        )
                    except DataFetchError:
                        await asyncio.sleep(1.0)
                        return await _build_one_subject_product(
                            suffix, product, pack_name,
                            stream_month, study_month,
                            token, client, semaphore,
                            week_filter=week_filter,
                        )
                finally:
                    done_count += 1
                    PROGRESS[job_id]["done"] = done_count

            # 5 subjects × 3 products in parallel
            tasks = [_track(s, p) for s in suffixes for p in products]
            results = await asyncio.gather(*tasks)

            PROGRESS[job_id]["status"]  = "done"
            PROGRESS[job_id]["results"] = results
    except Exception:
        PROGRESS[job_id]["error"] = (
            "Деректердің бір бөлігі жүктелмеді. Қайталап көріңіз — "
            "жүктелген бөлігі кэште сақталды."
        )
        PROGRESS[job_id]["status"] = "failed"
        raise
