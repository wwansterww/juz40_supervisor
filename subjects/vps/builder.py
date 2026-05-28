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
)
from cache import api_get_async
from store import PROGRESS
from concurrency import report_slot
from subjects.base_builder import GLOBAL_SEMAPHORE_LIMIT, CLIENT_LIMITS

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
    """Raw course list for one (subject, product, month). Returns [] on errors."""
    url = (
        f"{BASE_URL}/v2/headteacher/subjects/{subject_id}/courses"
        f"?size=100&page=0&searchWord=&sort=year,DESC&sort=month,DESC"
        f"&product={product_key}&month={month_num}"
    )
    try:
        data = await api_get_async(url, token, client)
    except Exception:
        return []
    return data.get("content") or []


async def _fetch_groups(course_id, token, client):
    """Raw group list for a course (empty list on any error)."""
    try:
        data = await api_get_async(
            f"{BASE_URL}/v1/headteacher/courses/{course_id}/groups",
            token, client,
        )
        return data if isinstance(data, list) else []
    except Exception:
        return []


async def _build_one_subject_product(
    suffix, product, pack_name, month_num, token, client, semaphore,
):
    """Fetch + build data for one (suffix, product) combination of a pack.

    Returns a dict shaped like:
        {
          "suffix":        "МАТ",
          "label":         "МАТ",
          "product_key":   "SMART_VIP",
          "product_label": "VIP",
          "course_name":   "SMART VIP ИНФО-МАТ МАТ",
          "stream_name":   "SMART VIP ИНФО-МАТ 2026",
          "groups": [
            {"base": {...}, "weeks": {1: {...}, 2: {...}, 3: {...}, 4: {...}}, "monthly": {...}},
            …
          ],
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

    courses = await _list_subject_courses(
        suffix_info["subject_id"], product["key"], month_num, token, client,
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
    # the local semaphore cap parallel HTTP load.
    results = await asyncio.gather(
        *[builder_fn(g, token, month_num, client, semaphore) for g in groups_raw],
        return_exceptions=True,
    )
    base_skeleton["groups"] = [
        r for r in results if not isinstance(r, Exception) and r is not None
    ]
    return base_skeleton


# ── Main entry point ──────────────────────────────────────────────────────────

async def build_vps_report_job(job_id, pack_name, month_num, token):
    """Async task: build combined VPS report for a pack across all 3 тарифs.

    Progress ticks once per (subject, product) pair processed — so for the
    ИНФО-МАТ pack the counter goes 0 → 15 (5 subjects × 3 products).
    """
    suffixes = VPS_PACKS.get(pack_name, [])
    products = VPS_PRODUCTS

    # Seed progress synchronously so polling never 404s.
    PROGRESS[job_id] = {
        "total":     len(suffixes) * len(products),
        "done":      0,
        "status":    "queued",
        "results":   [],
        "pack_name": pack_name,
        "month_num": month_num,
    }

    try:
        async with report_slot(job_id):
            PROGRESS[job_id]["status"] = "running"

            semaphore  = asyncio.Semaphore(VPS_SEMAPHORE_LIMIT)
            done_count = 0

            # Custom HTTP limits for VPS — the default CLIENT_LIMITS
            # (max_connections=100) becomes the new bottleneck once we raise
            # the semaphore to 200. Match the new ceiling.
            vps_limits = httpx.Limits(
                max_connections=220,
                max_keepalive_connections=80,
                keepalive_expiry=30,
            )
            async with httpx.AsyncClient(limits=vps_limits) as client:

                async def _track(suffix, product):
                    nonlocal done_count
                    try:
                        return await _build_one_subject_product(
                            suffix, product, pack_name, month_num, token, client, semaphore,
                        )
                    except Exception:
                        return {
                            "suffix":        suffix,
                            "product_key":   product["key"],
                            "product_label": product["label"],
                            "label":         VPS_SUFFIX_TO_SUBJECT.get(suffix, {}).get("label", suffix),
                            "course_name":   "",
                            "stream_name":   "",
                            "groups":        [],
                        }
                    finally:
                        done_count += 1
                        PROGRESS[job_id]["done"] = done_count

                # 5 subjects × 3 products in parallel
                tasks = [_track(s, p) for s in suffixes for p in products]
                results = await asyncio.gather(*tasks)

            PROGRESS[job_id]["status"]  = "done"
            PROGRESS[job_id]["results"] = results
    except Exception:
        PROGRESS[job_id]["status"] = "failed"
        raise
