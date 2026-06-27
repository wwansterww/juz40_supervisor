"""
Curator report builder.

The supervisor (headteacher) report iterates over EVERY group/curator in a
course. A curator only ever sees their OWN group, so this builder is the same
logic scoped to a single group — and it talks to the curator API surface
(/v2/curator/* + /v3/curator/*) instead of /headteacher/*.

All the per-subject metric logic is reused untouched: given a subject_id we
resolve that subject's extract/merge/empty/metrics_to_row functions from the
existing registry, so a geography curator gets the geography columns, a history
curator gets the history columns, etc.

On top of the normal report this builder also computes, per week, a
"кто не сдал" list: for the allowlisted assignment types only (see
_NS_ALLOW_KEYWORDS), the count + names of students who didn't submit.
"""

import asyncio
import importlib
import logging
import re

import httpx

from config import BASE_URL, GLOBAL_SEMAPHORE_LIMIT
from cache import api_get_async, get_shared_client
from store import PROGRESS
from concurrency import report_slot
from utils import normalize
from subjects.common import is_quiz_theme
from subjects._registry import SUBJECTS
from subjects.base_builder import (
    DataFetchError,
    is_submitted,
    is_left_course,
    get_student_id,
    to_int,
    _collect_left_ids,
    _lesson_left_ids,
    _recalc_item,
    _ZERO_SCORE_THEME_KEYWORDS,
)

logger = logging.getLogger("juz40.curator")


# ── Subject → metric-function resolution ──────────────────────────────────────
# Each curator works one subject, identified by the group's subjectId. We map it
# back to the existing SubjectConfig and pull that subject's metric functions so
# the curator report is column-for-column identical to the supervisor's.

_SUBJECT_BY_ID = {c.subject_id: c for c in SUBJECTS}


def get_cfg(subject_id: str):
    return _SUBJECT_BY_ID.get(subject_id)


def resolve_metric_fns(subject_id: str):
    """Return (extract_metrics, merge_metrics, empty_metrics, metrics_to_row)
    for a subject, or None if the subject isn't known.

    `extract_metrics` and `metrics_to_row` are uniformly named across every
    subject module; the merge/empty helpers carry a per-subject suffix
    (merge_metrics_geo, merge_metrics_info, …) so we resolve them by prefix.
    The bare common helpers (`merge_metrics`, `empty_metrics`) have no trailing
    underscore, so the `_`-suffixed filter never picks them up by mistake.
    """
    cfg = _SUBJECT_BY_ID.get(subject_id)
    if cfg is None:
        return None
    mod = importlib.import_module(cfg.metrics_module)
    extract = getattr(mod, "extract_metrics")
    to_row = getattr(mod, "metrics_to_row")
    merges = sorted(n for n in dir(mod) if n.startswith("merge_metrics_"))
    empties = sorted(n for n in dir(mod) if n.startswith("empty_metrics_"))
    if not merges or not empties:
        return None
    return extract, getattr(mod, merges[0]), getattr(mod, empties[0]), to_row


# ── Curator API endpoints ─────────────────────────────────────────────────────

def _themes_url(group_id, month, week):
    return f"{BASE_URL}/v2/curator/groups/{group_id}/themes?month={month}&week={week}"


def _summary_url(group_id, theme_id):
    return f"{BASE_URL}/v3/curator/groups/{group_id}/themes/{theme_id}/lessons/summary"


def _progresses_url(group_id, lesson_id):
    return f"{BASE_URL}/v2/curator/groups/{group_id}/lessons/{lesson_id}/progresses"


# Only these assignment types appear in the "кто не сдал" block (allowlist —
# everything else, incl. ОҚУЛЫҚ / БАЗА / видео / практика, is left out). Matched
# against the NORMALISED theme name, so Latin/Cyrillic look-alikes and the
# "5-АЙ 4-АПТА …" prefix don't matter. Куиз is matched via is_quiz_theme, which
# already handles the QUIZ / КУИЗ / КВИЗ spellings.
_NS_ALLOW_KEYWORDS = tuple(normalize(k) for k in (
    "ҮЙ ЖҰМЫС",            # Үй жұмысы
    "ЖҰМЫС ДӘПТЕР",        # Жұмыс дәптері (география т.б. — үй жұмысының баламасы)
    "ҚАТЕМЕН ЖҰМЫС",       # Қатемен жұмыс
    "КОНСПЕКТ",            # Конспект
    "САБАҚ ТАПСЫРУ",       # Сабақ тапсыру
    "КАРТАМЕН ЖҰМЫС",      # Картамен жұмыс
    "ТЕОРИЯЛЫҚ ТАПСЫРМА",  # Теориялық тапсырма
    "ТАҚЫРЫПТЫҚ ТАПСЫРМА", # Тақырыптық тапсырма
))


# Themes that must NEVER appear, even if they'd otherwise match above. "Сынақ
# тест" (mock/exam test) contains "ТЕСТ", so the Куиз matcher (is_quiz_theme,
# shared with the metrics and not safe to change) would wrongly include it —
# this block list wins over the allowlist to keep it out for every subject.
_NS_BLOCK_KEYWORDS = tuple(normalize(k) for k in (
    "СЫНАҚ",   # Сынақ тест
))


def _ns_included(theme_name: str) -> bool:
    n = normalize(theme_name or "")
    if any(kw in n for kw in _NS_BLOCK_KEYWORDS):
        return False
    return is_quiz_theme(n) or any(kw in n for kw in _NS_ALLOW_KEYWORDS)

# "5-АЙ 4-АПТА КАРТАМЕН ЖҰМЫС" → "КАРТАМЕН ЖҰМЫС" for display.
_WEEK_PREFIX_RE = re.compile(r"^\s*\d+\s*-?\s*АЙ\s+\d+\s*-?\s*АПТА\s+", re.IGNORECASE)


def _clean_theme_label(theme_name: str) -> str:
    return _WEEK_PREFIX_RE.sub("", theme_name or "").strip() or (theme_name or "").strip()


def _student_name(progress: dict) -> str:
    fn = (progress.get("studentFirstname") or "").strip()
    ln = (progress.get("studentLastname") or "").strip()
    full = f"{fn} {ln}".strip()
    return full or (progress.get("username") or "—")


# ── Fetchers (404 = legitimately empty; anything else = loud failure) ──────────

async def _fetch_themes(group_id, month, week, token, client):
    try:
        data = await api_get_async(_themes_url(group_id, month, week), token, client)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return []
        raise DataFetchError(f"curator themes group={group_id} week={week}") from exc
    except Exception as exc:
        raise DataFetchError(f"curator themes group={group_id} week={week}") from exc

    themes = data.get("themes", []) if isinstance(data, dict) else []
    out = []
    for t in themes:
        if not t.get("themeId"):
            continue
        # Defensive: keep only the requested week/month even if the endpoint
        # ignored the query params and returned everything.
        tw, tm = t.get("week"), t.get("month")
        if tw is not None and tw != week:
            continue
        if tm is not None and tm != month:
            continue
        out.append(t)
    return out


async def _fetch_summary(group_id, theme_id, token, client, semaphore):
    async with semaphore:
        try:
            data = await api_get_async(_summary_url(group_id, theme_id), token, client)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return []
            raise DataFetchError(f"curator summary theme={theme_id}") from exc
        except Exception as exc:
            raise DataFetchError(f"curator summary theme={theme_id}") from exc
        # The theme-level endpoint returns a list of lessons; the single-lesson
        # endpoint returns one object. Accept either so the same code path works
        # whichever shape the API gives us.
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return []


async def _fetch_progresses(group_id, lesson_id, token, client, semaphore):
    async with semaphore:
        try:
            data = await api_get_async(_progresses_url(group_id, lesson_id), token, client)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return []
            raise DataFetchError(f"curator progresses lesson={lesson_id}") from exc
        except Exception as exc:
            raise DataFetchError(f"curator progresses lesson={lesson_id}") from exc
        return data if isinstance(data, list) else []


# ── Per-week build ─────────────────────────────────────────────────────────────

async def _fetch_week(group_id, month, week, token, client, semaphore,
                      extract_fn, merge_fn, empty_fn):
    """Returns (metrics, student_count, not_submitted) for one week."""
    themes = await _fetch_themes(group_id, month, week, token, client)
    valid_themes = [t for t in themes if t.get("themeId")]
    if not valid_themes:
        return empty_fn(), 0, []

    summaries = await asyncio.gather(
        *[_fetch_summary(group_id, t["themeId"], token, client, semaphore)
          for t in valid_themes],
    )

    # Collect every lesson id (parents + children) across all themes.
    max_students = 0
    all_lesson_ids: list[str] = []
    seen: set[str] = set()
    for sr in summaries:
        for item in sr:
            sc = to_int(item.get("studentsCount") or item.get("totalStudentsCount") or 0)
            max_students = max(max_students, sc)
            lid = item.get("lessonId") or item.get("id")
            if lid and lid not in seen:
                seen.add(lid)
                all_lesson_ids.append(lid)
            for child in (item.get("children") or []):
                c_sc = to_int(child.get("studentsCount") or child.get("totalStudentsCount") or 0)
                max_students = max(max_students, c_sc)
                clid = child.get("lessonId") or child.get("id")
                if clid and clid not in seen:
                    seen.add(clid)
                    all_lesson_ids.append(clid)

    progress_lists = await asyncio.gather(
        *[_fetch_progresses(group_id, lid, token, client, semaphore)
          for lid in all_lesson_ids],
    )
    progress_cache = dict(zip(all_lesson_ids, progress_lists))

    week_left_ids = _collect_left_ids(progress_lists)
    student_count = max(0, max_students - len(week_left_ids))

    # Recalc each summary from real progress data (excludes "шыққан оқушы" from
    # both numerator and denominator), then extract this subject's metrics.
    fixed_summaries = []
    for t, sr in zip(valid_themes, summaries):
        theme_upper = (t.get("themeName") or "").upper()
        inc_zero = any(kw in theme_upper for kw in _ZERO_SCORE_THEME_KEYWORDS)
        new_sr = []
        for item in sr:
            lid = item.get("lessonId") or item.get("id")
            progresses = progress_cache.get(lid, []) if lid else []
            parent_left = _lesson_left_ids(progresses, week_left_ids)
            new_item = _recalc_item(item, progresses, parent_left, include_zero_score=inc_zero)
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

    week_theme_metrics = [
        extract_fn(sr, normalize(t.get("themeName") or ""))
        for t, sr in zip(valid_themes, fixed_summaries)
    ]
    metrics = merge_fn(week_theme_metrics) if week_theme_metrics else empty_fn()

    not_submitted = _build_not_submitted(valid_themes, summaries, progress_cache, week_left_ids)
    return metrics, int(student_count), not_submitted


def _build_not_submitted(valid_themes, summaries, progress_cache, week_left_ids):
    """Per allowlisted theme (_NS_ALLOW_KEYWORDS): who didn't submit, and how many.

    Only top-level submission lessons are considered — child rows (e.g. ҚЖ) and
    LECTURE-type lessons (videos / reading, which aren't "submitted") are
    skipped so the list stays meaningful. Students who left the course are
    excluded — they didn't "fail to submit", they left."""
    out = []
    for t, sr in zip(valid_themes, summaries):
        theme_name = t.get("themeName") or ""
        if not _ns_included(theme_name):
            continue
        theme_upper = normalize(theme_name)
        inc_zero = any(kw in theme_upper for kw in _ZERO_SCORE_THEME_KEYWORDS)
        missing: dict[str, str] = {}
        for item in sr:
            if item.get("parentId") is not None:
                continue
            if (item.get("lessonType") or "").upper() == "LECTURE":
                continue
            lid = item.get("lessonId") or item.get("id")
            for p in progress_cache.get(lid, []) if lid else []:
                sid = get_student_id(p)
                if not sid or sid in week_left_ids or is_left_course(p):
                    continue
                if not is_submitted(p, include_zero_score=inc_zero):
                    missing[sid] = _student_name(p)
        if missing:
            names = sorted(missing.values(), key=lambda s: s.lower())
            out.append({
                "theme": _clean_theme_label(theme_name),
                "count": len(names),
                "names": names,
            })
    return out


# ── Background job ─────────────────────────────────────────────────────────────

async def _build_curator_report_job(job_id, group, curator_name, subject_id,
                                    token, month, week_filter=None):
    PROGRESS[job_id] = {"total": 0, "done": 0, "status": "queued", "results": []}

    fns = resolve_metric_fns(subject_id)
    if fns is None:
        PROGRESS[job_id]["status"] = "failed"
        PROGRESS[job_id]["error"] = "Бұл пән бойынша отчет қолжетімсіз."
        return
    extract_fn, merge_fn, empty_fn, _to_row = fns

    group_id = group.get("groupId") or group.get("id")
    course_name = group.get("courseName", "")

    try:
        async with report_slot(job_id):
            PROGRESS[job_id]["status"] = "running"
            client = get_shared_client()

            weeks_to_fetch = [week_filter] if week_filter in (1, 2, 3, 4) else [1, 2, 3, 4]
            PROGRESS[job_id]["total"] = len(weeks_to_fetch)
            semaphore = asyncio.Semaphore(GLOBAL_SEMAPHORE_LIMIT)
            done = 0

            async def _do_week(w):
                nonlocal done
                try:
                    try:
                        return await _fetch_week(group_id, month, w, token, client,
                                                 semaphore, extract_fn, merge_fn, empty_fn)
                    except DataFetchError:
                        # One more pass — cache holds whatever already succeeded.
                        await asyncio.sleep(1.0)
                        return await _fetch_week(group_id, month, w, token, client,
                                                 semaphore, extract_fn, merge_fn, empty_fn)
                finally:
                    done += 1
                    PROGRESS[job_id]["done"] = done

            results = await asyncio.gather(
                *[_do_week(w) for w in weeks_to_fetch],
                return_exceptions=True,
            )

            if any(isinstance(r, BaseException) for r in results):
                logger.warning("curator report %s: week fetch failed", job_id)
                PROGRESS[job_id]["error"] = (
                    "Деректерді жүктеу кезінде қате шықты. Қайталап көріңіз."
                )
                PROGRESS[job_id]["status"] = "failed"
                return

            weeks_data: dict[str, dict] = {}
            weeks_ns: dict[str, list] = {}
            all_week_metrics = []
            student_total = 0
            for w, (metrics, sc, ns) in zip(weeks_to_fetch, results):
                weeks_data[str(w)] = metrics
                weeks_ns[str(w)] = ns
                student_total = max(student_total, sc)
                if any(v is not None for v in metrics.values()):
                    all_week_metrics.append(metrics)

            for w in (1, 2, 3, 4):
                weeks_data.setdefault(str(w), empty_fn())
                weeks_ns.setdefault(str(w), [])

            monthly = merge_fn(all_week_metrics) if all_week_metrics else empty_fn()
            base = {"Поток": course_name, "Куратор": curator_name, "Оқушы саны": student_total}

            PROGRESS[job_id]["results"] = [{
                "base": base,
                "weeks": weeks_data,
                "weeks_not_submitted": weeks_ns,
                "monthly": monthly,
            }]
            PROGRESS[job_id]["status"] = "done"
            logger.info("curator report %s: done", job_id)
    except Exception:
        logger.exception("curator report %s: crashed", job_id)
        PROGRESS[job_id]["status"] = "failed"
        raise
