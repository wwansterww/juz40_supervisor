"""
Route factory: builds an APIRouter with every endpoint for a subject from a
config object. Replaces the 16 nearly-identical routes.py files.

Per-subject differences are captured in `SubjectConfig` (see _registry.py for
the concrete instances). Behavior selected by config:
    • use_paginated_fetch     — does the courses listing need page=0..N?
    • pass_stream_month       — pass stream_month to the courses URL?
    • has_section_report      — also register /section-report/* endpoints?
    • has_debug_themes        — register the informatics /debug/themes endpoint?
    • report_template         — Jinja template for the result page.

If a future subject is structurally different, prefer adding a flag here over
forking the factory.
"""

from __future__ import annotations

import io
import asyncio
import uuid
import importlib
from dataclasses import dataclass, field
from typing import Optional

import httpx
import pandas as pd
from fastapi import APIRouter, Request, Form, Response
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

from config import (
    BASE_URL,
    COURSE_TYPES, COURSE_TYPE_TO_PRODUCTS,
    STREAM_MONTHS, STUDY_MONTHS,
    MONTH_NAME_TO_NUM,
    TYPE_NAME_KEYWORDS, TYPE_EXCLUDE_KEYWORDS,
)
from cache import api_get_async, get_shared_client
from store import PROGRESS, REPORT_STORE
from concurrency import get_queue_position
from subjects.route_utils import fetch_all_course_pages


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass
class SubjectConfig:
    # Identity
    slug: str                       # URL slug, e.g. "biology" or "kazakh-language"
    name: str                       # Display name, e.g. "Биология"
    subject_id: str                 # The platform's subject UUID

    # Routing
    prefix: str                     # URL prefix, e.g. "/biology" or "" for informatics
    active_subject: str             # JS hint matching the dashboard tab, often == slug

    # Templates
    report_template: str = "report.html"

    # Module paths (importlib)
    metrics_module: str = ""        # e.g. "subjects.biology.metrics"
    builder_module: str = ""        # e.g. "subjects.biology.builder"

    # Behavior switches
    use_paginated_fetch: bool = False    # use route_utils.fetch_all_course_pages
    pass_stream_month: bool = False      # include &month=N in the courses URL
    has_section_report: bool = False
    has_debug_themes: bool = False       # informatics only

    # Misc
    csv_filename: str = "report.csv"


# ── Factory ───────────────────────────────────────────────────────────────────

def make_subject_router(cfg: SubjectConfig) -> APIRouter:
    """Build a fully-wired APIRouter for one subject."""

    metrics_mod = importlib.import_module(cfg.metrics_module)
    builder_mod = importlib.import_module(cfg.builder_module)

    metrics_to_row    = getattr(metrics_mod, "metrics_to_row")
    compute_avg_row   = getattr(metrics_mod, "compute_avg_row")
    _build_report_job = getattr(builder_mod, "_build_report_job")
    _build_section_report_job = getattr(builder_mod, "_build_section_report_job", None)

    router = APIRouter()

    # The dashboard URL we redirect to when something goes wrong. Informatics
    # has no /informatics prefix → falls back to the global /dashboard.
    dashboard_path = f"{cfg.prefix}/dashboard" if cfg.prefix else "/dashboard"

    # Context shared across template renders so every page knows which subject
    # it belongs to (used by the navbar/active-tab UI).
    def _subject_ctx():
        return {
            "active_subject": cfg.active_subject,
            "subject_name":   cfg.name,
            "subject_prefix": cfg.prefix,
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _matches_type(name: str, course_type: str) -> bool:
        name_up = name.upper()
        exclude  = TYPE_EXCLUDE_KEYWORDS.get(course_type.upper(), [])
        if any(ex in name_up for ex in exclude):
            return False
        keywords = TYPE_NAME_KEYWORDS.get(course_type.upper(), [course_type.upper()])
        return any(kw in name_up for kw in keywords)

    async def _fetch_courses_by_type(
        course_type: str,
        token: str,
        stream_month: Optional[str] = None,
    ) -> list:
        products = COURSE_TYPE_TO_PRODUCTS.get(course_type.upper(), [course_type.upper()])
        month_num = MONTH_NAME_TO_NUM.get((stream_month or "").upper()) if cfg.pass_stream_month else None

        page_size = 50 if cfg.use_paginated_fetch else 200
        urls = []
        for p in products:
            url = (
                f"{BASE_URL}/v2/headteacher/subjects/{cfg.subject_id}/courses"
                f"?size={page_size}&page=0&searchWord=&sort=year,DESC&sort=month,DESC"
                f"&product={p}"
            )
            if month_num:
                url += f"&month={month_num}"
            urls.append(url)

        client = get_shared_client()
        if cfg.use_paginated_fetch:
            results = await asyncio.gather(
                *[fetch_all_course_pages(u, token, client) for u in urls],
                return_exceptions=True,
            )
            out: list = []
            for r in results:
                if not isinstance(r, Exception):
                    out.extend(r)
            return out
        else:
            responses = await asyncio.gather(
                *[api_get_async(u, token, client) for u in urls],
                return_exceptions=True,
            )
            out: list = []
            for resp in responses:
                if not isinstance(resp, Exception):
                    out.extend(resp.get("content", []))
            return out

    # ── Dashboard ─────────────────────────────────────────────────────────────

    @router.get("/dashboard", response_class=HTMLResponse)
    async def dashboard(request: Request):
        from main import templates
        token = request.session.get("token")
        if not token:
            return RedirectResponse("/", status_code=302)
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "course_types":  COURSE_TYPES,
            "stream_months": STREAM_MONTHS,
            "study_months":  STUDY_MONTHS,
            "courses": None,
            "selected_type": None,
            "selected_month": None,
            "error": None,
            **_subject_ctx(),
        })

    @router.post("/filter-courses", response_class=HTMLResponse)
    async def filter_courses(
        request: Request,
        course_type:  str = Form(...),
        stream_month: str = Form(...),
    ):
        from main import templates
        token = request.session.get("token")
        if not token:
            return RedirectResponse("/", status_code=302)
        try:
            all_courses = await _fetch_courses_by_type(course_type, token, stream_month)
            month_num = MONTH_NAME_TO_NUM.get(stream_month.upper())
            filtered = [
                c for c in all_courses
                if (
                    stream_month.upper() in c["name"].upper()
                    or (month_num is not None and c.get("month") == month_num)
                )
                and _matches_type(c["name"], course_type)
                and "(КОПИЯ" not in c["name"].upper()
            ]
        except Exception:
            return templates.TemplateResponse("dashboard.html", {
                "request": request,
                "course_types":  COURSE_TYPES,
                "stream_months": STREAM_MONTHS,
                "study_months":  STUDY_MONTHS,
                "courses": [],
                "selected_type":  course_type,
                "selected_month": stream_month,
                "error": "API қатесі. Токен мерзімі өтуі мүмкін — қайта кіріңіз.",
                **_subject_ctx(),
            })
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "course_types":  COURSE_TYPES,
            "stream_months": STREAM_MONTHS,
            "study_months":  STUDY_MONTHS,
            "courses": filtered,
            "selected_type":  course_type,
            "selected_month": stream_month,
            "error": None,
            **_subject_ctx(),
        })

    # ── Weekly / monthly report ───────────────────────────────────────────────

    # Phases the client shows while a single-course report is being built.
    # The first threshold (p ≤ pct) wins; the bar passes through all of them.
    _REPORT_STAGES = [
        {"p": 0,  "icon": "📥", "title": "Топтар жүктелуде…"},
        {"p": 12, "icon": "📊", "title": "Сабақтар талданады…"},
        {"p": 35, "icon": "🧮", "title": "Орташа балл есептелуде…"},
        {"p": 65, "icon": "📈", "title": "Кесте құрастырылуда…"},
        {"p": 88, "icon": "✨", "title": "Қорытынды дайындалуда…"},
    ]

    @router.post("/report", response_class=HTMLResponse)
    async def report(
        request: Request,
        course_id:   str = Form(...),
        course_name: str = Form(...),
        study_month: str = Form(...),
        week:        str = Form("all"),
    ):
        from main import templates
        # Show the picked week in the loading screen subtitle so the user
        # knows whether they're waiting for one week or for all four.
        if week and week != "all":
            week_label = f"{week}-апта"
            subtitle = f"<strong>{course_name}</strong> · {study_month} · {week_label}"
        else:
            subtitle = f"<strong>{course_name}</strong> · {study_month}"
        return templates.TemplateResponse("loading.html", {
            "request": request,
            "title":             "Отчет жасалуда…",
            "subtitle_html":     subtitle,
            "unit":              "Топ",
            "start_url":         f"{cfg.prefix}/report/start",
            "progress_url_base": f"{cfg.prefix}/report/progress",
            "result_url":        f"{cfg.prefix}/report/result",
            "hidden_fields": {
                "course_id":   course_id,
                "course_name": course_name,
                "study_month": study_month,
                "week":        week,
            },
            "stages": _REPORT_STAGES,
        })

    @router.post("/report/start")
    async def report_start(
        request: Request,
        course_id:   str = Form(...),
        course_name: str = Form(...),
        study_month: str = Form(...),
        week:        str = Form("all"),
    ):
        token = request.session.get("token")
        if not token:
            return RedirectResponse("/", status_code=302)
        try:
            month_num = int(study_month.replace("-ай", ""))
        except ValueError:
            return JSONResponse({"error": "Жарамсыз оқу айы"}, status_code=400)

        # Parse the optional single-week filter. Anything other than "1"..."4"
        # falls back to "all weeks" so the report still builds even when the
        # form value is missing / "all" / something garbled.
        week_filter = None
        if week and week != "all":
            try:
                wf = int(week)
                if wf in (1, 2, 3, 4):
                    week_filter = wf
            except ValueError:
                pass

        # Use the shared keep-alive client so we don't pay TLS handshake cost
        # for this one quick lookup before kicking off the background job.
        try:
            groups = await api_get_async(
                f"{BASE_URL}/v1/headteacher/courses/{course_id}/groups",
                token, get_shared_client(),
            )
        except Exception:
            return JSONResponse({"error": "Топтарды жүктеу кезінде қате шықты."}, status_code=500)
        if not groups:
            return JSONResponse({"error": "Топтар табылмады."}, status_code=404)

        job_id = str(uuid.uuid4())
        request.session["last_job_id"]      = job_id
        request.session["last_course_name"] = course_name
        request.session["last_study_month"] = study_month
        request.session["last_week_filter"] = week_filter  # None or 1..4

        asyncio.create_task(_build_report_job(job_id, groups, token, month_num, week_filter=week_filter))
        return JSONResponse({"job_id": job_id, "total": len(groups)})

    @router.get("/report/progress/{job_id}")
    async def report_progress(job_id: str):
        p = await PROGRESS.aget(job_id)
        if not p:
            return JSONResponse({"total": 0, "done": 0, "status": "initializing", "queue_position": 0})
        return JSONResponse({
            "total":          p.get("total", 0),
            "done":           p.get("done", 0),
            "status":         p.get("status", "running"),
            "queue_position": get_queue_position(job_id),
        })

    @router.get("/report/result", response_class=HTMLResponse)
    async def report_result(request: Request):
        from main import templates
        token = request.session.get("token")
        if not token:
            return RedirectResponse("/", status_code=302)

        job_id      = request.session.get("last_job_id")
        course_name = request.session.get("last_course_name", "")
        study_month = request.session.get("last_study_month", "")
        week_filter = request.session.get("last_week_filter")  # None or 1..4

        p = (await PROGRESS.aget(job_id)) if job_id else None
        if not p or p["status"] != "done":
            return RedirectResponse(dashboard_path, status_code=302)

        group_results = p["results"]

        # If the user picked a single week we only render that one tab — the
        # other weeks weren't actually fetched (all-None metrics) and the
        # monthly aggregate would just be a copy of the one fetched week,
        # so suppressing it avoids confusing duplicate-looking columns.
        weeks_to_show = [week_filter] if week_filter in (1, 2, 3, 4) else [1, 2, 3, 4]

        tables = []
        for week in weeks_to_show:
            rows    = [metrics_to_row(gr["base"], gr["weeks"][week]) for gr in group_results]
            avg_row = compute_avg_row(rows)
            tables.append({
                "title":    f"{week}-апта",
                "subtitle": f"{study_month} {week}-апта нәтижелері",
                "week":     week,
                "rows":     rows,
                "avg_row":  avg_row,
            })

        if week_filter is None:
            monthly_rows = [metrics_to_row(gr["base"], gr["monthly"]) for gr in group_results]
            monthly_avg  = compute_avg_row(monthly_rows)
            tables.append({
                "title":    "Айлық қорытынды",
                "subtitle": f"{study_month} бойынша жалпы қорытынды",
                "week":     "monthly",
                "rows":     monthly_rows,
                "avg_row":  monthly_avg,
            })

        report_key = job_id
        REPORT_STORE[report_key] = {
            "tables": [
                {"title": f"{study_month} {t['title']}", "rows": t["rows"], "avg_row": t["avg_row"]}
                for t in tables
            ],
            "title": f"{course_name} {study_month}",
        }
        request.session["last_report_key"] = report_key

        return templates.TemplateResponse(cfg.report_template, {
            "request":     request,
            "tables":      tables,
            "course_name": course_name,
            "study_month": study_month,
            "error":       None,
            "group_count": len(group_results),
            **_subject_ctx(),
        })

    # ── Section report (only for subjects that have it) ───────────────────────

    if cfg.has_section_report and _build_section_report_job is not None:

        @router.post("/section-report/start")
        async def section_report_start(
            request: Request,
            course_type: str = Form(...),
            study_month: str = Form(...),
        ):
            token = request.session.get("token")
            if not token:
                return JSONResponse({"error": "not logged in"}, status_code=401)
            try:
                month_num = int(study_month.replace("-ай", ""))
            except ValueError:
                return JSONResponse({"error": "Жарамсыз оқу айы"}, status_code=400)
            try:
                courses = await _fetch_courses_by_type(course_type, token)
                courses = [c for c in courses if "(КОПИЯ" not in c["name"].upper()]
            except Exception:
                return JSONResponse({"error": "Курстарды жүктеу кезінде қате."}, status_code=500)
            if not courses:
                return JSONResponse({"error": "Курстар табылмады."}, status_code=404)

            job_id = str(uuid.uuid4())
            request.session["last_section_job_id"]      = job_id
            request.session["last_section_course_type"] = course_type
            request.session["last_section_study_month"] = study_month

            asyncio.create_task(_build_section_report_job(job_id, courses, token, month_num))
            return JSONResponse({"job_id": job_id, "total": len(courses)})

        @router.get("/section-report/progress/{job_id}")
        async def section_report_progress(job_id: str):
            p = await PROGRESS.aget(job_id)
            if not p:
                return JSONResponse({"total": 0, "done": 0, "status": "initializing", "queue_position": 0})
            return JSONResponse({
                "total":          p.get("total", 0),
                "done":           p.get("done", 0),
                "status":         p.get("status", "running"),
                "queue_position": get_queue_position(job_id),
            })

        @router.get("/section-report/result", response_class=HTMLResponse)
        async def section_report_result(request: Request):
            from main import templates
            token = request.session.get("token")
            if not token:
                return RedirectResponse("/", status_code=302)

            job_id       = request.session.get("last_section_job_id")
            course_type  = request.session.get("last_section_course_type", "")
            study_month  = request.session.get("last_section_study_month", "")

            p = (await PROGRESS.aget(job_id)) if job_id else None
            if not p or p["status"] != "done":
                return RedirectResponse(dashboard_path, status_code=302)

            rows    = [r for r in p["results"] if r is not None]
            avg_row = compute_avg_row(rows)

            report_key = job_id
            REPORT_STORE[report_key] = {
                "tables": [{
                    "title":   f"{cfg.name} {course_type} {study_month}",
                    "rows":    rows,
                    "avg_row": avg_row,
                }],
                "title": f"{cfg.name} {course_type} {study_month}",
            }
            request.session["last_report_key"] = report_key

            return templates.TemplateResponse(cfg.report_template, {
                "request": request,
                "tables": [{
                    "title":    "Раздел бойынша жалпы қорытынды",
                    "subtitle": f"{cfg.name} {course_type} {study_month}",
                    "week":     "section",
                    "rows":     rows,
                    "avg_row":  avg_row,
                }],
                "course_name": f"{cfg.name} {course_type}",
                "study_month": study_month,
                "error":       None,
                "group_count": len(rows),
                **_subject_ctx(),
            })

    # ── CSV export ────────────────────────────────────────────────────────────

    @router.get("/export")
    async def export_csv(request: Request):
        token = request.session.get("token")
        if not token:
            return RedirectResponse("/", status_code=302)
        report_key = request.session.get("last_report_key")
        store = (await REPORT_STORE.aget(report_key)) if report_key else None
        if not store:
            return Response(
                content="Экспортқа деректер жоқ. Алдымен отчет жасаңыз.",
                status_code=400,
            )
        tables = store["tables"]
        if not tables:
            return Response(content="Экспортқа деректер жоқ", status_code=400)

        output = io.StringIO()
        for table in tables:
            rows    = list(table.get("rows", []))
            avg_row = table.get("avg_row")
            if not rows:
                continue
            output.write(f"# {table['title']}\n")
            df = pd.DataFrame(rows)
            if avg_row:
                df = pd.concat([df, pd.DataFrame([avg_row])], ignore_index=True)
            df.to_csv(output, index=False)
            output.write("\n")

        return Response(
            content=output.getvalue().encode("utf-8-sig"),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename={cfg.csv_filename}"},
        )

    # ── Course months helper (used by dashboard.html JS) ──────────────────────

    @router.get("/course-months")
    async def course_months(request: Request, course_id: str):
        token = request.session.get("token")
        if not token:
            return JSONResponse({"error": "not logged in"}, status_code=401)
        try:
            client = get_shared_client()
            groups = await api_get_async(
                f"{BASE_URL}/v1/headteacher/courses/{course_id}/groups",
                token, client,
            )
            if not groups:
                return JSONResponse({"months": list(range(1, 6))})
            group_id = groups[0]["id"]
            data = await api_get_async(
                f"{BASE_URL}/v1/headteacher/groups/{group_id}/themes?week=1&month=1",
                token, client,
            )
            months = data.get("months", list(range(1, 6)))
            return JSONResponse({"months": sorted(months)})
        except Exception:
            return JSONResponse({"months": list(range(1, 6))})

    # ── Debug (informatics only) ──────────────────────────────────────────────

    if cfg.has_debug_themes:

        @router.get("/debug/themes")
        async def debug_themes(request: Request, group_id: str, month: int = 2, week: int = 1):
            token = request.session.get("token")
            if not token:
                return JSONResponse({"error": "not logged in"})
            client = get_shared_client()
            themes_data = await api_get_async(
                f"{BASE_URL}/v1/headteacher/groups/{group_id}/themes?week={week}&month={month}",
                token, client,
            )
            themes = themes_data.get("themes", [])
            result = []
            for t in themes:
                theme_id = t.get("themeId")
                try:
                    summary = await api_get_async(
                        f"{BASE_URL}/v3/headteacher/groups/{group_id}/themes/{theme_id}/lessons/summary",
                        token, client,
                    )
                except Exception as e:
                    summary = {"error": str(e)}
                result.append({
                        "themeName": t.get("themeName", ""),
                        "themeId":   theme_id,
                        "lessons": [
                            {
                                "name":            item.get("name"),
                                "lessonType":      item.get("lessonType"),
                                "studentsCount":   item.get("studentsCount"),
                                "submittedCount":  item.get("submittedCount"),
                            }
                            for item in (summary if isinstance(summary, list) else [])
                        ],
                    })
            return JSONResponse(result)

    return router
