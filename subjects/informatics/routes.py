import io
import asyncio
import uuid

import httpx
import pandas as pd
from fastapi import APIRouter, Request, Form, Response
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse

from config import (
    BASE_URL, INFORMATICS_SUBJECT_ID,
    COURSE_TYPES, COURSE_TYPE_TO_PRODUCTS,
    STREAM_MONTHS, STUDY_MONTHS,
    MONTH_NAME_TO_NUM,
    TYPE_NAME_KEYWORDS, TYPE_EXCLUDE_KEYWORDS,
)
from cache import api_get_async
from store import PROGRESS, REPORT_STORE
from subjects.informatics.metrics import metrics_to_row, compute_avg_row
from subjects.informatics.builder import _build_report_job

router = APIRouter()


# ── Helpers ────────────────────────────────────────────────────────────────────

def matches_type(name: str, course_type: str) -> bool:
    name_up = name.upper()
    exclude = TYPE_EXCLUDE_KEYWORDS.get(course_type.upper(), [])
    if any(ex in name_up for ex in exclude):
        return False
    keywords = TYPE_NAME_KEYWORDS.get(course_type.upper(), [course_type.upper()])
    return any(kw in name_up for kw in keywords)


async def fetch_courses_by_type(course_type: str, token: str) -> list:
    products = COURSE_TYPE_TO_PRODUCTS.get(course_type.upper(), [course_type.upper()])
    urls = [
        f"{BASE_URL}/v2/headteacher/subjects/{INFORMATICS_SUBJECT_ID}/courses"
        f"?size=200&page=0&searchWord=&sort=year,DESC&sort=month,DESC&product={p}"
        for p in products
    ]
    async with httpx.AsyncClient() as client:
        responses = await asyncio.gather(
            *[api_get_async(url, token, client) for url in urls],
            return_exceptions=True,
        )
    all_courses = []
    for resp in responses:
        if not isinstance(resp, Exception):
            all_courses.extend(resp.get("content", []))
    return all_courses


# ── Dashboard ──────────────────────────────────────────────────────────────────

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    from main import templates
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "course_types": COURSE_TYPES,
        "stream_months": STREAM_MONTHS,
        "study_months": STUDY_MONTHS,
        "courses": None,
        "selected_type": None,
        "selected_month": None,
        "error": None,
    })


@router.post("/filter-courses", response_class=HTMLResponse)
async def filter_courses(
    request: Request,
    course_type: str = Form(...),
    stream_month: str = Form(...),
):
    from main import templates
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)
    try:
        all_courses = await fetch_courses_by_type(course_type, token)
        month_num = MONTH_NAME_TO_NUM.get(stream_month.upper())
        filtered = [
            c for c in all_courses
            if (
                stream_month.upper() in c["name"].upper()
                or (month_num is not None and c.get("month") == month_num)
            )
            and matches_type(c["name"], course_type)
            and "(КОПИЯ" not in c["name"].upper()
        ]
    except Exception:
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "course_types": COURSE_TYPES,
            "stream_months": STREAM_MONTHS,
            "study_months": STUDY_MONTHS,
            "courses": [],
            "selected_type": course_type,
            "selected_month": stream_month,
            "error": "API қатесі. Токен мерзімі өтуі мүмкін — қайта кіріңіз.",
        })
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "course_types": COURSE_TYPES,
        "stream_months": STREAM_MONTHS,
        "study_months": STUDY_MONTHS,
        "courses": filtered,
        "selected_type": course_type,
        "selected_month": stream_month,
        "error": None,
    })


# ── Weekly/Monthly report ──────────────────────────────────────────────────────

@router.post("/report", response_class=HTMLResponse)
async def report(
    request: Request,
    course_id: str = Form(...),
    course_name: str = Form(...),
    study_month: str = Form(...),
):
    from main import templates
    return templates.TemplateResponse("report_loading.html", {
        "request": request,
        "course_id": course_id,
        "course_name": course_name,
        "study_month": study_month,
        "subject_name": "Информатика",
        "subject_prefix": "",
    })


@router.post("/report/start")
async def report_start(
    request: Request,
    course_id: str = Form(...),
    course_name: str = Form(...),
    study_month: str = Form(...),
):
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)

    try:
        month_num = int(study_month.replace("-ай", ""))
    except ValueError:
        return JSONResponse({"error": "Жарамсыз оқу айы"}, status_code=400)

    try:
        async with httpx.AsyncClient() as client:
            groups = await api_get_async(
                f"{BASE_URL}/v1/headteacher/courses/{course_id}/groups", token, client
            )
    except Exception:
        return JSONResponse({"error": "Топтарды жүктеу кезінде қате шықты."}, status_code=500)

    if not groups:
        return JSONResponse({"error": "Топтар табылмады."}, status_code=404)

    job_id = str(uuid.uuid4())
    request.session["last_job_id"] = job_id
    request.session["last_course_name"] = course_name
    request.session["last_study_month"] = study_month

    asyncio.create_task(_build_report_job(job_id, groups, token, month_num))
    return JSONResponse({"job_id": job_id, "total": len(groups)})


@router.get("/report/progress/{job_id}")
async def report_progress(job_id: str):
    p = PROGRESS.get(job_id)
    if not p:
        return JSONResponse({"error": "Job not found"}, status_code=404)
    return JSONResponse({"total": p["total"], "done": p["done"], "status": p["status"]})


@router.get("/report/result", response_class=HTMLResponse)
async def report_result(request: Request):
    from main import templates
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)

    job_id = request.session.get("last_job_id")
    course_name = request.session.get("last_course_name", "")
    study_month = request.session.get("last_study_month", "")

    p = PROGRESS.get(job_id) if job_id else None
    if not p or p["status"] != "done":
        return RedirectResponse("/dashboard", status_code=302)

    group_results = p["results"]
    tables = []
    for week in range(1, 5):
        rows = [metrics_to_row(gr["base"], gr["weeks"][week]) for gr in group_results]
        avg_row = compute_avg_row(rows)
        tables.append({
            "title": f"{week}-апта",
            "subtitle": f"{study_month} {week}-апта нәтижелері",
            "week": week,
            "rows": rows,
            "avg_row": avg_row,
        })

    monthly_rows = [metrics_to_row(gr["base"], gr["monthly"]) for gr in group_results]
    monthly_avg = compute_avg_row(monthly_rows)
    tables.append({
        "title": "Айлық қорытынды",
        "subtitle": f"{study_month} бойынша жалпы қорытынды",
        "week": "monthly",
        "rows": monthly_rows,
        "avg_row": monthly_avg,
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

    return templates.TemplateResponse("report.html", {
        "request": request,
        "tables": tables,
        "course_name": course_name,
        "study_month": study_month,
        "error": None,
        "group_count": len(group_results),
    })


# ── CSV Export ─────────────────────────────────────────────────────────────────

@router.get("/export")
async def export_csv(request: Request):
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)

    report_key = request.session.get("last_report_key")
    store = REPORT_STORE.get(report_key) if report_key else None

    if not store:
        return Response(content="Экспортқа деректер жоқ. Алдымен отчет жасаңыз.", status_code=400)

    tables = store["tables"]
    if not tables:
        return Response(content="Экспортқа деректер жоқ", status_code=400)

    output = io.StringIO()
    for table in tables:
        rows = list(table.get("rows", []))
        avg_row = table.get("avg_row")
        if not rows:
            continue
        output.write(f"# {table['title']}\n")
        df = pd.DataFrame(rows)
        if avg_row:
            df = pd.concat([df, pd.DataFrame([avg_row])], ignore_index=True)
        df.to_csv(output, index=False)
        output.write("\n")

    csv_bytes = output.getvalue().encode("utf-8-sig")
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=report.csv"},
    )


# ── Course months (динамикалық оқу айы) ───────────────────────────────────────

@router.get("/course-months")
async def course_months(request: Request, course_id: str):
    """Курстың бірінші тобынан оқу айларын қайтарады."""
    token = request.session.get("token")
    if not token:
        return JSONResponse({"error": "not logged in"}, status_code=401)
    try:
        async with httpx.AsyncClient() as client:
            groups = await api_get_async(
                f"{BASE_URL}/v1/headteacher/courses/{course_id}/groups",
                token, client,
            )
        if not groups:
            return JSONResponse({"months": list(range(1, 6))})  # fallback
        # Бірінші топтан айларды аламыз
        group_id = groups[0]["id"]
        async with httpx.AsyncClient() as client:
            data = await api_get_async(
                f"{BASE_URL}/v1/headteacher/groups/{group_id}/themes?week=1&month=1",
                token, client,
            )
        months = data.get("months", list(range(1, 6)))
        return JSONResponse({"months": sorted(months)})
    except Exception:
        return JSONResponse({"months": list(range(1, 6))})  # fallback


# ── Debug ──────────────────────────────────────────────────────────────────────

@router.get("/debug/themes")
async def debug_themes(request: Request, group_id: str, month: int = 2, week: int = 1):
    token = request.session.get("token")
    if not token:
        return JSONResponse({"error": "not logged in"})
    async with httpx.AsyncClient() as client:
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
            "themeId": theme_id,
            "lessons": [
                {
                    "name": item.get("name"),
                    "lessonType": item.get("lessonType"),
                    "studentsCount": item.get("studentsCount"),
                    "submittedCount": item.get("submittedCount"),
                }
                for item in (summary if isinstance(summary, list) else [])
            ],
        })
    return JSONResponse(result)