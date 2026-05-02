import asyncio
import uuid

import httpx
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

from config import BASE_URL, INFORMATICS_SUBJECT_ID
from cache import api_get_async
from store import PROGRESS, REPORT_STORE
from subjects.informatics.metrics import compute_avg_row
from subjects.informatics.section.constants import (
    STREAM_MONTH_ORDER,
    MONTH_NUM_TO_NAME,
    SECTION_TYPE_PRODUCTS,
    get_current_report_number,
    get_active_streams_for_report,
)
from subjects.informatics.section.builder import build_sliding_section_report_job

router = APIRouter()


@router.post("/section-report", response_class=HTMLResponse)
async def section_report(
    request: Request,
    course_type: str = Form(...),
):
    from main import templates
    return templates.TemplateResponse("section_loading.html", {
        "request": request,
        "course_type": course_type,
        "study_month": "",
    })


@router.post("/section-report/start")
async def section_report_start(
    request: Request,
    course_type: str = Form(...),
):
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)

    report_num = get_current_report_number()

    # Барлық ағын айларын аламыз
    try:
        async with httpx.AsyncClient() as client:
            stream_months_resp = await api_get_async(
                f"{BASE_URL}/v2/headteacher/subjects/{INFORMATICS_SUBJECT_ID}/course-month",
                token, client,
            )
        all_stream_months = stream_months_resp if isinstance(stream_months_resp, list) else []
    except Exception:
        all_stream_months = STREAM_MONTH_ORDER

    active_streams = get_active_streams_for_report(report_num, all_stream_months)
    if not active_streams:
        return JSONResponse({"error": "Белсенді потоктар табылмады."}, status_code=404)

    products = SECTION_TYPE_PRODUCTS.get(course_type.upper(), [course_type.upper()])

    # Ең жаңа жылды аламыз
    try:
        async with httpx.AsyncClient() as client:
            years_resp = await api_get_async(
                f"{BASE_URL}/v2/headteacher/subjects/{INFORMATICS_SUBJECT_ID}/course-year",
                token, client,
            )
        latest_year = max(years_resp) if isinstance(years_resp, list) and years_resp else 2025
    except Exception:
        latest_year = 2025

    # Белсенді потоктар бойынша курстарды жинаймыз
    stream_courses = []
    async with httpx.AsyncClient() as client:
        for stream_info in active_streams:
            sm = stream_info["stream_month"]
            study_m = stream_info["study_month"]
            try:
                urls = [
                    f"{BASE_URL}/v2/headteacher/subjects/{INFORMATICS_SUBJECT_ID}/courses"
                    f"?size=200&page=0&searchWord=&sort=year,DESC&sort=month,DESC&product={p}&month={sm}"
                    for p in products
                ]
                responses = await asyncio.gather(
                    *[api_get_async(url, token, client) for url in urls],
                    return_exceptions=True,
                )
                courses_for_stream = []
                for resp in responses:
                    if not isinstance(resp, Exception):
                        courses_for_stream.extend(resp.get("content", []))

                courses_for_stream = [
                    c for c in courses_for_stream
                    if c.get("year") == latest_year
                    and "(КОПИЯ" not in c.get("name", "").upper()
                ]

                if courses_for_stream:
                    stream_courses.append({
                        "stream_month": sm,
                        "study_month": study_m,
                        "courses": courses_for_stream,
                    })
            except Exception:
                pass

    if not stream_courses:
        return JSONResponse({"error": "Курстар табылмады."}, status_code=404)

    job_id = str(uuid.uuid4())
    request.session["last_section_job_id"] = job_id
    request.session["last_section_type"] = course_type
    request.session["last_section_report_num"] = report_num

    asyncio.create_task(build_sliding_section_report_job(job_id, stream_courses, token))
    return JSONResponse({"job_id": job_id, "total": sum(len(s["courses"]) for s in stream_courses)})


@router.get("/section-report/result", response_class=HTMLResponse)
async def section_report_result(request: Request):
    from main import templates
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)

    job_id = request.session.get("last_section_job_id")
    course_type = request.session.get("last_section_type", "")
    report_num = request.session.get("last_section_report_num", 1)

    p = PROGRESS.get(job_id) if job_id else None
    if not p or p["status"] != "done":
        return RedirectResponse("/dashboard", status_code=302)

    stream_results = p["results"]

    all_rows = []
    for sr in stream_results:
        all_rows.extend(sr["rows"])
    overall_avg = compute_avg_row(all_rows) if all_rows else None
    if overall_avg:
        overall_avg.pop("Куратор", None)
        overall_avg["Поток"] = "⌀ Жалпы орта"

    report_title = f"{course_type} — №{report_num} отчет"

    REPORT_STORE[job_id] = {
        "tables": [
            {
                "title": f"{MONTH_NUM_TO_NAME.get(sr['stream_month'], str(sr['stream_month']))} ({sr['study_month']}-ай)",
                "rows": sr["rows"],
                "avg_row": sr["avg_row"],
            }
            for sr in stream_results
        ],
        "title": report_title,
    }
    request.session["last_report_key"] = job_id

    return templates.TemplateResponse("section_report.html", {
        "request": request,
        "stream_results": stream_results,
        "overall_avg": overall_avg,
        "course_type": course_type,
        "report_num": report_num,
        "report_title": report_title,
        "month_names": MONTH_NUM_TO_NAME,
        "group_count": len(all_rows),
    })
