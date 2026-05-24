"""HTTP routes for VPS combined reports."""

import asyncio
import uuid

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

from config import (
    VPS_PRODUCTS, VPS_SUFFIX_TO_SUBJECT, VPS_PACKS,
    VPS_WEEK_SUBJECTS, VPS_DEFAULT_MONTH,
)
from store import PROGRESS
from concurrency import get_queue_position
from subjects.vps.builder import build_vps_report_job

router = APIRouter()


# ── Template helpers ──────────────────────────────────────────────────────────

def _vps_ctx():
    """Common template context — keeps the navbar / active-tab UI consistent."""
    return {
        "active_subject": "vps",
        "subject_name":   "VPS",
        "subject_prefix": "/vps",
    }


def _pct(v):
    if v is None or v == "-" or v == "":
        return "-"
    try:
        return f"{float(v):.1f}%"
    except Exception:
        return "-"


def _num(v, dp=2):
    if v is None or v == "-" or v == "":
        return "-"
    try:
        return f"{float(v):.{dp}f}"
    except Exception:
        return "-"


# ── Per-subject row projections ───────────────────────────────────────────────
# Each builder accepts (base_dict, week_metrics_dict) and returns a dict whose
# keys are the column headers and whose values are formatted display strings.
# Order matters — Python 3.7+ preserves dict insertion order.

def _row_info(base, m):
    """ИНФО — 11 columns."""
    return {
        "Жалпы оқушы саны":     base.get("Оқушы саны") or 0,
        "ПС қатысты":           _pct(m.get("praktika_pct")),
        "ОЖ көрді":             _pct(m.get("video_pct")),
        "ҮЖ салды":             _pct(m.get("ujum_pct")),
        "ҚЖ салды":             _pct(m.get("kjum_pct")),
        "Куиз салды":           _pct(m.get("quiz_pct")),
        "Жалпы":                _pct(m.get("monthly_pct")),
        "Теориум":              "-",
        "Платформа тазалылғы":  "-",
        "СТ балл":              _num(m.get("sabak_score")),
        "СТ тапсырды":          _pct(m.get("sabak_pct")),
    }


def _row_mat(base, m):
    """МАТ — 10 columns (no Теориум)."""
    return {
        "Жалпы оқушы саны":     base.get("Оқушы саны") or 0,
        "ПС қатысты":           _pct(m.get("praktika_pct")),
        "ОЖ көрді":             _pct(m.get("video_pct")),
        "ҮЖ салды":             _pct(m.get("ujum_pct")),
        "ҚЖ салды":             _pct(m.get("kjum_pct")),
        "Куиз салды":           _pct(m.get("quiz_pct")),
        "Жалпы":                _pct(m.get("monthly_pct")),
        "Платформа тазалылғы":  "-",
        "СТ балл":              _num(m.get("sabak_score")),
        "СТ тапсырды":          _pct(m.get("sabak_pct")),
    }


def _row_simple(base, m):
    """ГЕОМ / МС — 7 columns."""
    return {
        "Жалпы оқушы саны":     base.get("Оқушы саны") or 0,
        "ПС қатысты":           _pct(m.get("praktika_pct")),
        "ОЖ көрді":             _pct(m.get("video_pct")),
        "ҮЖ салды":             _pct(m.get("ujum_pct")),
        "ҚЖ салды":             _pct(m.get("kjum_pct")),
        "Куиз салды":           _pct(m.get("quiz_pct")),
        "Жалпы":                _pct(m.get("monthly_pct")),
    }


def _row_tarih(base, m):
    """ТАРИХ — 4 columns. ТТ салды = the lesson-submission %."""
    return {
        "Жалпы оқушы саны":     base.get("Оқушы саны") or 0,
        "ПС қатысты":           _pct(m.get("praktika_pct")),
        "ОЖ көрді":             _pct(m.get("video_pct")),
        "ТТ салды":             _pct(m.get("sabak_pct")),
    }


ROW_BUILDER_BY_SUFFIX = {
    "ИНФО":  _row_info,
    "МАТ":   _row_mat,
    "ГЕО":   _row_simple,
    "МС":    _row_simple,
    "ТАРИХ": _row_tarih,
}


# ── Aggregation ───────────────────────────────────────────────────────────────

def _build_agg_row(rows, columns):
    """Aggregate row for a тариф section.

    • "Жалпы оқушы саны" → sum of student counts.
    • Percent columns   → student-count-weighted mean.
    • Numeric columns   → simple mean (no good weight signal).
    """
    if not rows:
        return None

    student_counts = [int(r.get("Жалпы оқушы саны") or 0) for r in rows]
    total_students = sum(student_counts)

    agg = {}
    for col in columns:
        if col == "Жалпы оқушы саны":
            agg[col] = total_students
            continue

        vals = [r.get(col) for r in rows]
        numeric_with_w = []
        for i, v in enumerate(vals):
            if v in (None, "-", ""):
                continue
            s = str(v).rstrip("%").replace(",", ".").strip()
            try:
                numeric_with_w.append((float(s), student_counts[i] or 1))
            except Exception:
                pass

        if not numeric_with_w:
            agg[col] = "-"
            continue

        # Treat the column as a percentage if at least one displayed value
        # ended with "%". Use student-count weighting in either case — for
        # raw scores it's still a reasonable "fair-share" average.
        is_pct = any(isinstance(v, str) and v.endswith("%") for v in vals if v)
        total_w = sum(w for _, w in numeric_with_w) or 1
        weighted = sum(v * w for v, w in numeric_with_w) / total_w
        agg[col] = f"{weighted:.1f}%" if is_pct else f"{weighted:.2f}"
    return agg


def _build_subject_table(suffix, by_product, metric_key, week_num=None):
    """Compose one subject's table for a given tab.

    metric_key: "weeks" (uses week_num) or "monthly".
    Returns a dict with `columns` and `sections` (one per тариф), or None if
    there's no data anywhere across all 3 тарифs.
    """
    row_builder = ROW_BUILDER_BY_SUFFIX.get(suffix, _row_simple)

    sections = []
    sample_columns = None

    for product in VPS_PRODUCTS:
        product_data = by_product.get(product["key"])
        if not product_data:
            continue

        rows = []
        for group in product_data.get("groups", []):
            base = group.get("base", {})
            if metric_key == "weeks":
                m = (group.get("weeks", {}) or {}).get(week_num, {}) or {}
            else:
                m = group.get("monthly", {}) or {}
            row = row_builder(base, m)
            row["__curator"] = base.get("Куратор", "") or "—"
            rows.append(row)

        rows.sort(key=lambda r: r.get("__curator", ""))
        cols = [k for k in (rows[0].keys() if rows else []) if not k.startswith("__")]
        if cols and sample_columns is None:
            sample_columns = cols

        agg = _build_agg_row(rows, cols) if rows else None

        # Skip totally empty (no curators, no aggregate) sections.
        if not rows and not agg:
            continue
        sections.append({
            "product_label": product["label"],
            "product_key":   product["key"],
            "rows":          rows,
            "agg":           agg,
        })

    if not sections:
        return None

    # If sample_columns is still None it means every section was empty — but
    # we filtered those out above, so this can only happen if the row_builder
    # returned an empty dict (shouldn't). Fall back to the suffix's own
    # default builder columns.
    if not sample_columns:
        sample_columns = list(row_builder({}, {}).keys())

    return {
        "label":   VPS_SUFFIX_TO_SUBJECT.get(suffix, {}).get("label", suffix),
        "suffix":  suffix,
        "columns": sample_columns,
        "sections": sections,
    }


def _assemble_view(results):
    """Reshape raw PROGRESS["results"] into the tabbed structure for the template.

    Output:
      {
        "tabs": [
          {"title": "1-апта", "subtitle": "тақ апта",  "week": 1, "subject_tables": [...]},
          {"title": "2-апта", "subtitle": "жұп апта", "week": 2, "subject_tables": [...]},
          {"title": "3-апта", "subtitle": "тақ апта",  "week": 3, "subject_tables": [...]},
          {"title": "4-апта", "subtitle": "жұп апта", "week": 4, "subject_tables": [...]},
          {"title": "Айлық қорытынды", "subtitle": "...", "week": "monthly", "subject_tables": [...]},
        ]
      }
    Each subject_table has {label, columns, sections}, and each section has
    {product_label, rows[], agg}.
    """
    # Index results by suffix → {product_key: data}
    by_suffix = {}
    for r in results:
        sfx = r.get("suffix")
        if sfx:
            by_suffix.setdefault(sfx, {})[r.get("product_key")] = r

    all_suffixes_in_packs = list(by_suffix.keys())

    tabs = []
    for week in (1, 2, 3, 4):
        parity = "odd" if week % 2 == 1 else "even"
        active = VPS_WEEK_SUBJECTS[parity]

        subject_tables = []
        for suffix in active:
            by_product = by_suffix.get(suffix, {})
            st = _build_subject_table(suffix, by_product, "weeks", week_num=week)
            if st:
                subject_tables.append(st)

        tabs.append({
            "title":          f"{week}-апта",
            "subtitle":       "тақ апта" if parity == "odd" else "жұп апта",
            "week":           week,
            "subject_tables": subject_tables,
        })

    # Monthly tab: every subject that participated in the pack.
    monthly_tables = []
    # Preserve a stable order: odd-week subjects first, then even-week.
    monthly_order = list(dict.fromkeys(VPS_WEEK_SUBJECTS["odd"] + VPS_WEEK_SUBJECTS["even"]))
    for suffix in monthly_order:
        if suffix not in by_suffix:
            continue
        st = _build_subject_table(suffix, by_suffix[suffix], "monthly")
        if st:
            monthly_tables.append(st)

    tabs.append({
        "title":          "📊 Айлық қорытынды",
        "subtitle":       "Айдың жалпы қорытындысы",
        "week":           "monthly",
        "subject_tables": monthly_tables,
    })

    return {"tabs": tabs}


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/dashboard", response_class=HTMLResponse)
async def vps_dashboard(request: Request):
    from main import templates
    if not request.session.get("token"):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("vps_dashboard.html", {
        "request": request,
        "packs":   list(VPS_PACKS.keys()),
        "month":   VPS_DEFAULT_MONTH,
        **_vps_ctx(),
    })


@router.post("/report", response_class=HTMLResponse)
async def vps_report(request: Request, pack: str = Form(...)):
    """Render the loading page that polls /vps/report/progress until done."""
    from main import templates
    return templates.TemplateResponse("loading.html", {
        "request": request,
        "title":             "VPS отчёт жасалуда…",
        "subtitle_html":     f"<strong>{pack}</strong> · барлық тарифтер · {VPS_DEFAULT_MONTH}-ай",
        "unit":              "Курс",
        "start_url":         "/vps/report/start",
        "progress_url_base": "/vps/report/progress",
        "result_url":        "/vps/report/result",
        "hidden_fields":     {"pack": pack},
        "stages": [
            {"p": 0,  "icon": "📥", "title": "Курстар жүктелуде…"},
            {"p": 20, "icon": "📊", "title": "Тарифтер өңделуде…"},
            {"p": 45, "icon": "🧮", "title": "Метрикалар есептелуде…"},
            {"p": 75, "icon": "📈", "title": "Кестелер құрастырылуда…"},
            {"p": 90, "icon": "✨", "title": "Қорытынды дайындалуда…"},
        ],
    })


@router.post("/report/start")
async def vps_report_start(request: Request, pack: str = Form(...)):
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)
    if pack not in VPS_PACKS:
        return JSONResponse({"error": f"Unknown pack: {pack}"}, status_code=400)

    job_id = str(uuid.uuid4())
    request.session["last_vps_job_id"] = job_id
    request.session["last_vps_pack"]   = pack

    asyncio.create_task(build_vps_report_job(
        job_id, pack, VPS_DEFAULT_MONTH, token,
    ))

    return JSONResponse({
        "job_id": job_id,
        "total":  len(VPS_PACKS.get(pack, [])) * len(VPS_PRODUCTS),
    })


@router.get("/report/progress/{job_id}")
async def vps_report_progress(job_id: str):
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
async def vps_report_result(request: Request):
    from main import templates
    if not request.session.get("token"):
        return RedirectResponse("/", status_code=302)

    job_id = request.session.get("last_vps_job_id")
    if not job_id:
        return RedirectResponse("/vps/dashboard", status_code=302)

    p = await PROGRESS.aget(job_id)
    if not p or p.get("status") != "done":
        return RedirectResponse("/vps/dashboard", status_code=302)

    view = _assemble_view(p.get("results", []))

    return templates.TemplateResponse("vps_report.html", {
        "request":   request,
        "pack_name": p.get("pack_name", ""),
        "month":    p.get("month_num", VPS_DEFAULT_MONTH),
        "tabs":     view["tabs"],
        **_vps_ctx(),
    })
