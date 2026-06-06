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


def _avg_pct(*vals):
    """Average of the supplied numeric percentages, ignoring None / non-numeric.
    Returns a formatted "%" string, or "-" if there was nothing to average."""
    nums = []
    for v in vals:
        if v is None:
            continue
        try:
            nums.append(float(v))
        except Exception:
            continue
    if not nums:
        return "-"
    return f"{sum(nums) / len(nums):.1f}%"


# ── Per-subject row projections ───────────────────────────────────────────────
# Each builder accepts (base_dict, week_metrics_dict) and returns a dict whose
# keys are the column headers and whose values are formatted display strings.
# Order matters — Python 3.7+ preserves dict insertion order.

def _row_info(base, m):
    """ИНФО — uses informatics metrics: video / uy_pct / kzh_pct / quiz_pct /
    praktika_pct / sabak_pct / sabak_score."""
    praktika = m.get("praktika_pct")
    video    = m.get("video")
    uy       = m.get("uy_pct")
    kzh      = m.get("kzh_pct")
    quiz     = m.get("quiz_pct")
    sabak    = m.get("sabak_pct")
    return {
        "Жалпы оқушы саны":     base.get("Оқушы саны") or 0,
        "ПС қатысты":           _pct(praktika),
        "ОЖ көрді":             _pct(video),
        "ҮЖ салды":             _pct(uy),
        "ҚЖ салды":             _pct(kzh),
        "Куиз салды":           _pct(quiz),
        "Жалпы":                _avg_pct(praktika, video, uy, kzh, quiz, sabak),
        "СТ балл":              _num(m.get("sabak_score")),
        "СТ тапсырды":          _pct(sabak),
    }


def _row_mat(base, m):
    """МАТ — same metrics as ИНФО (math reuses informatics keys)."""
    praktika = m.get("praktika_pct")
    video    = m.get("video")
    uy       = m.get("uy_pct")
    kzh      = m.get("kzh_pct")
    quiz     = m.get("quiz_pct")
    sabak    = m.get("sabak_pct")
    return {
        "Жалпы оқушы саны":     base.get("Оқушы саны") or 0,
        "ПС қатысты":           _pct(praktika),
        "ОЖ көрді":             _pct(video),
        "ҮЖ салды":             _pct(uy),
        "ҚЖ салды":             _pct(kzh),
        "Куиз салды":           _pct(quiz),
        "Жалпы":                _avg_pct(praktika, video, uy, kzh, quiz, sabak),
        "СТ балл":              _num(m.get("sabak_score")),
        "СТ тапсырды":          _pct(sabak),
    }


def _row_simple(base, m):
    """ГЕОМ / МС — reuse informatics metric keys (video / uy_pct / kzh_pct /
    quiz_pct / praktika_pct / sabak_pct)."""
    praktika = m.get("praktika_pct")
    video    = m.get("video")
    uy       = m.get("uy_pct")
    kzh      = m.get("kzh_pct")
    quiz     = m.get("quiz_pct")
    sabak    = m.get("sabak_pct")
    return {
        "Жалпы оқушы саны":     base.get("Оқушы саны") or 0,
        "ПС қатысты":           _pct(praktika),
        "ОЖ көрді":             _pct(video),
        "ҮЖ салды":             _pct(uy),
        "ҚЖ салды":             _pct(kzh),
        "Куиз салды":           _pct(quiz),
        "Жалпы":                _avg_pct(praktika, video, uy, kzh, quiz, sabak),
        "СТ балл":              _num(m.get("sabak_score")),
        "СТ тапсырды":          _pct(sabak),
    }


def _row_tarih(base, m):
    """ТАРИХ — history metric keys (video / jumys_dapter_pct / quiz_pct /
    praktika_pct / sabak_pct / sabak_score). "ТТ салды" maps to jumys_dapter
    (the workbook/тематикалық submission %)."""
    praktika = m.get("praktika_pct")
    video    = m.get("video")
    tt       = m.get("jumys_dapter_pct")
    quiz     = m.get("quiz_pct")
    sabak    = m.get("sabak_pct")
    return {
        "Жалпы оқушы саны":     base.get("Оқушы саны") or 0,
        "ПС қатысты":           _pct(praktika),
        "ОЖ көрді":             _pct(video),
        "ТТ салды":             _pct(tt),
        "Куиз салды":           _pct(quiz),
        "Жалпы":                _avg_pct(praktika, video, tt, quiz, sabak),
        "СТ балл":              _num(m.get("sabak_score")),
        "СТ тапсырды":          _pct(sabak),
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

    # Pre-compute the canonical column list from the row_builder itself, so
    # all 3 тариф sections share identical column structure even when one of
    # them has no curators (no rows to derive columns from). Without this,
    # an empty VIP section would render with a different column count from
    # PREM/STAN and the table would look broken.
    sample_columns = [
        k for k in row_builder({}, {}).keys() if not k.startswith("__")
    ]

    sections = []
    has_any_data = False

    # Always emit all 3 тарифs in the canonical VPS_PRODUCTS order (VIP →
    # PREM → STAN). Previously we skipped тарифs that had zero curators,
    # which made the VIP section silently disappear whenever a VIP course
    # had no curator-assigned groups yet — users reported "нету VIP" as a
    # bug. Showing an empty section with a "—" placeholder is much clearer.
    for product in VPS_PRODUCTS:
        product_data = by_product.get(product["key"]) or {}

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
        agg = _build_agg_row(rows, sample_columns) if rows else None

        if rows:
            has_any_data = True

        sections.append({
            "product_label": product["label"],
            "product_key":   product["key"],
            "rows":          rows,
            "agg":           agg,
        })

    # If literally none of the 3 тарифs had any curators, the subject
    # contributes nothing useful — bail so the caller can omit it.
    if not has_any_data:
        return None

    return {
        "label":   VPS_SUFFIX_TO_SUBJECT.get(suffix, {}).get("label", suffix),
        "suffix":  suffix,
        "columns": sample_columns,
        "sections": sections,
    }


def _assemble_view(results, week_filter=None):
    """Reshape raw PROGRESS["results"] into the tabbed structure for the template.

    When ``week_filter`` is 1..4, only that week's tab is emitted and the
    monthly aggregate is suppressed (it would just duplicate the single
    week's data, which is confusing).

    Each subject_table has {label, columns, sections}, and each section has
    {product_label, rows[], agg}.
    """
    # Index results by suffix → {product_key: data}
    by_suffix = {}
    for r in results:
        sfx = r.get("suffix")
        if sfx:
            by_suffix.setdefault(sfx, {})[r.get("product_key")] = r

    weeks_to_emit = [week_filter] if week_filter in (1, 2, 3, 4) else [1, 2, 3, 4]

    tabs = []
    for week in weeks_to_emit:
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

    # Monthly tab is only meaningful when all 4 weeks were processed.
    if week_filter is None:
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
        "request":         request,
        "packs":           list(VPS_PACKS.keys()),
        "month":           VPS_DEFAULT_MONTH,
        # Months offered to the user in the picker. Same range as SMART so
        # the two dashboards behave consistently.
        "available_months": [1, 2, 3, 4, 5],
        **_vps_ctx(),
    })


def _parse_vps_month(month_raw: str) -> int:
    """Accepts '2', '2-ай' or '' and returns an int month, falling back to
    VPS_DEFAULT_MONTH when nothing usable was provided."""
    if not month_raw:
        return VPS_DEFAULT_MONTH
    try:
        return int(str(month_raw).replace("-ай", "").strip())
    except ValueError:
        return VPS_DEFAULT_MONTH


def _parse_vps_week(week_raw: str):
    """Returns None for 'all' / empty / garbage, or int 1..4."""
    if not week_raw or week_raw == "all":
        return None
    try:
        wf = int(week_raw)
        return wf if wf in (1, 2, 3, 4) else None
    except ValueError:
        return None


@router.post("/report", response_class=HTMLResponse)
async def vps_report(
    request: Request,
    pack:  str = Form(...),
    month: str = Form(""),
    week:  str = Form("all"),
):
    """Render the loading page that polls /vps/report/progress until done."""
    from main import templates

    month_num = _parse_vps_month(month)
    week_filter = _parse_vps_week(week)
    week_label = f" · {week_filter}-апта" if week_filter else ""

    return templates.TemplateResponse("loading.html", {
        "request": request,
        "title":             "VPS отчёт жасалуда…",
        "subtitle_html":     f"<strong>{pack}</strong> · барлық тарифтер · {month_num}-ай{week_label}",
        "unit":              "Курс",
        "start_url":         "/vps/report/start",
        "progress_url_base": "/vps/report/progress",
        "result_url":        "/vps/report/result",
        "hidden_fields":     {
            "pack":  pack,
            "month": str(month_num),
            "week":  week or "all",
        },
        "stages": [
            {"p": 0,  "icon": "📥", "title": "Курстар жүктелуде…"},
            {"p": 20, "icon": "📊", "title": "Тарифтер өңделуде…"},
            {"p": 45, "icon": "🧮", "title": "Метрикалар есептелуде…"},
            {"p": 75, "icon": "📈", "title": "Кестелер құрастырылуда…"},
            {"p": 90, "icon": "✨", "title": "Қорытынды дайындалуда…"},
        ],
    })


@router.post("/report/start")
async def vps_report_start(
    request: Request,
    pack:  str = Form(...),
    month: str = Form(""),
    week:  str = Form("all"),
):
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)
    if pack not in VPS_PACKS:
        return JSONResponse({"error": f"Unknown pack: {pack}"}, status_code=400)

    month_num = _parse_vps_month(month)
    week_filter = _parse_vps_week(week)

    job_id = str(uuid.uuid4())
    request.session["last_vps_job_id"]      = job_id
    request.session["last_vps_pack"]        = pack
    request.session["last_vps_week_filter"] = week_filter  # None or 1..4

    # month_num here is study_month — when the user picks 4-ай they mean
    # "show me the cohort's lesson data for the 4th month of studying",
    # not "find a cohort that enrolled in April" (which would be empty).
    asyncio.create_task(build_vps_report_job(
        job_id, pack, month_num, token, week_filter=week_filter,
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

    week_filter = request.session.get("last_vps_week_filter")
    view = _assemble_view(p.get("results", []), week_filter=week_filter)

    return templates.TemplateResponse("vps_report.html", {
        "request":   request,
        "pack_name": p.get("pack_name", ""),
        "month":    p.get("month_num", VPS_DEFAULT_MONTH),
        "tabs":     view["tabs"],
        **_vps_ctx(),
    })
