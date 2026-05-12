from typing import Optional
from subjects.common import safe_pct, avg_of, fmt, empty_metrics, merge_metrics
from subjects.common import compute_avg_row as _compute_avg_row
from subjects.informatics.metrics import METRIC_KEYS as INFO_KEYS, extract_metrics as info_extract

EXTRA_KEYS = ["theory_pct", "theory_score", "theory_kzh_pct", "theory_kzh_score"]
METRIC_KEYS = INFO_KEYS + EXTRA_KEYS

PERCENT_COLS = [
    "Видео сабақ %", "Конспект %", "Үй жұмысы %",
    "ҚЖ %", "Quiz %", "Практикалық сабақ %", "Сабақ тапсыру %",
    "Теориялық тапсырма %", "Теориялық тапсырма ҚЖ %",
]
SCORE_COLS = [
    "Конспект балл", "Үй жұмысы балл", "ҚЖ балл", "Quiz балл",
    "Сабақ тапсыру балл", "Теориялық тапсырма балл", "Теориялық тапсырма ҚЖ балл",
]

def empty_metrics_phys():
    return empty_metrics(METRIC_KEYS)

def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = info_extract(summary, theme_name_upper)
    for k in EXTRA_KEYS:
        m.setdefault(k, None)

    if any(kw in theme_name_upper for kw in ("КУИЗ", "QUIZ", "КВИЗ")):
        qp, qs = [], []
        for item in summary:
            name = (item.get("name") or "").upper()
            if any(kw in name for kw in ("КУИЗ", "QUIZ", "КВИЗ", "ТЕСТ")):
                sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
                sub = item.get("submittedCount") or 0
                p = safe_pct(sub, sc)
                if p is not None:
                    qp.append(p)
                score = item.get("averageScore")
                if score is not None:
                    qs.append(score)
        m["quiz_pct"] = avg_of(qp)
        m["quiz_score"] = avg_of(qs)

    if "ТЕОРИЯЛЫҚ ТАПСЫРМА" in theme_name_upper:
        theory_p, theory_s, theory_kzh_p, theory_kzh_s = [], [], [], []
        for item in summary:
            name = (item.get("name") or "").upper()
            pid = item.get("parentId")
            is_main = (
                "ТЕОРИЯЛЫҚ ТАПСЫРМА" in name
                and "ҚЖ" not in name and "КЖ" not in name
                and pid is None
            )
            if is_main:
                sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
                sub = item.get("submittedCount") or 0
                p = safe_pct(sub, sc)
                if p is not None:
                    theory_p.append(p)
                score = item.get("averageScore")
                if score is not None:
                    theory_s.append(score)
                for child in item.get("children", []):
                    cn = (child.get("name") or "").upper()
                    if "ҚЖ" in cn or "КЖ" in cn or "ҚАТЕМЕН" in cn:
                        total = child.get("totalStudentsCount") or child.get("studentsCount") or 0
                        c_sub = child.get("submittedCount") or 0
                        cp = safe_pct(c_sub, total)
                        if cp is not None:
                            theory_kzh_p.append(cp)
                        cs = child.get("averageScore")
                        if cs is not None:
                            theory_kzh_s.append(cs)
        m["theory_pct"] = avg_of(theory_p)
        m["theory_score"] = avg_of(theory_s)
        m["theory_kzh_pct"] = avg_of(theory_kzh_p)
        m["theory_kzh_score"] = avg_of(theory_kzh_s)

    return m

def merge_metrics_phys(all_metrics: list) -> dict:
    return merge_metrics(all_metrics, METRIC_KEYS)

def metrics_to_row(base: dict, m: dict) -> dict:
    from subjects.informatics.metrics import metrics_to_row as info_row
    row = info_row(base, m)
    row["Теориялық тапсырма %"] = fmt(m.get("theory_pct"))
    row["Теориялық тапсырма балл"] = fmt(m.get("theory_score"))
    row["Теориялық тапсырма ҚЖ %"] = fmt(m.get("theory_kzh_pct"))
    row["Теориялық тапсырма ҚЖ балл"] = fmt(m.get("theory_kzh_score"))
    return row

def compute_avg_row_phys(rows: list) -> Optional[dict]:
    return _compute_avg_row(rows, PERCENT_COLS, SCORE_COLS)