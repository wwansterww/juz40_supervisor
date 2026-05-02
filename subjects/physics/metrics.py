from subjects.informatics.metrics import *


PHYSICS_EXTRA_KEYS = [
    "theory_pct",
    "theory_score",
    "theory_kzh_pct",
    "theory_kzh_score",
]


def empty_metrics():
    m = {k: None for k in METRIC_KEYS}
    for k in PHYSICS_EXTRA_KEYS:
        m[k] = None
    return m


def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = super_extract_metrics(summary, theme_name_upper)

    # ФИЗИКА: КУИЗ / QUIZ / КВИЗ
    if "КУИЗ" in theme_name_upper or "QUIZ" in theme_name_upper or "КВИЗ" in theme_name_upper:
        qp, qs = [], []

        for item in summary:
            name = (item.get("name") or "").upper()

            if (
                "КУИЗ" in name
                or "QUIZ" in name
                or "КВИЗ" in name
                or "ТЕСТ" in name
            ):
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

    # ФИЗИКА: ТЕОРИЯЛЫҚ ТАПСЫРМА
    if "ТЕОРИЯЛЫҚ ТАПСЫРМА" in theme_name_upper:
        theory_p, theory_s = [], []
        theory_kzh_p, theory_kzh_s = [], []

        for item in summary:
            name = (item.get("name") or "").upper()
            pid = item.get("parentId")

            is_main_theory = (
                "ТЕОРИЯЛЫҚ ТАПСЫРМА" in name
                and "ҚЖ" not in name
                and "КЖ" not in name
                and pid is None
            )

            if is_main_theory:
                sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
                sub = item.get("submittedCount") or 0

                p = safe_pct(sub, sc)
                if p is not None:
                    theory_p.append(p)

                score = item.get("averageScore")
                if score is not None:
                    theory_s.append(score)

                for child in item.get("children", []):
                    child_name = (child.get("name") or "").upper()

                    if "ҚЖ" in child_name or "КЖ" in child_name or "ҚАТЕМЕН" in child_name:
                        child_total = child.get("totalStudentsCount") or child.get("studentsCount") or 0
                        child_sub = child.get("submittedCount") or 0

                        child_pct = safe_pct(child_sub, child_total)
                        if child_pct is not None:
                            theory_kzh_p.append(child_pct)

                        child_score = child.get("averageScore")
                        if child_score is not None:
                            theory_kzh_s.append(child_score)

        m["theory_pct"] = avg_of(theory_p)
        m["theory_score"] = avg_of(theory_s)
        m["theory_kzh_pct"] = avg_of(theory_kzh_p)
        m["theory_kzh_score"] = avg_of(theory_kzh_s)

    return m


def merge_metrics(all_metrics: list) -> dict:
    keys = METRIC_KEYS + PHYSICS_EXTRA_KEYS
    merged = {}

    for k in keys:
        vals = [mm[k] for mm in all_metrics if mm.get(k) is not None]
        merged[k] = avg_of(vals)

    return merged


def metrics_to_row(base: dict, m: dict) -> dict:
    row = super_metrics_to_row(base, m)

    row["Теориялық тапсырма %"] = fmt(m.get("theory_pct"))
    row["Теориялық тапсырма балл"] = fmt(m.get("theory_score"))
    row["Теориялық тапсырма ҚЖ %"] = fmt(m.get("theory_kzh_pct"))
    row["Теориялық тапсырма ҚЖ балл"] = fmt(m.get("theory_kzh_score"))

    return row


def weighted_avg(rows, pct_key, count_key):
    total_students = 0
    total_submitted = 0

    for row in rows:
        pct = row.get(pct_key)
        count = row.get(count_key)

        if pct == "-" or pct is None or not count:
            continue

        try:
            pct = float(pct)
            count = float(count)
            total_submitted += pct / 100 * count
            total_students += count
        except Exception:
            pass

    if total_students == 0:
        return "-"

    return round(total_submitted / total_students * 100, 1)


def compute_avg_row(rows: list):
    if not rows:
        return None

    avg_row = {
        "Поток": "—",
        "Куратор": "⌀ Орта көрсеткіш",
        "Оқушы саны": sum(
            r["Оқушы саны"] for r in rows
            if isinstance(r.get("Оқушы саны"), (int, float))
        ),
    }

    percent_cols = [
        "Видео сабақ %",
        "Конспект %",
        "Үй жұмысы %",
        "ҚЖ %",
        "Quiz %",
        "Практикалық сабақ %",
        "Сабақ тапсыру %",
        "Теориялық тапсырма %",
        "Теориялық тапсырма ҚЖ %",
    ]

    score_cols = [
        "Конспект балл",
        "Үй жұмысы балл",
        "ҚЖ балл",
        "Quiz балл",
        "Сабақ тапсыру балл",
        "Теориялық тапсырма балл",
        "Теориялық тапсырма ҚЖ балл",
    ]

    for col in percent_cols:
        avg_row[col] = weighted_avg(rows, col, "Оқушы саны")

    for col in score_cols:
        vals = []
        for r in rows:
            v = r.get(col)
            if v != "-" and v is not None:
                try:
                    vals.append(float(v))
                except Exception:
                    pass

        avg_row[col] = round(sum(vals) / len(vals), 1) if vals else "-"

    return avg_row


from subjects.informatics.metrics import extract_metrics as super_extract_metrics
from subjects.informatics.metrics import metrics_to_row as super_metrics_to_row