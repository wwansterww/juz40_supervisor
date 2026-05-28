from typing import Optional
from subjects.common import safe_pct, avg_of, fmt, empty_metrics, merge_metrics, is_quiz_theme
from subjects.common import compute_avg_row as _compute_avg_row

METRIC_KEYS = [
    "conspect_pct",

    "uy_pct",
    "uy_score",

    "qj_pct",
    "qj_score",

    "quiz_pct",
    "quiz_score",

    "sabak_pct",
    "sabak_score",
]

PERCENT_COLS = [
    "Конспект %",
    "Үй жұмысы %",
    "Қатемен жұмыс %",
    "Quiz %",
    "Сабақ тапсыру %",
]

SCORE_COLS = [
    "Үй жұмысы балл",
    "Қатемен жұмыс балл",
    "Quiz балл",
    "Сабақ тапсыру балл",
]


def empty_metrics_kazakh_language():
    return empty_metrics(METRIC_KEYS)


def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = empty_metrics_kazakh_language()

    # КОНСПЕКТ
    if "КОНСПЕКТ" in theme_name_upper:
        vals = []

        for item in summary:
            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)

            if p is not None:
                vals.append(p)

        m["conspect_pct"] = avg_of(vals)

    # ҮЙ ЖҰМЫСЫ
    # ҮЙ ЖҰМЫСЫ + ҚАТЕМЕН ЖҰМЫС
    if "ҮЙ ЖҰМЫСЫ" in theme_name_upper:
        uy_pcts, uy_scores = [], []
        qj_pcts, qj_scores = [], []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            name = (item.get("name") or "").upper()

            # Үй жұмысы parent
            if "ҮЙ ЖҰМЫСЫ" in name and "ҚАТЕМЕН" not in name:
                sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
                sub = item.get("submittedCount") or 0

                p = safe_pct(sub, sc)
                if p is not None:
                    uy_pcts.append(p)

                score = item.get("averageScore")
                if score is not None:
                    uy_scores.append(score)

            # Қатемен жұмыс child
            for child in item.get("children", []):
                child_name = (child.get("name") or "").upper()

                if "ҚАТЕМЕН" not in child_name:
                    continue

                c_sc = child.get("studentsCount") or child.get("totalStudentsCount") or 0
                c_sub = child.get("submittedCount") or 0

                cp = safe_pct(c_sub, c_sc)
                if cp is not None:
                    qj_pcts.append(cp)

                c_score = child.get("averageScore")
                if c_score is not None:
                    qj_scores.append(c_score)

        m["uy_pct"] = avg_of(uy_pcts)
        m["uy_score"] = avg_of(uy_scores)
        m["qj_pct"] = avg_of(qj_pcts)
        m["qj_score"] = avg_of(qj_scores)

    # ҚАТЕМЕН ЖҰМЫС
    if "ҚАТЕМЕН" in theme_name_upper:
        pcts, scores = [], []

        for item in summary:
            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)

            if p is not None:
                pcts.append(p)

            score = item.get("averageScore")

            if score is not None:
                scores.append(score)

        m["qj_pct"] = avg_of(pcts)
        m["qj_score"] = avg_of(scores)

    # QUIZ
    if is_quiz_theme(theme_name_upper):
        pcts, scores = [], []

        for item in summary:
            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)

            if p is not None:
                pcts.append(p)

            score = item.get("averageScore")

            if score is not None:
                scores.append(score)

        m["quiz_pct"] = avg_of(pcts)
        m["quiz_score"] = avg_of(scores)

        # САБАҚ ТАПСЫРУ
    if "САБАҚ ТАПСЫРУ" in theme_name_upper:
        pcts, scores = [], []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)
            if p is not None:
                pcts.append(p)

            score = item.get("averageScore")
            if score is not None:
                scores.append(score)

        m["sabak_pct"] = avg_of(pcts)
        m["sabak_score"] = avg_of(scores)

    return m


def merge_metrics_kazakh_language(all_metrics: list) -> dict:
    return merge_metrics(all_metrics, METRIC_KEYS)


def metrics_to_row(base: dict, m: dict) -> dict:
    return {
        **base,

        "Конспект %": fmt(m.get("conspect_pct")),

        "Үй жұмысы %": fmt(m.get("uy_pct")),
        "Үй жұмысы балл": fmt(m.get("uy_score")),

        "Қатемен жұмыс %": fmt(m.get("qj_pct")),
        "Қатемен жұмыс балл": fmt(m.get("qj_score")),

        "Quiz %": fmt(m.get("quiz_pct")),
        "Quiz балл": fmt(m.get("quiz_score")),

        "Сабақ тапсыру %": fmt(m.get("sabak_pct")),
        "Сабақ тапсыру балл": fmt(m.get("sabak_score")),
    }


def compute_avg_row(rows: list) -> Optional[dict]:
    return _compute_avg_row(rows, PERCENT_COLS, SCORE_COLS)