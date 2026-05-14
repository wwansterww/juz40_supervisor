from typing import Optional
from subjects.common import safe_pct, avg_of, fmt, empty_metrics, merge_metrics
from subjects.common import compute_avg_row as _compute_avg_row

METRIC_KEYS = [
    "video",
    "jumys_pct", "jumys_score",
    "quiz_pct", "quiz_score",
    "praktika_pct",
    "sabak_pct", "sabak_score",
]

PERCENT_COLS = [
    "Видео сабақ %",
    "Жұмыс дәптері %",
    "Quiz %",
    "Практикалық сабақ %",
    "Сабақ тапсыру %",
]
SCORE_COLS = [
    "Жұмыс дәптері балл",
    "Quiz балл",
    "Сабақ тапсыру балл",
]


def empty_metrics_kukyk():
    return empty_metrics(METRIC_KEYS)


def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = empty_metrics_kukyk()

    # ВИДЕОСАБАҚ
    if "ВИДЕОСАБАҚ" in theme_name_upper:
        video_vals = []
        for item in summary:
            if item.get("lessonType") == "LECTURE":
                v = item.get("averageVideoViewing")
                if v is not None:
                    video_vals.append(min(v, 100))
        m["video"] = avg_of(video_vals)

    # ЖҰМЫС ДӘПТЕРІ
    if "ЖҰМЫС ДӘПТЕРІ" in theme_name_upper:
        pcts, scores = [], []
        for item in summary:
            if item.get("parentId") is not None:
                continue
            sc = item.get("studentsCount") or 0
            sub = item.get("submittedCount") or 0
            p = safe_pct(sub, sc)
            if p is not None:
                pcts.append(p)
            score = item.get("averageScore")
            if score is not None:
                scores.append(score)
        m["jumys_pct"] = avg_of(pcts)
        m["jumys_score"] = avg_of(scores)

    # КУИЗ ТЕСТ
    if "КУИЗ" in theme_name_upper or "QUIZ" in theme_name_upper:
        qp, qs = [], []
        for item in summary:
            sc = item.get("studentsCount") or 0
            sub = item.get("submittedCount") or 0
            p = safe_pct(sub, sc)
            if p is not None:
                qp.append(p)
            score = item.get("averageScore")
            if score is not None:
                qs.append(score)
        m["quiz_pct"] = avg_of(qp)
        m["quiz_score"] = avg_of(qs)

    # ПРАКТИКАЛЫҚ САБАҚ — аптасына 2 рет, avg аламыз
    if "ПРАКТИКАЛЫҚ" in theme_name_upper:
        pr = []
        for item in summary:
            sc = item.get("studentsCount") or 0
            sub = item.get("submittedCount") or 0
            p = safe_pct(sub, sc)
            if p is not None:
                pr.append(p)
        m["praktika_pct"] = avg_of(pr)

    # САБАҚ ТАПСЫРУ
    if "САБАҚ ТАПСЫРУ" in theme_name_upper:
        sp, ss = [], []
        for item in summary:
            pid = item.get("parentId")
            if pid is not None:
                continue
            sc = item.get("studentsCount") or 0
            sub = item.get("submittedCount") or 0
            p = safe_pct(sub, sc)
            if p is not None:
                sp.append(p)
            score = item.get("averageScore")
            if score is not None:
                ss.append(score)
        m["sabak_pct"] = avg_of(sp)
        m["sabak_score"] = avg_of(ss)

    # ҚАЙТАЛАУ ТЕСТ — САБАҚ ТАПСЫРУ жоқ болса fallback
    if "ҚАЙТАЛАУ ТЕСТ" in theme_name_upper:
        sp, ss = [], []
        for item in summary:
            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0
            p = safe_pct(sub, sc)
            if p is not None:
                sp.append(p)
            score = item.get("averageScore")
            if score is not None:
                ss.append(score)
        m["sabak_pct"] = avg_of(sp)
        m["sabak_score"] = avg_of(ss)

    return m


def merge_metrics_kukyk(all_metrics: list) -> dict:
    return merge_metrics(all_metrics, METRIC_KEYS)


def metrics_to_row(base: dict, m: dict) -> dict:
    return {
        **base,
        "Видео сабақ %": fmt(m.get("video")),
        "Жұмыс дәптері %": fmt(m.get("jumys_pct")),
        "Жұмыс дәптері балл": fmt(m.get("jumys_score")),
        "Quiz %": fmt(m.get("quiz_pct")),
        "Quiz балл": fmt(m.get("quiz_score")),
        "Практикалық сабақ %": fmt(m.get("praktika_pct")),
        "Сабақ тапсыру %": fmt(m.get("sabak_pct")),
        "Сабақ тапсыру балл": fmt(m.get("sabak_score")),
    }


def compute_avg_row(rows: list) -> Optional[dict]:
    return _compute_avg_row(rows, PERCENT_COLS, SCORE_COLS)
