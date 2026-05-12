from typing import Optional
from subjects.common import safe_pct, avg_of, fmt, empty_metrics, merge_metrics
from subjects.common import compute_avg_row as _compute_avg_row

METRIC_KEYS = [
    "video", "konspekt_pct", "konspekt_score",
    "uy_pct", "uy_score",
    "kzh_pct", "kzh_score",
    "quiz_pct", "quiz_score",
    "praktika_pct",
    "sabak_pct", "sabak_score",
]

PERCENT_COLS = [
    "Видео сабақ %", "Конспект %", "Үй жұмысы %",
    "ҚЖ %", "Quiz %", "Практикалық сабақ %", "Сабақ тапсыру %",
]
SCORE_COLS = [
    "Конспект балл", "Үй жұмысы балл", "ҚЖ балл",
    "Quiz балл", "Сабақ тапсыру балл",
]

def empty_metrics_info():
    return empty_metrics(METRIC_KEYS)

def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = empty_metrics_info()

    if "ВИДЕОСАБАҚ" in theme_name_upper or "КОНСПЕКТ" in theme_name_upper:
        video_vals, k_pcts, k_scores = [], [], []
        for item in summary:
            lt = item.get("lessonType", "")
            name = (item.get("name") or "").upper()
            if lt == "LECTURE":
                v = item.get("averageVideoViewing")
                if v is not None:
                    video_vals.append(min(v, 100))
            if "КОНСПЕКТ" in name:
                sc = item.get("studentsCount") or 0
                sub = item.get("submittedCount") or 0
                p = safe_pct(sub, sc)
                if p is not None:
                    k_pcts.append(p)
                score = item.get("averageScore")
                if score is not None:
                    k_scores.append(score)
        m["video"] = avg_of(video_vals)
        m["konspekt_pct"] = avg_of(k_pcts)
        m["konspekt_score"] = avg_of(k_scores)

    if "ҮЙ ЖҰМЫСЫ" in theme_name_upper or "ТАҚЫРЫПТЫҚ ТАПСЫРМА" in theme_name_upper:
        uy_p, uy_s, kzh_p, kzh_s = [], [], [], []
        is_tt = "ТАҚЫРЫПТЫҚ ТАПСЫРМА" in theme_name_upper
        for item in summary:
            name = (item.get("name") or "").upper()
            pid = item.get("parentId")
            if is_tt:
                is_main = pid is None and "ҚОСЫМША" not in name
            else:
                is_main = "ҮЙ ЖҰМЫСЫ" in name and "ҚОСЫМША" not in name and pid is None
            if is_main:
                sc = item.get("studentsCount") or 0
                sub = item.get("submittedCount") or 0
                p = safe_pct(sub, sc)
                if p is not None:
                    uy_p.append(p)
                score = item.get("averageScore")
                if score is not None:
                    uy_s.append(score)
                for child in item.get("children", []):
                    cn = (child.get("name") or "").upper()
                    if "ҚАТЕМЕН ЖҰМЫС" in cn or "ҚЖ" in cn:
                        total_sc = child.get("totalStudentsCount") or 0
                        c_sub = child.get("submittedCount") or 0
                        cp = safe_pct(c_sub, total_sc)
                        if cp is not None:
                            kzh_p.append(cp)
                        c_score = child.get("averageScore")
                        if c_score is not None:
                            kzh_s.append(c_score)
        m["uy_pct"] = avg_of(uy_p)
        m["uy_score"] = avg_of(uy_s)
        m["kzh_pct"] = avg_of(kzh_p)
        m["kzh_score"] = avg_of(kzh_s)

    if (
            "QUIZ" in theme_name_upper
            or "КУИЗ" in theme_name_upper
            or "КВИЗ" in theme_name_upper
            or "ТЕСТ" in theme_name_upper
            or "TEST" in theme_name_upper
    ):
        qp, qs = [], []
        for item in summary:
            name = (item.get("name") or "").upper()
            if (
                    "QUIZ" in name
                    or "КУИЗ" in name
                    or "КВИЗ" in name
                    or "ТЕСТ" in name
                    or "TEST" in name
            ) and "САБАҚ" not in name and "ҚАЙТАЛАУ" not in name:
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

    if "ПРАКТИКАЛЫҚ" in theme_name_upper:
        pr = []
        for item in summary:
            sc = item.get("studentsCount") or 0
            sub = item.get("submittedCount") or 0
            p = safe_pct(sub, sc)
            if p is not None:
                pr.append(p)
        m["praktika_pct"] = avg_of(pr)

    # САБАҚ ТАПСЫРУ — бар болса соны аламыз
    if "САБАҚ ТАПСЫРУ" in theme_name_upper:
        sp, ss = [], []
        for item in summary:
            name = (item.get("name") or "").upper()
            pid = item.get("parentId")
            if "ҚЖ" in name or pid is not None:
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

    # ҚАЙТАЛАУ ТЕСТ — САБАҚ ТАПСЫРУ жоқ болса fallback ретінде
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

def merge_metrics_info(all_metrics: list) -> dict:
    return merge_metrics(all_metrics, METRIC_KEYS)

def metrics_to_row(base: dict, m: dict) -> dict:
    return {
        **base,
        "Видео сабақ %": fmt(m.get("video")),
        "Конспект %": fmt(m.get("konspekt_pct")),
        "Конспект балл": fmt(m.get("konspekt_score")),
        "Үй жұмысы %": fmt(m.get("uy_pct")),
        "Үй жұмысы балл": fmt(m.get("uy_score")),
        "ҚЖ %": fmt(m.get("kzh_pct")),
        "ҚЖ балл": fmt(m.get("kzh_score")),
        "Quiz %": fmt(m.get("quiz_pct")),
        "Quiz балл": fmt(m.get("quiz_score")),
        "Практикалық сабақ %": fmt(m.get("praktika_pct")),
        "Сабақ тапсыру %": fmt(m.get("sabak_pct")),
        "Сабақ тапсыру балл": fmt(m.get("sabak_score")),
    }

def compute_avg_row_info(rows: list) -> Optional[dict]:
    return _compute_avg_row(rows, PERCENT_COLS, SCORE_COLS)