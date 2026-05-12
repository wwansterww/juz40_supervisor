from typing import Optional
from subjects.common import safe_pct, avg_of, fmt, empty_metrics, merge_metrics
from subjects.common import compute_avg_row as _compute_avg_row

METRIC_KEYS = [
    "video",
    "jumys_dapter_pct", "jumys_dapter_score",
    "quiz_pct",
    "karta_pct", "karta_score",
    "karta_kzh_pct", "karta_kzh_score",
    "sabak_pct", "sabak_score",
]

PERCENT_COLS = [
    "Видео сабақ %",
    "Жұмыс дәптері %",
    "Quiz %",
    "Картамен жұмыс %",
    "Картамен жұмыс ҚЖ %",
    "Сабақ тапсыру %",
]
SCORE_COLS = [
    "Жұмыс дәптері балл",
    "Картамен жұмыс балл",
    "Картамен жұмыс ҚЖ балл",
    "Сабақ тапсыру балл",
]


def empty_metrics_geo():
    return empty_metrics(METRIC_KEYS)


def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = empty_metrics_geo()

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
        m["jumys_dapter_pct"] = avg_of(pcts)
        m["jumys_dapter_score"] = avg_of(scores)

    # КУИЗ ТЕСТ
    if "КУИЗ" in theme_name_upper or "QUIZ" in theme_name_upper or "QUIZIZ" in theme_name_upper:
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

    # КАРТАМЕН ЖҰМЫС
    if "КАРТАМЕН ЖҰМЫС" in theme_name_upper:
        kp, ks = [], []
        kzh_p, kzh_s = [], []
        for item in summary:
            name = (item.get("name") or "").upper()
            pid = item.get("parentId")

            # Главный урок
            if pid is None and "КАРТАМЕН ЖҰМЫС" in name and "ҚЖ" not in name:
                sc = item.get("studentsCount") or 0
                sub = item.get("submittedCount") or 0
                p = safe_pct(sub, sc)
                if p is not None:
                    kp.append(p)
                score = item.get("averageScore")
                if score is not None:
                    ks.append(score)

                # ҚЖ в children
                for child in item.get("children", []):
                    cn = (child.get("name") or "").upper()
                    if "ҚЖ" in cn:
                        total_sc = child.get("totalStudentsCount") or 0
                        c_sub = child.get("submittedCount") or 0
                        cp = safe_pct(c_sub, total_sc)
                        if cp is not None:
                            kzh_p.append(cp)
                        c_score = child.get("averageScore")
                        if c_score is not None:
                            kzh_s.append(c_score)

        m["karta_pct"] = avg_of(kp)
        m["karta_score"] = avg_of(ks)
        m["karta_kzh_pct"] = avg_of(kzh_p)
        m["karta_kzh_score"] = avg_of(kzh_s)

    # САБАҚ ТАПСЫРУ
    if "САБАҚ ТАПСЫРУ" in theme_name_upper:
        sp, ss = [], []
        for item in summary:
            if item.get("parentId") is not None:
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

    return m


def merge_metrics_geo(all_metrics: list) -> dict:
    return merge_metrics(all_metrics, METRIC_KEYS)


def metrics_to_row(base: dict, m: dict) -> dict:
    return {
        **base,
        "Видео сабақ %": fmt(m.get("video")),
        "Жұмыс дәптері %": fmt(m.get("jumys_dapter_pct")),
        "Жұмыс дәптері балл": fmt(m.get("jumys_dapter_score")),
        "Quiz %": fmt(m.get("quiz_pct")),
        "Картамен жұмыс %": fmt(m.get("karta_pct")),
        "Картамен жұмыс балл": fmt(m.get("karta_score")),
        "Картамен жұмыс ҚЖ %": fmt(m.get("karta_kzh_pct")),
        "Картамен жұмыс ҚЖ балл": fmt(m.get("karta_kzh_score")),
        "Сабақ тапсыру %": fmt(m.get("sabak_pct")),
        "Сабақ тапсыру балл": fmt(m.get("sabak_score")),
    }


def compute_avg_row(rows: list) -> Optional[dict]:
    return _compute_avg_row(rows, PERCENT_COLS, SCORE_COLS)
