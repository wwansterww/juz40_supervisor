METRIC_KEYS = [
    "video", "konspekt_pct", "konspekt_score",
    "uy_pct", "uy_score",
    "kzh_pct", "kzh_score",
    "quiz_pct", "quiz_score",
    "praktika_pct",
    "sabak_pct", "sabak_score",
    "theory_pct", "theory_score",
    "theory_kzh_pct", "theory_kzh_score",
]


def safe_pct(submitted, total):
    if not total:
        return None
    return min(round(submitted / total * 100, 1), 100)


def avg_of(values):
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return round(sum(vals) / len(vals), 1)


def fmt(val):
    return "-" if val is None else val


def empty_metrics():
    return {k: None for k in METRIC_KEYS}


def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = empty_metrics()

    # СТ ҚЖ — игнорировать полностью
    if "СТ ҚЖ" in theme_name_upper:
        return m

    # ВИДЕОСАБАҚ / КОНСПЕКТ
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

    # ҮЙ ЖҰМЫСЫ — ҚОСЫМША ҮЙ ЖҰМЫСЫ игнорировать
    if "ҮЙ ЖҰМЫСЫ" in theme_name_upper and "ҚОСЫМША" not in theme_name_upper:
        uy_p, uy_s, kzh_p, kzh_s = [], [], [], []
        for item in summary:
            name = (item.get("name") or "").upper()
            pid = item.get("parentId")

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

    # QUIZIZZ / QUIZ / ҚАЙТАЛАУ ТЕСТ
    if (
        "QUIZIZ" in theme_name_upper
        or "QUIZ" in theme_name_upper
        or "КВИЗ" in theme_name_upper
        or "КУИЗ" in theme_name_upper
        or "ҚАЙТАЛАУ ТЕСТ" in theme_name_upper
    ):
        qp, qs = [], []
        for item in summary:
            name = (item.get("name") or "").upper()
            if (
                "QUIZIZ" in name or "QUIZ" in name
                or "КВИЗ" in name or "КУИЗ" in name
                or "ТЕСТ" in name
            ):
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

    # ПРАКТИКАЛЫҚ САБАҚ
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
    if "АБАҚ ТАПСЫРУ" in theme_name_upper:
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

    # ТЕОРИЯЛЫҚ ТАПСЫРМА — children ішінде ҚЖ бар
    if "ТЕОРИЯЛЫҚ ТАПСЫРМА" in theme_name_upper:
        theory_p, theory_s = [], []
        theory_kzh_p, theory_kzh_s = [], []
        for item in summary:
            name = (item.get("name") or "").upper()
            is_main = (
                "ТЕОРИЯЛЫҚ ТАПСЫРМА" in name
                and "ҚЖ" not in name
                and "КЖ" not in name
                and item.get("parentId") is None
            )
            if is_main:
                sc = item.get("studentsCount") or 0
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
                        child_total = child.get("totalStudentsCount") or 0
                        child_sub = child.get("submittedCount") or 0
                        cp = safe_pct(child_sub, child_total)
                        if cp is not None:
                            theory_kzh_p.append(cp)
                        child_score = child.get("averageScore")
                        if child_score is not None:
                            theory_kzh_s.append(child_score)
        m["theory_pct"] = avg_of(theory_p)
        m["theory_score"] = avg_of(theory_s)
        m["theory_kzh_pct"] = avg_of(theory_kzh_p)
        m["theory_kzh_score"] = avg_of(theory_kzh_s)

    return m


def merge_metrics(all_metrics: list) -> dict:
    merged = {}
    for k in METRIC_KEYS:
        vals = [mm[k] for mm in all_metrics if mm.get(k) is not None]
        merged[k] = avg_of(vals)
    return merged


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
        "Теориялық тапсырма %": fmt(m.get("theory_pct")),
        "Теориялық тапсырма балл": fmt(m.get("theory_score")),
        "Теориялық тапсырма ҚЖ %": fmt(m.get("theory_kzh_pct")),
        "Теориялық тапсырма ҚЖ балл": fmt(m.get("theory_kzh_score")),
        "Практикалық сабақ %": fmt(m.get("praktika_pct")),
        "Сабақ тапсыру %": fmt(m.get("sabak_pct")),
        "Сабақ тапсыру балл": fmt(m.get("sabak_score")),
    }


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
        "Теориялық тапсырма %",
        "Теориялық тапсырма ҚЖ %",
        "Практикалық сабақ %",
        "Сабақ тапсыру %",
    ]

    score_cols = [
        "Конспект балл",
        "Үй жұмысы балл",
        "ҚЖ балл",
        "Quiz балл",
        "Теориялық тапсырма балл",
        "Теориялық тапсырма ҚЖ балл",
        "Сабақ тапсыру балл",
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