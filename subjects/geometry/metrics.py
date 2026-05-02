from subjects.informatics.metrics import *


def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = empty_metrics()

    # Берем обычную инфо-логику
    base = __import__(
        "subjects.informatics.metrics",
        fromlist=["extract_metrics"]
    ).extract_metrics(summary, theme_name_upper)

    m.update(base)

    # Геометрия: КОНСПЕКТ может быть отдельной темой
    if "КОНСПЕКТ" in theme_name_upper:
        k_pcts = []
        k_scores = []

        for item in summary:
            name = (item.get("name") or "").upper()

            if "КОНСПЕКТ" not in name:
                continue

            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)

            if p is not None:
                k_pcts.append(p)

            score = item.get("averageScore")
            if score is not None:
                k_scores.append(score)

        m["konspekt_pct"] = avg_of(k_pcts)
        m["konspekt_score"] = avg_of(k_scores)

    return m