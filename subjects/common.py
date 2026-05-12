from typing import Optional

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

def empty_metrics(keys: list) -> dict:
    return {k: None for k in keys}

def merge_metrics(all_metrics: list, keys: list) -> dict:
    merged = {}
    for k in keys:
        vals = [mm[k] for mm in all_metrics if mm.get(k) is not None]
        merged[k] = avg_of(vals)
    return merged

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
        except (ValueError, TypeError):
            pass
    if total_students == 0:
        return "-"
    return round(total_submitted / total_students * 100, 1)

def compute_avg_row(rows: list, percent_cols: list, score_cols: list) -> Optional[dict]:
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
    for col in percent_cols:
        avg_row[col] = weighted_avg(rows, col, "Оқушы саны")
    for col in score_cols:
        vals = []
        for r in rows:
            v = r.get(col)
            if v != "-" and v is not None:
                try:
                    vals.append(float(v))
                except (ValueError, TypeError):
                    pass
        avg_row[col] = round(sum(vals) / len(vals), 1) if vals else "-"
    return avg_row