from datetime import date

# Оқу жылының ағын айлар реті (тамыздан маусымға дейін)
STREAM_MONTH_ORDER = [8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6]

MONTH_NUM_TO_NAME = {
    1: "ҚАҢТАР", 2: "АҚПАН", 3: "НАУРЫЗ", 4: "СӘУІР", 5: "МАМЫР",
    6: "МАУСЫМ", 7: "ШІЛДЕ", 8: "ТАМЫЗ", 9: "ҚЫРКҮЙЕК",
    10: "ҚАЗАН", 11: "ҚАРАША", 12: "ЖЕЛТОҚСАН",
}

# Курс типіне қарай API продукттар тізімі
SECTION_TYPE_PRODUCTS = {
    "SMART":     ["SMART", "EXPRESS", "INTENSIVE"],
    "TURBO":     ["TURBO"],
    "VPS":       ["SMART_STANDARD", "SMART_PREMIUM", "SMART_VIP"],
    "JUNIOR":    ["JUNIOR"],
    "GENIUS":    ["GENIUS"],
    "PAKET":     ["PAKET"],
}


def get_current_report_number() -> int:
    """Қазіргі уақыт бойынша отчёт нөмірін анықтайды (1-11)."""
    current_month = date.today().month
    if current_month in STREAM_MONTH_ORDER:
        return STREAM_MONTH_ORDER.index(current_month) + 1
    return 1


def get_active_streams_for_report(report_num: int, all_stream_months: list) -> list:
    """
    Отчёт нөміріне сәйкес белсенді потоктарды қайтарады.
    Қайтарылатын тізім: [{"stream_month": 8, "study_month": 3}, ...]
    """
    result = []
    for stream_month in all_stream_months:
        if stream_month not in STREAM_MONTH_ORDER:
            continue
        position = STREAM_MONTH_ORDER.index(stream_month) + 1
        study_month = report_num - position + 1
        if 1 <= study_month <= 5:
            result.append({
                "stream_month": stream_month,
                "study_month": study_month,
            })
    return result
