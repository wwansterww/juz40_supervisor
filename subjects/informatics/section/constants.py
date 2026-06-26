from datetime import date

# Ағын айлар реті мен аттары — жалғыз қайнар көзі config.py-да
# (оқу жылы шілдеден басталады). Бұл жерде тек қайта экспорттаймыз.
from config import STREAM_MONTH_ORDER, MONTH_NUM_TO_NAME

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
    """Қазіргі уақыт бойынша отчёт нөмірін анықтайды (1-12)."""
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
