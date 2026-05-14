import os

BASE_URL = "https://api.juz40-edu.kz"

INFORMATICS_SUBJECT_ID = "6e172165-57c2-4b01-9fd1-70ccca7b96a7"
MATH_SUBJECT_ID        = "11c81c50-c914-4030-8083-e5d4bfe6e6d0"
GEOMETRY_SUBJECT_ID    = "aefcbf13-8928-40a5-bddb-1b5c7eac2e07"
MS_SUBJECT_ID          = "e6d6f884-5f5a-46c0-9b5a-929051b9a3d8"
PHYSICS_SUBJECT_ID     = "0b14d605-8adb-436d-8953-e2472d4ff048"
CHEMISTRY_SUBJECT_ID   = "24beb7d0-dc5f-4e2e-a66c-c44b51af9e67"
GEOGRAPHY_SUBJECT_ID = "3019bfe4-8e9e-4c9a-9059-626d6dff3d48"
KUKYK_SUBJECT_ID       = "79d6a013-68f6-4829-a75f-f1618fc9c244"
HISTORY_SUBJECT_ID = "2f9a8bf5-4a39-4c5f-aa32-4c7ae09521b2"

SECRET_KEY = os.environ.get("SECRET_KEY", "change-me-in-production-please")

CACHE_TTL = 600  # 10 минут

COURSE_TYPES = ["SMART", "TURBO", "VPS", "EXPRESS", "JUNIOR", "INTENSIVE", "GENIUS", "PAKET"]

COURSE_TYPE_TO_PRODUCTS = {
    "SMART":     ["SMART"],
    "TURBO":     ["TURBO"],
    "VPS":       ["SMART_STANDARD", "SMART_PREMIUM", "SMART_VIP"],
    "EXPRESS":   ["EXPRESS"],
    "JUNIOR":    ["JUNIOR"],
    "INTENSIVE": ["INTENSIVE"],
    "GENIUS":    ["GENIUS"],
    "PAKET":     ["PAKET"],
}

STREAM_MONTHS = [
    "АҚПАН", "НАУРЫЗ", "СӘУІР", "МАМЫР", "МАУСЫМ",
    "ШІЛДЕ", "ТАМЫЗ", "ҚЫРКҮЙЕК", "ҚАЗАН", "ҚАРАША", "ЖЕЛТОҚСАН"
]

MONTH_NAME_TO_NUM = {
    "АҚПАН": 2, "НАУРЫЗ": 3, "СӘУІР": 4, "МАМЫР": 5,
    "МАУСЫМ": 6, "ШІЛДЕ": 7, "ТАМЫЗ": 8, "ҚЫРКҮЙЕК": 9,
    "ҚАЗАН": 10, "ҚАРАША": 11, "ЖЕЛТОҚСАН": 12,
}

STUDY_MONTHS = ["1-ай", "2-ай", "3-ай", "4-ай", "5-ай"]

TYPE_NAME_KEYWORDS = {
    "SMART":     ["SMART"],
    "TURBO":     ["TURBO", " T ", " T-"],
    "VPS":       ["STANDARD", "PREMIUM", "VIP", "STAN", "PREM"],
    "EXPRESS":   ["EXPRESS"],
    "JUNIOR":    ["JUNIOR"],
    "INTENSIVE": ["INTENSIVE"],
    "GENIUS":    ["GENIUS"],
    "PAKET":     ["PAKET"],
}

TYPE_EXCLUDE_KEYWORDS = {
    "SMART": ["STANDARD", "PREMIUM", "VIP", "STAN", "PREM"],
}