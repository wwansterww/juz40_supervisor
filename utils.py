_LATIN_TO_CYR = {
    'A': 'А', 'B': 'В', 'C': 'С', 'E': 'Е', 'H': 'Н',
    'I': 'І', 'K': 'К', 'M': 'М', 'O': 'О', 'P': 'Р',
    'S': 'С',
    'T': 'Т', 'X': 'Х', 'Y': 'У',
    'a': 'а', 'c': 'с', 'e': 'е', 'o': 'о', 'p': 'р',
    'x': 'х', 'y': 'у',
}

def normalize(s: str) -> str:
    return ''.join(_LATIN_TO_CYR.get(c, c) for c in s).upper()