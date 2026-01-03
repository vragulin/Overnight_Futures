import re
from pathlib import Path

MONTH_CODES = "FGHJKMNQUVXZ"  # standard futures month codes


def parse_contract_filename(filename: str):
    # Strip extension
    stem = Path(filename).stem  # e.g. 'ADF18'
    # Continuous contracts have no year → ignore anything that doesn’t end with 3 chars [LetterDigitDigit]
    if len(stem) < 3:
        return None

    month_code = stem[-3]  # e.g. 'F'
    year_two = stem[-2:]  # e.g. '18'

    if month_code not in MONTH_CODES or not year_two.isdigit():
        # Probably continuous contract or something else → ignore
        return None

    symbol_code = stem[:-3]  # e.g. 'AD'
    year = 2000 + int(year_two)  # works for 2000–2099

    return symbol_code, month_code, year

