# python
"""
Load `data/futs_roll_info.csv` into existing `rollover_rules` table.
Skips the header row. Columns expected:
  Symbol,Description,RolloverDays,RolloverType
"""
from __future__ import annotations

import csv
import logging
import sqlite3
import sys
from pathlib import Path

# Ensure project `src` root is on sys.path so we can import config
_script_dir = Path(__file__).resolve().parent
_project_src = _script_dir.parent
if str(_project_src) not in sys.path:
    sys.path.insert(0, str(_project_src))

from python.config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def csv_path() -> Path:
    # script at src/python -> project root = parents[2]
    return Path(__file__).resolve().parents[2] / "data" / "futs_roll_info.csv"


def parse_csv(p: Path) -> list[tuple[str, str, int, str]]:
    rows: list[tuple[str, str, int, str]] = []
    with p.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        for i, r in enumerate(reader):
            if not r:
                continue
            # skip header (first row) or rows where first cell looks like header
            if i == 0 and r[0].strip().lower() in ("symbol", "symbol_code"):
                continue
            symbol = r[0].strip()
            if not symbol:
                continue
            desc = r[1].strip() if len(r) > 1 else ""
            try:
                days = int(r[2]) if len(r) > 2 and r[2].strip() != "" else 0
            except ValueError:
                days = 0
            rtype = r[3].strip() if len(r) > 3 else ""
            rows.append((symbol, desc, days, rtype))
    return rows


def upsert_into_db(db_path: str, rows: list[tuple[str, str, int, str]]) -> int:
    if not rows:
        return 0
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT INTO rollover_rules (symbol_code, description, rollover_days, rollover_type)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol_code) DO UPDATE SET
                description   = excluded.description,
                rollover_days = excluded.rollover_days,
                rollover_type = excluded.rollover_type;
            """,
            rows,
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def main():
    p = csv_path()
    if not p.exists():
        logging.error("CSV not found at `%s`.", p)
        return 2

    rows = parse_csv(p)
    if not rows:
        logging.error("No rows parsed from `%s`.", p)
        return 2

    count = upsert_into_db(DB_PATH, rows)
    logging.info("Inserted/updated %d rows into `rollover_rules` from `%s`.", count, p)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())