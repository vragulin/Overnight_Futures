import argparse
import logging
import sys
import sqlite3
from pathlib import Path
from datetime import datetime

# Ensure project `src` root is on sys.path when this file is executed as a script.
# Place this before any `from python.* import ...` lines in `src/python/load_contracts.py`.
_script_dir = Path(__file__).resolve().parent
_project_src = _script_dir.parent  # one level up: `src`
if str(_project_src) not in sys.path:
    sys.path.insert(0, str(_project_src))

from python.config import FUTURES_DATA_FOLDER, DB_PATH
from python.parse_contract_name import parse_contract_filename


def ensure_symbol(conn, symbol_code, description=None):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO symbols (symbol_code, description)
        VALUES (?, COALESCE(?, ?))
        ON CONFLICT(symbol_code) DO NOTHING;
    """, (symbol_code, description, symbol_code))
    conn.commit()


def ensure_contract(conn, symbol_code, month_code, year, filename):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO contracts (symbol_code, month_code, year, kibot_filename)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(kibot_filename) DO UPDATE SET
            symbol_code=excluded.symbol_code,
            month_code=excluded.month_code,
            year=excluded.year
        RETURNING contract_id;
    """, (symbol_code, month_code, year, filename))
    (contract_id,) = cur.fetchone()
    return contract_id


def load_bars_for_file(conn, filepath: Path, contract_id: int):
    cur = conn.cursor()
    with filepath.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            date_str, time_str, o, h, l, c, vol = line.split(",")
            # Parse
            dt = datetime.strptime(f"{date_str} {time_str}", "%m/%d/%Y %H:%M")
            ts = dt.strftime("%Y-%m-%d %H:%M")  # adjust to UTC later if needed

            volume = int(vol) if vol.strip() != "" else None

            cur.execute("""
                INSERT OR REPLACE INTO bars_5min
                    (contract_id, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            """, (contract_id, ts, float(o), float(h), float(l), float(c), volume))

    conn.commit()


def load_all(root_dir, db_path):
    conn = sqlite3.connect(db_path)
    for path in Path(root_dir).glob("*.txt"):
        parsed = parse_contract_filename(path.name)
        if not parsed:
            continue  # skip continuous
        symbol_code, month_code, year = parsed
        ensure_symbol(conn, symbol_code)
        contract_id = ensure_contract(conn, symbol_code, month_code, year, path.name)
        load_bars_for_file(conn, path, contract_id)
        logging.info("Successfully uploaded file: %s", path.name)  # Log progress
    conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Load kibot contract files into the database.")
    parser.add_argument(
        "--root-dir",
        default=FUTURES_DATA_FOLDER,
        help="Directory containing contract text files (default: value from config)."
    )
    parser.add_argument(
        "--db-path",
        default=DB_PATH,
        help="SQLite database path (default: value from config)."
    )
    parser.add_argument("--dry-run", action="store_true", help="List files but do not modify the database.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    root = Path(args.root_dir)
    if not root.exists() or not root.is_dir():
        logging.error("Root directory `%s` does not exist or is not a directory.", root)
        sys.exit(2)

    if args.dry_run:
        logging.info("Dry run: listing matching files in `%s`", root)
        for path in root.glob("*.txt"):
            logging.info("Found: %s", path.name)
        return

    logging.info("Starting load from `%s` into `%s`", root, args.db_path)
    load_all(root, args.db_path)
    logging.info("Finished loading.")


if __name__ == "__main__":
    main()
