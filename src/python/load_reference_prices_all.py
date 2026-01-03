
"""
Load daily reference prices for all symbols present in `liquid_contract_daily`.
Re-uses `process_symbol` from `load_reference_prices.py`.
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Iterable, List

# Ensure project `src` root is on sys.path when executed
_script_dir = Path(__file__).resolve().parent
_project_src = _script_dir.parent
if str(_project_src) not in sys.path:
    sys.path.insert(0, str(_project_src))

from python.config import DB_PATH
from python.load_reference_prices import process_symbol


def fetch_all_symbols(cur: sqlite3.Cursor) -> List[str]:
    cur.execute(
        """
        SELECT DISTINCT symbol_code
        FROM liquid_contract_daily
        ORDER BY symbol_code
        """
    )
    return [r[0] for r in cur.fetchall()]


def process_symbols(conn: sqlite3.Connection, symbols: Iterable[str], dry_run: bool = False) -> int:
    total = 0
    for sym in symbols:
        logging.info("Processing symbol %s", sym)
        try:
            count = process_symbol(conn, sym, dry_run=dry_run)
            logging.info("Symbol %s: %d rows (dry_run=%s)", sym, count, dry_run)
            total += count
        except Exception:
            logging.exception("Failed processing symbol %s (continuing)", sym)
    return total


def main(argv=None):
    parser = argparse.ArgumentParser(description="Compute daily reference prices for all symbols in liquid_contract_daily")
    parser.add_argument("--db", default=DB_PATH, help="path to sqlite database file (default from config)")
    parser.add_argument(
        "--symbols",
        nargs="*",
        help="Optional list of symbol codes to process (e.g. --symbols ES GC). If omitted all symbols from liquid_contract_daily are processed."
    )
    parser.add_argument("--dry-run", action="store_true", help="compute but do not modify the database (runs inside a transaction and rolls back)")
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    conn = sqlite3.connect(args.db)
    try:
        cur = conn.cursor()
        if args.symbols:
            symbols = list(args.symbols)
        else:
            symbols = fetch_all_symbols(cur)
            if not symbols:
                logging.info("No symbols found in `liquid_contract_daily`.")
                return

        logging.info("Will process %d symbols", len(symbols))

        if args.dry_run:
            # run inside a transaction and rollback to avoid writes
            conn.isolation_level = None
            cur.execute("BEGIN")
            try:
                total = process_symbols(conn, symbols, dry_run=True)
            finally:
                cur.execute("ROLLBACK")
            logging.info("Dry run complete: would have written %d rows (summed across symbols).", total)
        else:
            total = process_symbols(conn, symbols, dry_run=False)
            conn.commit()
            logging.info("Finished: %d rows written (summed across symbols).", total)
    finally:
        conn.close()


if __name__ == "__main__":
    main()