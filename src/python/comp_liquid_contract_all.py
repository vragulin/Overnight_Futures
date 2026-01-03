# python
"""Compute daily liquid contracts for all symbols.

This script calls compute_liquid_contracts(conn, symbol_code, dry_run)
for every symbol in the `symbols` table.
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path
from typing import List

# Ensure project `src` root is on sys.path when this file is executed as a script.
_script_dir = Path(__file__).resolve().parent
_project_src = _script_dir.parent
if str(_project_src) not in sys.path:
    sys.path.insert(0, str(_project_src))

from python.config import DB_PATH
from python.comp_liquid_contract import compute_liquid_contracts


def all_symbols(conn: sqlite3.Connection) -> List[str]:
    cur = conn.cursor()
    cur.execute("SELECT symbol_code FROM symbols ORDER BY symbol_code")
    return [row[0] for row in cur.fetchall()]


def main(argv=None):
    parser = argparse.ArgumentParser(description="Compute daily liquid contracts for all symbols")
    parser.add_argument("--db", default=DB_PATH, help="path to sqlite database file")
    parser.add_argument("--dry-run", action="store_true", help="compute but do not modify the database")
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    conn = sqlite3.connect(args.db)
    try:
        symbols = all_symbols(conn)
        if not symbols:
            logging.info("No symbols found in the database.")
            return

        logging.info("Found %d symbols; processing...", len(symbols))
        for i, sym in enumerate(symbols, start=1):
            logging.info("(%d/%d) Processing symbol %s", i, len(symbols), sym)
            try:
                compute_liquid_contracts(conn, sym, dry_run=args.dry_run)
            except Exception:
                logging.exception("Error processing symbol %s; continuing to next symbol.", sym)
        logging.info("Finished processing all symbols.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()