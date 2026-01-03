# File: `src/python/load_reference_prices.py`
"""
loader for daily reference prices (open, close, prev close)
===============================================
Writes into `daily_reference_prices` (symbol_code, trade_date, price_open, price_close, prev_close).

Notes for readers unfamiliar with Kibot data:
- Kibot agent text files contain rows: Date, Time, Open, High, Low, Close, Volume
  (e.g. "12/30/2025,16:00,6945.25,6946.00,6940.50,6943.00,1234").
  See: https://www.kibot.com/
- In this project `bars_5min.timestamp` is the bar *start* time in the format
  'YYYY-MM-DD HH:MM' (local exchange / New York time). For 5-minute bars:
    - a row with timestamp '2025-12-30 16:00' represents the bar covering 16:00–16:05,
      so its `close` is the price at 16:05, not 16:00.
    - the true market close at 16:00 is the `close` of the previous bar (e.g. '15:55').
- This file computes:
    price_open  = price at 09:30 on trade_date (bar open or nearest bar <= 09:30)
    price_close = price at 16:00 on trade_date (taken as the last bar *strictly before* 16:00)
    prev_close  = price at 16:00 on the previous trade date (same contract), also strictly before 16:00
- `inclusive=False` is used when fetching closes to avoid selecting a bar that starts at 16:00
  (which would give the 16:05 price instead of the 16:00 market close).
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

# Ensure project `src` root is on sys.path when executed
_script_dir = Path(__file__).resolve().parent
_project_src = _script_dir.parent
if str(_project_src) not in sys.path:
    sys.path.insert(0, str(_project_src))

from python.config import DB_PATH

ALLOWED_FIELDS = {"open", "high", "low", "close"}


def get_price_at(cur: sqlite3.Cursor, contract_id: int, date: str, time: str, inclusive: bool = True) -> Optional[
    float]:
    """
    Fetch the most-recent bar (for contract_id) on date at or before (inclusive=True) or
    strictly before (inclusive=False) the given time. Match hour:minute using strftime to avoid
    mismatches due to seconds or different timestamp formats.
    """
    comparator = "<=" if inclusive else "<"
    sql = f"""
        SELECT close
        FROM bars_5min
        WHERE contract_id = ?
          AND date(timestamp) = ?
          AND strftime('%H:%M', timestamp) {comparator} ?
        ORDER BY timestamp DESC
        LIMIT 1
        """
    cur.execute(sql, (contract_id, date, time))
    row = cur.fetchone()
    return row[0] if row else None


def get_exact_field(cur: sqlite3.Cursor, contract_id: int, date: str, time: str, field: str) -> Optional[float]:
    """Return `field` value from the bar that starts exactly at `time` on `date`, or None.
    Use strftime('%H:%M', timestamp) to ensure we match hour:minute exactly.
    """
    if field not in ALLOWED_FIELDS:
        raise ValueError("invalid field")
    sql = f"""
        SELECT {field}
        FROM bars_5min
        WHERE contract_id = ?
          AND date(timestamp) = ?
          AND strftime('%H:%M', timestamp) = ?
        LIMIT 1
    """
    cur.execute(sql, (contract_id, date, time))
    row = cur.fetchone()
    return row[0] if row else None


def get_last_before(cur: sqlite3.Cursor, contract_id: int, date: str, time: str, field: str) -> Optional[float]:
    """Return `field` from the last bar strictly before `time` on `date`, or None.
    Use strftime('%H:%M', timestamp) for consistent comparison.
    """
    if field not in ALLOWED_FIELDS:
        raise ValueError("invalid field")
    sql = f"""
        SELECT {field}
        FROM bars_5min
        WHERE contract_id = ?
          AND date(timestamp) = ?
          AND strftime('%H:%M', timestamp) < ?
        ORDER BY timestamp DESC
        LIMIT 1
    """
    cur.execute(sql, (contract_id, date, time))
    row = cur.fetchone()
    return row[0] if row else None


def fetch_trade_dates(cur: sqlite3.Cursor, symbol_code: str) -> List[str]:
    # Trade dates are taken from the daily liquid contract mapping.
    cur.execute(
        """
        SELECT DISTINCT trade_date
        FROM liquid_contract_daily
        WHERE symbol_code = ?
        ORDER BY trade_date
        """,
        (symbol_code,),
    )
    return [r[0] for r in cur.fetchall()]


def fetch_contract_for_date(cur: sqlite3.Cursor, symbol_code: str, trade_date: str) -> Optional[int]:
    # Which contract was considered the liquid contract on `trade_date`.
    cur.execute(
        """
        SELECT contract_id
        FROM liquid_contract_daily
        WHERE symbol_code = ? AND trade_date = ?
        """,
        (symbol_code, trade_date),
    )
    row = cur.fetchone()
    return row[0] if row else None


def upsert_reference_prices(cur: sqlite3.Cursor,
                            rows: Sequence[Tuple[str, str, Optional[float], Optional[float], Optional[float]]]) -> int:
    # Bulk insert/replace into daily_reference_prices table
    cur.executemany(
        """
        INSERT OR REPLACE INTO daily_reference_prices
            (symbol_code, trade_date, price_open, price_close, prev_close)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def process_symbol(conn: sqlite3.Connection, symbol_code: str, dry_run: bool = False) -> int:
    """
    For each trade date for `symbol_code`:
      - determine the liquid contract_id for that date
      - pick price_open at 09:30 (inclusive: accepts a bar that starts at 09:30)
      - pick price_close at 16:00 (exclusive: picks the previous bar's close)
      - pick prev_close as the prior trade date's 16:00 close (exclusive)
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    days = fetch_trade_dates(cur, symbol_code)
    if not days:
        logging.info("No trade days found for symbol %s", symbol_code)
        return 0

    rows: List[Tuple[str, str, Optional[float], Optional[float], Optional[float]]] = []

    for idx, day in enumerate(days):
        cid = fetch_contract_for_date(cur, symbol_code, day)
        if cid is None:
            logging.debug("No liquid contract for %s on %s; skipping", symbol_code, day)
            continue

        # open: include a bar at 09:30 if present (bar that starts at 09:30 has correct open)
        # prefer 09:30 bar open; if missing, use the prior bar's close (strictly before 09:30)
        price_open = get_exact_field(cur, cid, day, "09:30", "open")
        if price_open is None:
            price_open = get_last_before(cur, cid, day, "09:30", "close")

        # Can also do this as a simpler solution
        # price_open = get_price_at(cur, cid, day, "09:30", inclusive=False)

        # close: exclude a bar that starts at 16:00 (select the prior 5-min bar for true 16:00 close)
        price_close = get_price_at(cur, cid, day, "16:00", inclusive=False)

        prev_close: Optional[float] = None
        if idx > 0:
            prev_day = days[idx - 1]
            # previous-day close should also be taken from the same contract (cid)
            # and exclude any bar that starts at 16:00 on the previous day
            prev_close = get_price_at(cur, cid, prev_day, "16:00", inclusive=False)

        rows.append((symbol_code, day, price_open, price_close, prev_close))

    if not rows:
        logging.info("No reference price rows to insert for %s", symbol_code)
        return 0

    if dry_run:
        logging.info("Dry run: would insert %d rows for %s", len(rows), symbol_code)
        return len(rows)

    inserted = upsert_reference_prices(cur, rows)
    conn.commit()
    logging.info("Inserted/updated %d rows into daily_reference_prices for %s", inserted, symbol_code)
    return inserted


def main(argv=None):
    parser = argparse.ArgumentParser(description="Compute daily reference prices for a symbol")
    parser.add_argument("symbol", help="symbol code to process (e.g. `ES`)")
    parser.add_argument("--db", default=DB_PATH, help="path to sqlite database file")
    parser.add_argument("--dry-run", action="store_true", help="compute but do not modify the database")
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    conn = sqlite3.connect(args.db)
    try:
        if args.dry_run:
            # Run inside a transaction and roll back to avoid writes
            conn.isolation_level = None
            cur = conn.cursor()
            cur.execute("BEGIN")
            try:
                count = process_symbol(conn, args.symbol, dry_run=True)
            finally:
                cur.execute("ROLLBACK")
            logging.info("Dry run complete: %d rows would have been written.", count)
        else:
            count = process_symbol(conn, args.symbol, dry_run=False)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
