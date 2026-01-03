"""Compute the most liquid contract per trading day for a given symbol and store
results in the liquid_contract_daily table.

This revision only considers trade dates where there is at least one bar
between REQUIRED_OPEN_START and REQUIRED_OPEN_END (to exclude weekends/holidays
or days where the market only opens in the evening).
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path
from typing import List, Tuple

# Ensure project `src` root is on sys.path when this file is executed as a script.
_script_dir = Path(__file__).resolve().parent
_project_src = _script_dir.parent
if str(_project_src) not in sys.path:
    sys.path.insert(0, str(_project_src))

from python.config import (
    DB_PATH,
    MAX_DAYS_TO_LAST_DAY,
    MIN_DAILY_VOLUME,
    REQUIRED_OPEN_START,
    REQUIRED_OPEN_END,
)


def compute_liquid_contracts(conn: sqlite3.Connection, symbol_code: str, dry_run: bool = False) -> None:
    """
    For the given symbol_code, identify the most liquid contract per trade_date T
    where:
      - contract's daily volume for T >= MIN_DAILY_VOLUME
      - contract.last_trade_date is not NULL and (last_trade_date - T) between 0 and MAX_DAYS_TO_LAST_DAY
      - trade_date has at least one bar with time between REQUIRED_OPEN_START and REQUIRED_OPEN_END

    Results are upserted into liquid_contract_daily (symbol_code, trade_date, contract_id).
    """
    cur = conn.cursor()

    sql = f"""
    WITH allowed_dates AS (
        -- days where the symbol has at least one bar in the required open window
        SELECT DISTINCT date(b.timestamp) AS trade_date
        FROM bars_5min b
        JOIN contracts c ON c.contract_id = b.contract_id
        WHERE c.symbol_code = ?
          AND time(b.timestamp) BETWEEN ? AND ?
    ),
    daily AS (
        SELECT
            date(b.timestamp) AS trade_date,
            b.contract_id,
            SUM(COALESCE(b.volume, 0)) AS vol_sum
        FROM bars_5min b
        JOIN contracts c ON c.contract_id = b.contract_id
        WHERE c.symbol_code = ?
          AND c.last_trade_date IS NOT NULL
          -- ensure contract still has <= MAX_DAYS_TO_LAST_DAY days left at date(b.timestamp)
          AND julianday(c.last_trade_date) - julianday(date(b.timestamp)) BETWEEN 0 AND ?
          -- only consider trade_dates that are in the allowed open-time window
          AND date(b.timestamp) IN (SELECT trade_date FROM allowed_dates)
        GROUP BY date(b.timestamp), b.contract_id
        HAVING vol_sum >= ?
    ),
    ranked AS (
        SELECT
            trade_date,
            contract_id,
            ROW_NUMBER() OVER (
                PARTITION BY trade_date
                ORDER BY vol_sum DESC, contract_id
            ) AS rn
        FROM daily
    )
    SELECT trade_date, contract_id
    FROM ranked
    WHERE rn = 1
    ORDER BY trade_date;
    """

    params = (
        symbol_code,                # allowed_dates: symbol
        REQUIRED_OPEN_START,        # allowed_dates: start time
        REQUIRED_OPEN_END,          # allowed_dates: end time
        symbol_code,                # daily: symbol
        MAX_DAYS_TO_LAST_DAY,       # daily: max days window
        MIN_DAILY_VOLUME,           # daily: minimum daily volume (HAVING)
    )

    cur.execute(sql, params)
    winners: List[Tuple[str, int]] = cur.fetchall()  # (trade_date, contract_id)

    if not winners:
        logging.info(
            "No winners for %s after applying filters (MIN_DAILY_VOLUME=%d, MAX_DAYS_TO_LAST_DAY=%d, required open %s-%s).",
            symbol_code,
            MIN_DAILY_VOLUME,
            MAX_DAYS_TO_LAST_DAY,
            REQUIRED_OPEN_START,
            REQUIRED_OPEN_END,
        )
        return

    rows = [(symbol_code, trade_date, contract_id) for trade_date, contract_id in winners]

    if dry_run:
        logging.info("Dry run: would upsert %d rows into liquid_contract_daily for %s.", len(rows), symbol_code)
        return

    cur.executemany(
        """
        INSERT OR REPLACE INTO liquid_contract_daily (symbol_code, trade_date, contract_id)
        VALUES (?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    logging.info("Inserted/updated %d rows into liquid_contract_daily for %s.", len(rows), symbol_code)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Compute daily liquid contracts for a symbol")
    parser.add_argument("symbol", help="symbol code to process")
    parser.add_argument("--db", default=DB_PATH, help="path to sqlite database file")
    parser.add_argument("--dry-run", action="store_true", help="compute but do not modify the database")
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    conn = sqlite3.connect(args.db)
    try:
        compute_liquid_contracts(conn, args.symbol, dry_run=args.dry_run)
    finally:
        conn.close()


if __name__ == "__main__":
    main()