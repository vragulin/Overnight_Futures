"""
Generate cumulative charts and xlsx stats for all symbols in daily_reference_prices
that have more than a threshold number of rows.
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import matplotlib.pyplot as plt

# Ensure project `src` root is on sys.path when executed
_script_dir = Path(__file__).resolve().parent
_project_src = _script_dir.parent
if str(_project_src) not in sys.path:
    sys.path.insert(0, str(_project_src))

from python.config import DB_PATH, RESULTS_FOLDER
from python.calc_overnight_stats import compute_reference_stats, plot_cumulative


def fetch_candidates(cur: sqlite3.Cursor, min_rows: int) -> List[Tuple[str, str, str, int]]:
    """
    Return list of tuples: (symbol_code, min_date, max_date, row_count)
    for symbols with COUNT(*) > min_rows in daily_reference_prices.
    """
    cur.execute(
        """
        SELECT symbol_code, MIN(trade_date) AS min_date, MAX(trade_date) AS max_date, COUNT(*) AS cnt
        FROM daily_reference_prices
        GROUP BY symbol_code
        HAVING cnt > ?
        ORDER BY symbol_code
        """,
        (min_rows,)
    )
    return cur.fetchall()


def safe_filename(name: str) -> str:
    # keep alphanumerics, dash, underscore; replace others with underscore
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate charts and xlsx stats for all liquid symbols")
    parser.add_argument("--db", default=DB_PATH, help="path to sqlite database file")
    parser.add_argument("--min-rows", type=int, default=1000, help="minimum number of rows in daily_reference_prices to include symbol")
    parser.add_argument("--results-dir", default=RESULTS_FOLDER,
                        help="directory to write output files (default: value from python.config.RESULTS_FOLDER)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--show-diff", dest="show_diff", action="store_true", help="show Overnight/Intraday ratio plot")
    group.add_argument("--no-show-diff", dest="show_diff", action="store_false", help="do not show Overnight/Intraday ratio plot")
    parser.set_defaults(show_diff=True)
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    logging.info("Writing results into `%s`", results_dir)

    conn = sqlite3.connect(args.db)
    try:
        cur = conn.cursor()
        candidates = fetch_candidates(cur, args.min_rows)
        if not candidates:
            logging.info("No symbols with > %d rows found in daily_reference_prices", args.min_rows)
            return 0

        logging.info("Found %d candidate symbols", len(candidates))

        for symbol, min_date, max_date, cnt in candidates:
            logging.info("Processing %s (%d rows) date range %s..%s", symbol, cnt, min_date, max_date)
            try:
                display_df, numeric_df, returns_df, cum_df = compute_reference_stats(args.db, symbol, min_date, max_date)
            except Exception:
                logging.exception("Failed to compute stats for %s (skipping)", symbol)
                continue

            safe_sym = safe_filename(symbol)
            xlsx_path = results_dir / f"{safe_sym}_stats.xlsx"
            try:
                with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
                    display_df.to_excel(writer, sheet_name=safe_sym[:31])
                logging.info("Wrote stats xlsx for %s -> %s", symbol, xlsx_path)
            except Exception:
                logging.exception("Failed to write xlsx for %s", symbol)

            # Plot and save chart WITHOUT popping up GUI windows.
            try:
                # Preserve current interactive state and plt.show, then disable interactive display.
                interactive_before = plt.isinteractive()
                prev_show = plt.show
                plt.ioff()
                plt.show = lambda *a, **k: None  # no-op to avoid popups

                try:
                    plot_cumulative(cum_df, symbol, show_diff=args.show_diff)
                    fig = plt.gcf()
                    png_path = results_dir / f"{safe_sym}_cumulative.png"
                    if fig is None or not fig.axes:
                        logging.warning("No figure created for %s; skipping save", symbol)
                    else:
                        fig.savefig(png_path, dpi=150, bbox_inches="tight")
                        logging.info("Saved chart for %s -> %s", symbol, png_path)
                finally:
                    # Restore plt.show and interactive state; close figures to free memory.
                    plt.show = prev_show
                    if interactive_before:
                        plt.ion()
                    else:
                        plt.ioff()
                    plt.close("all")
            except Exception:
                logging.exception("Failed to plot/save chart for %s", symbol)
                try:
                    plt.close("all")
                except Exception:
                    pass

    finally:
        conn.close()

    logging.info("All done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())