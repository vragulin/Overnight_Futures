"""
Batch compute overnight stats for all symbols and save results (PNG + XLSX).
Saves symbol description (from rollover_rules) in filenames and dataframe headers.
"""
from __future__ import annotations

import argparse
import contextlib
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

RESULTS_FOLDER = Path(RESULTS_FOLDER)


def fetch_candidates(cur: sqlite3.Cursor, min_rows: int) -> List[Tuple[str, int]]:
    """
    Return list of (symbol_code, num_rows) from daily_reference_prices
    having more than min_rows rows.
    """
    cur.execute(
        """
        SELECT symbol_code, COUNT(*) AS cnt
        FROM daily_reference_prices
        GROUP BY symbol_code
        HAVING cnt >= ?
        ORDER BY symbol_code
        """,
        (min_rows,),
    )
    return [(r[0], int(r[1])) for r in cur.fetchall()]


def safe_filename(name: str) -> str:
    """
    Make a filesystem-safe filename fragment by replacing problematic chars.
    """
    # Keep alnum, dot, dash, underscore, space -> replace others with underscore
    import re
    name = name.strip()
    name = re.sub(r"[\\/:\*\?\"<>\|]", "_", name)
    name = re.sub(r"\s+", "_", name)
    return name


@contextlib.contextmanager
def suppress_interactive_plots():
    """
    Context manager to prevent interactive plot popups.
    Temporarily turn interactive mode off and monkeypatch plt.show to no-op.
    Restores prior state on exit.
    """
    prev_interactive = plt.isinteractive()
    prev_show = plt.show
    try:
        plt.ioff()
        plt.show = lambda *a, **kw: None
        yield
    finally:
        plt.show = prev_show
        if prev_interactive:
            plt.ion()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Batch compute overnight stats and save figures/xlsx.")
    parser.add_argument("--db", default=DB_PATH, help="path to sqlite database file")
    parser.add_argument("--min-rows", type=int, default=1000, help="minimum number of rows in daily_reference_prices to include symbol")
    parser.add_argument("--results-dir", default=str(RESULTS_FOLDER), help="directory to write results into")
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
        logging.info("Found %d candidate symbols (min_rows=%d)", len(candidates), args.min_rows)

        for symbol, cnt in candidates:
            logging.info("Processing %s (%d rows)...", symbol, cnt)
            # Determine date range for this symbol
            cur.execute(
                "SELECT MIN(trade_date), MAX(trade_date) FROM daily_reference_prices WHERE symbol_code = ?",
                (symbol,),
            )
            row = cur.fetchone()
            if not row or row[0] is None or row[1] is None:
                logging.warning("No dates for %s; skipping", symbol)
                continue
            start_date, end_date = row[0], row[1]

            try:
                display_df, numeric_df, returns_df, cum_df = compute_reference_stats(args.db, symbol, start_date, end_date)
            except Exception:
                logging.exception("Failed to compute stats for %s; skipping", symbol)
                continue

            # Extract top label (format: "<description> (<symbol>) stats")
            try:
                top_label = display_df.columns.levels[0][0]
            except Exception:
                top_label = f"{symbol}"
            # Try to extract description portion (split at last " (")
            description = top_label.rsplit(" (", 1)[0] if " (" in top_label else symbol

            base_name = safe_filename(f"{symbol}_{description}")
            png_path = results_dir / f"{base_name}.png"
            xlsx_path = results_dir / f"{base_name}.xlsx"

            # Save numeric_df to excel first
            try:
                numeric_df.to_excel(xlsx_path, sheet_name="stats")
                logging.info("Wrote stats XLSX: %s", xlsx_path)
            except Exception:
                logging.exception("Failed to write XLSX for %s", symbol)

            # Plot and save figure without popping up windows
            if cum_df is None or cum_df.empty:
                logging.info("No cumulative data for %s; skipping plot", symbol)
                continue

            try:
                with suppress_interactive_plots():
                    # call plotting routine (will create a figure but not show it)
                    plot_cumulative(cum_df, symbol, show_diff=True)
                    # capture the current figure and save it
                    fig = plt.gcf()
                    # Ensure layout and save
                    fig.tight_layout()
                    fig.savefig(str(png_path), dpi=150)
                    plt.close(fig)
                logging.info("Saved plot PNG: %s", png_path)
            except Exception:
                logging.exception("Failed to save plot for %s", symbol)
    finally:
        conn.close()

    logging.info("All done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())