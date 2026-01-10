import sys
import sqlite3
import argparse
import logging
from pathlib import Path
from math import sqrt
from typing import Tuple

import numpy as np
import pandas as pd

# Ensure project `src` root is on sys.path when executed as a script.
_script_dir = Path(__file__).resolve().parent
_project_src = _script_dir.parent
if str(_project_src) not in sys.path:
    sys.path.insert(0, str(_project_src))

from python.config import DB_PATH

TRADING_DAYS = 252


def compute_reference_stats(db_path: str, symbol: str, start_date: str, end_date: str) -> Tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - display_df: pretty DataFrame (strings) with top-level header "<description> (<symbol>) stats"
      - numeric_df: numeric DataFrame (float) with identical layout for downstream use
      - returns_df: log-return series indexed by trade_date (columns: full_log, intraday_log,
                    overnight_log (all), overnight_business_log, overnight_weekend_log)
      - cum_df: cumulative $1 series for plotting (columns: Full, Intraday, Overnight (all),
                Overnight (business), Overnight (weekend), indexed by trade_date)
    Weekend overnight is defined as previous trading day being Friday and current trading day Monday.
    Business overnight is defined as previous trading day being Mon-Thu and current trading day
    being the immediate next weekday (Mon->Tue, Tue->Wed, Wed->Thu, Thu->Fri).
    """
    conn = sqlite3.connect(db_path)
    sql = """
        SELECT trade_date, price_open, price_close, prev_close
        FROM daily_reference_prices
        WHERE symbol_code = ?
          AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date
    """
    df = pd.read_sql_query(sql, conn, params=(symbol, start_date, end_date))

    # Fetch description from rollover_rules (fallback to symbol if missing)
    try:
        cur = conn.cursor()
        cur.execute("SELECT description FROM rollover_rules WHERE symbol_code = ?", (symbol,))
        row = cur.fetchone()
        description = row[0] if row and row[0] else symbol
    except Exception:
        description = symbol

    conn.close()

    # Ensure trade_date is datetime and sorted
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date').reset_index(drop=True)

    # Compute previous trading day's date (from the ordered rows)
    df['prev_trade_date'] = df['trade_date'].shift(1)

    # intraday: ln(close / open) only when prev_close exists (per requirement)
    df['intraday_log'] = np.where(
        (df['price_open'].notna()) &
        (df['price_close'].notna()) &
        (df['price_open'] != 0) &
        (df['prev_close'].notna()),
        np.log(df['price_close'] / df['price_open']),
        np.nan
    )

    # overnight: ln(open / prev_close) for all rows where prev_close exists
    df['overnight_log'] = np.where(
        (df['price_open'].notna()) & (df['prev_close'].notna()) & (df['prev_close'] != 0),
        np.log(df['price_open'] / df['prev_close']),
        np.nan
    )

    # full = intraday + overnight
    df['full_log'] = df['intraday_log'] + df['overnight_log']

    # Determine day-of-week for current and previous trade dates
    # Monday=0, Tuesday=1, ..., Friday=4
    prev_day = df['prev_trade_date'].dt.dayofweek
    curr_day = df['trade_date'].dt.dayofweek

    # Weekend overnight: prev was Friday (4) and current is Monday (0)
    weekend_mask = (prev_day == 4) & (curr_day == 0)

    # Business overnight: prev is Mon-Thu (0..3) and current is prev + 1 (i.e. immediate next weekday)
    business_mask = prev_day.isin([0, 1, 2, 3]) & (curr_day == prev_day + 1)

    # Create separate overnight series
    df['overnight_weekend_log'] = np.where(weekend_mask, df['overnight_log'], np.nan)
    df['overnight_business_log'] = np.where(business_mask, df['overnight_log'], np.nan)

    # Helper to compute stats from a log-return series
    def stats_from_log_series(log_series: pd.Series) -> Tuple[float, float, float, float, float]:
        """Return (final_value, daily_mean_pct, annual_mean_pct, annual_std_pct, ann_sharpe_decimal)."""
        s = log_series.dropna()
        n = len(s)
        if n == 0:
            return (np.nan, np.nan, np.nan, np.nan, np.nan)

        simple = np.exp(s.values) - 1.0
        final_value = float(np.prod(1.0 + simple))
        daily_mean_pct = float(np.nanmean(simple)) * 100.0

        if final_value <= 0:
            annual_mean_pct = np.nan
            annual_mean_decimal = np.nan
        else:
            annual_mean_decimal = final_value ** (TRADING_DAYS / n) - 1.0
            annual_mean_pct = float(annual_mean_decimal * 100.0)

        ddof = 1 if n > 1 else 0
        daily_std = float(np.std(simple, ddof=ddof))
        annual_std_decimal = daily_std * sqrt(TRADING_DAYS)
        annual_std_pct = annual_std_decimal * 100.0

        # Sharpe (risk-free = 0) -> decimal (not percent)
        if annual_std_decimal == 0 or np.isnan(annual_mean_decimal):
            ann_sharpe = np.nan
        else:
            ann_sharpe = annual_mean_decimal / annual_std_decimal

        return (final_value, daily_mean_pct, annual_mean_pct, annual_std_pct, ann_sharpe)

    # Compute stats for each series
    full_stats = stats_from_log_series(df['full_log'])
    intraday_stats = stats_from_log_series(df['intraday_log'])
    overnight_all_stats = stats_from_log_series(df['overnight_log'])
    overnight_business_stats = stats_from_log_series(df['overnight_business_log'])
    overnight_weekend_stats = stats_from_log_series(df['overnight_weekend_log'])

    rows = [
        'Final value of $1',
        'Daily Mean Ret (%)',
        'Annual Mean Ret (%)',
        'Annual StdDev(%)',
        'Ann Sharpe Ratio'  # decimal, not percent
    ]

    numeric_df = pd.DataFrame(
        data={
            'Full': full_stats,
            'Intraday': intraday_stats,
            'Overnight (all)': overnight_all_stats,
            'Overnight (business)': overnight_business_stats,
            'Overnight (weekend)': overnight_weekend_stats
        },
        index=rows
    )

    # Format for display (3 decimals). Keep numeric_df separate for programmatic use.
    def fmt_cell(x):
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return "nan"
        return f"{x:.3f}"

    display_df = numeric_df.copy()
    display_df = display_df.stack().map(fmt_cell).unstack()

    # Put top-level label "<description> (<symbol>) stats" above the columns (MultiIndex)
    top_label = f"{description} ({symbol}) stats"
    display_df.columns = pd.MultiIndex.from_product([[top_label], display_df.columns])
    numeric_df.columns = pd.MultiIndex.from_product([[top_label], numeric_df.columns])

    # Build returns DataFrame indexed by trade_date for downstream use
    returns_df = df[['trade_date', 'full_log', 'intraday_log', 'overnight_log',
                     'overnight_business_log', 'overnight_weekend_log']].copy()
    returns_df = returns_df.set_index('trade_date')

    # Compute cumulative $1 series for each return type
    cum_df = pd.DataFrame(index=returns_df.index)
    col_map = [
        ('full_log', 'Full'),
        ('intraday_log', 'Intraday'),
        ('overnight_log', 'Overnight (all)'),
        ('overnight_business_log', 'Overnight (business)'),
        ('overnight_weekend_log', 'Overnight (weekend)')
    ]
    for col, name in col_map:
        s = returns_df[col]
        valid = s.dropna()
        if valid.empty:
            cum_df[name] = np.nan
            continue

        cum_valid = np.exp(valid.cumsum())
        cum_series = cum_valid.reindex(returns_df.index)
        cum_series.ffill(inplace=True)
        first_valid = valid.index[0]
        cum_series.loc[:first_valid] = 1.0
        cum_df[name] = cum_series

    return display_df, numeric_df, returns_df, cum_df


def main(argv=None, start_date: str = None, end_date: str = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compute overnight/intraday/full return stats from daily_reference_prices (no plotting)"
    )
    parser.add_argument("symbol", help="symbol code to process (e.g. `ES`)")
    parser.add_argument("--start-date", dest="start_date", help="start date (YYYY-MM-DD) - optional")
    parser.add_argument("--end-date", dest="end_date", help="end date (YYYY-MM-DD) - optional")
    parser.add_argument("--db", default=DB_PATH, help="path to sqlite database file (default from config)")
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    effective_start = args.start_date if args.start_date is not None else start_date
    effective_end = args.end_date if args.end_date is not None else end_date

    # If either date is still missing, query the DB for min/max trade_date for the symbol
    if effective_start is None or effective_end is None:
        try:
            conn = sqlite3.connect(args.db)
            cur = conn.cursor()
            cur.execute(
                "SELECT MIN(trade_date), MAX(trade_date) FROM daily_reference_prices WHERE symbol_code = ?",
                (args.symbol,),
            )
            row = cur.fetchone()
            conn.close()
        except Exception:
            logging.exception("Failed to query date range from database")
            return 2

        if not row or row[0] is None or row[1] is None:
            logging.error("No `daily_reference_prices` rows found for symbol %s", args.symbol)
            return 2

        min_date, max_date = row[0], row[1]
        effective_start = effective_start if effective_start is not None else min_date
        effective_end = effective_end if effective_end is not None else max_date

    try:
        display_df, numeric_df, returns_df, cum_df = compute_reference_stats(args.db, args.symbol, effective_start,
                                                                             effective_end)
    except Exception:
        logging.exception("Failed to compute stats")
        return 2

    # Print the pretty display DataFrame (top-left header includes description now)
    print(display_df.to_string())

    # No plotting in this version
    return 0


if __name__ == "__main__":
    sys.exit(main())