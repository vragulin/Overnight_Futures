import sys
import sqlite3
import argparse
import logging
from pathlib import Path
from math import sqrt
from typing import Tuple

import numpy as np
import pandas as pd

# Silence verbose font discovery messages from matplotlib
logging.getLogger("matplotlib.font_manager").setLevel(logging.WARNING)
import matplotlib.pyplot as plt
import seaborn as sns

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
      - returns_df: log-return series indexed by trade_date (columns: full_log, intraday_log, overnight_log)
      - cum_df: cumulative $1 series for plotting (columns: Full, Intraday, Overnight), indexed by trade_date
    Description is taken from `rollover_rules.description` (falls back to `symbol`).
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

    # intraday: ln(close / open) only when prev_close exists (per requirement)
    df['intraday_log'] = np.where(
        (df['price_open'].notna()) &
        (df['price_close'].notna()) &
        (df['price_open'] != 0) &
        (df['prev_close'].notna()),
        np.log(df['price_close'] / df['price_open']),
        np.nan
    )

    # overnight: ln(open / prev_close)
    df['overnight_log'] = np.where(
        (df['price_open'].notna()) & (df['prev_close'].notna()) & (df['prev_close'] != 0),
        np.log(df['price_open'] / df['prev_close']),
        np.nan
    )

    # full = intraday + overnight
    df['full_log'] = df['intraday_log'] + df['overnight_log']

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

    full_stats = stats_from_log_series(df['full_log'])
    intraday_stats = stats_from_log_series(df['intraday_log'])
    overnight_stats = stats_from_log_series(df['overnight_log'])

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
            'Overnight': overnight_stats
        },
        index=rows
    )

    # Format for display (3 decimals). Keep numeric_df separate for programmatic use.
    def fmt_cell(x):
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return "nan"
        return f"{x:.3f}"

    # elementwise formatting using Series.map via stack/unstack (avoids DataFrame.applymap)
    display_df = numeric_df.copy()
    display_df = display_df.stack().map(fmt_cell).unstack()

    # Put top-level label "<description> (<symbol>) stats" above the three columns (MultiIndex)
    top_label = f"{description} ({symbol}) stats"
    display_df.columns = pd.MultiIndex.from_product([[top_label], display_df.columns])
    numeric_df.columns = pd.MultiIndex.from_product([[top_label], numeric_df.columns])

    # Build returns DataFrame indexed by trade_date for plotting
    returns_df = df[['trade_date', 'full_log', 'intraday_log', 'overnight_log']].copy()
    returns_df['trade_date'] = pd.to_datetime(returns_df['trade_date'])
    returns_df = returns_df.set_index('trade_date')

    # Compute cumulative $1 series for each return type
    cum_df = pd.DataFrame(index=returns_df.index)
    for col, name in [('full_log', 'Full'), ('intraday_log', 'Intraday'), ('overnight_log', 'Overnight')]:
        s = returns_df[col]
        valid = s.dropna()
        if valid.empty:
            cum_df[name] = np.nan
            continue

        # cumulative value on days with defined returns: cum = exp(cumsum(log_returns))
        cum_valid = np.exp(valid.cumsum())
        # Reindex to full index, forward-fill so plot shows most recent accumulated value on subsequent days
        cum_series = cum_valid.reindex(returns_df.index)
        cum_series.ffill(inplace=True)
        # For days before first valid return, set to 1.0 (so series starts at 1.0)
        first_valid = valid.index[0]
        cum_series.loc[:first_valid] = 1.0
        cum_df[name] = cum_series

    return display_df, numeric_df, returns_df, cum_df


def plot_cumulative(cum_df: pd.DataFrame, symbol: str, show_diff: bool = True):
    """Plot cumulative $1 growth for Full, Intraday, Overnight.
    If show_diff is True, add a second (stacked) chart sharing the x-axis that shows
    the ratio Overnight / Intraday.
    The chart title includes the description from `rollover_rules` when available.
    """
    try:
        sns.set_theme(style="darkgrid")
        logging.debug("Using seaborn theme 'darkgrid' for plotting.")
    except Exception:
        logging.debug("seaborn theme failed; using matplotlib defaults.")

    # Try to fetch description from DB (fallback to symbol)
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT description FROM rollover_rules WHERE symbol_code = ?", (symbol,))
        row = cur.fetchone()
        description = row[0] if row and row[0] else symbol
        conn.close()
    except Exception:
        description = symbol

    # Determine which series to plot
    plotted = []
    for col in ['Full', 'Intraday', 'Overnight']:
        if col in cum_df.columns and not cum_df[col].dropna().empty:
            plotted.append(col)

    if not plotted:
        logging.info("No data to plot for %s", symbol)
        return

    # Create either one axes or two stacked axes sharing x-axis
    if show_diff:
        fig, (ax, ax_ratio) = plt.subplots(nrows=2, sharex=True, figsize=(10, 8),
                                           gridspec_kw={'height_ratios': [3, 1]})
    else:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax_ratio = None

    # Top: cumulative series
    for col in ['Full', 'Intraday', 'Overnight']:
        if col not in cum_df.columns:
            continue
        series = cum_df[col].dropna()
        if series.empty:
            continue
        ax.plot(series.index, series.values, label=col)

    # Legend for top plot showing final values (2 decimals)
    handles, labels = ax.get_legend_handles_labels()
    new_labels = []
    for lab in labels:
        if lab in cum_df.columns:
            col_series = cum_df[lab].dropna()
            final_val = col_series.iloc[-1] if not col_series.empty else np.nan
            new_labels.append(f"{lab} (final={final_val:.2f})" if not np.isnan(final_val) else f"{lab} (final=nan)")
        else:
            new_labels.append(lab)
    ax.legend(handles, new_labels, loc='best')
    ax.set_title(f"Cumulative $1 returns - {description} ({symbol})")
    ax.set_ylabel("Value of $1")

    # Bottom: ratio Overnight / Intraday
    if show_diff and ax_ratio is not None:
        if 'Overnight' in cum_df.columns and 'Intraday' in cum_df.columns:
            num = cum_df['Overnight']
            den = cum_df['Intraday']
            # avoid division by zero and invalid values
            den_safe = den.replace(0, np.nan)
            ratio = num / den_safe
            ratio = ratio.replace([np.inf, -np.inf], np.nan)

            ratio_plot = ratio.dropna()
            if not ratio_plot.empty:
                ax_ratio.plot(ratio_plot.index, ratio_plot.values, color='tab:purple', label='Overnight / Intraday')
                ax_ratio.axhline(1.0, linestyle='--', color='gray', linewidth=0.8)
                last_ratio = ratio_plot.iloc[-1]
                ax_ratio.legend([f"Overnight/Intraday (last={last_ratio:.3f})"], loc='best')
                ax_ratio.set_ylabel("Overnight / Intraday")
            else:
                logging.info("No valid Overnight/Intraday ratio data for %s", symbol)
                ax_ratio.text(0.5, 0.5, "No valid ratio data", ha='center', va='center', transform=ax_ratio.transAxes)
                ax_ratio.set_ylabel("Overnight / Intraday")
        else:
            logging.info("Missing Overnight or Intraday column; skipping ratio plot for %s", symbol)
            ax_ratio.text(0.5, 0.5, "Missing Overnight or Intraday", ha='center', va='center',
                          transform=ax_ratio.transAxes)
            ax_ratio.set_ylabel("Overnight / Intraday")

        ax_ratio.set_xlabel("Trade Date")
    else:
        ax.set_xlabel("Trade Date")

    fig.autofmt_xdate()
    plt.tight_layout()
    plt.show()


def main(argv=None, start_date: str = None, end_date: str = None, show_diff: bool = True) -> int:
    parser = argparse.ArgumentParser(
        description="Compute overnight/intraday/full return stats from daily_reference_prices"
    )
    parser.add_argument("symbol", help="symbol code to process (e.g. `ES`)")
    parser.add_argument("--start-date", dest="start_date", help="start date (YYYY-MM-DD) - optional")
    parser.add_argument("--end-date", dest="end_date", help="end date (YYYY-MM-DD) - optional")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--show-diff", dest="show_diff", action="store_true", help="show Overnight/Intraday ratio plot")
    group.add_argument("--no-show-diff", dest="show_diff", action="store_false",
                       help="do not show Overnight/Intraday ratio plot")
    # ensure absence of either flag yields None so we can defer to the function keyword arg
    parser.set_defaults(show_diff=None)
    parser.add_argument("--db", default=DB_PATH, help="path to sqlite database file (default from config)")
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    # CLI flags take precedence over keyword args passed into main()
    effective_start = args.start_date if args.start_date is not None else start_date
    effective_end = args.end_date if args.end_date is not None else end_date
    effective_show_diff = args.show_diff if args.show_diff is not None else show_diff

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

    # Plot cumulative returns, passing effective_show_diff
    try:
        plot_cumulative(cum_df, args.symbol, show_diff=effective_show_diff)
    except Exception:
        logging.exception("Plotting failed (continuing)")

    return 0


if __name__ == "__main__":
    sys.exit(main())