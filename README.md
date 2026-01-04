# Scripts to analyze intraday and overnight futures returns

## Main script
calc_overnight_stats.py

Purpose
-------
Compare intraday and overnight returns.  Create plots and tables.  Runs from the Windows command line.

Usage
-----
Command-line:
    python calc_overnight_stats.py [OPTIONS]

Options
-------
  -h, --help
        Show this help message and exit.

  --db PATH
        Path to the SQLite database file (default: data/database.db).

  --start DATE
        Start date (inclusive) for calculations, format YYYY-MM-DD.

  --end DATE
        End date (inclusive) for calculations, format YYYY-MM-DD.

  --symbols SYMBOLS
        Comma-separated list of symbol codes to process (e.g., GC,FV). Default: all.

  --use-liquid
        Use `liquid_contract_daily` view to resolve active contracts per trade_date.

  --rollover PATH
        Path to a CSV or JSON file with custom rollover rules (overrides defaults).

  --agg {none,daily,weekly,monthly}
        Aggregation level for output statistics. Default: none (per-day).

  --metrics METRICS
        Comma-separated list of metrics to compute (e.g., mean,median,std,p99).
        Default: mean,median,std,count.

  --output PATH
        Path to write results (CSV). If omitted, prints to stdout.

  --dry-run
        Validate inputs and show summary of work without performing calculations.

  --threads N
        Number of worker threads for parallel processing (default: 1).

  --verbose
        Increase logging verbosity.

Input assumptions
-----------------
- Table `bars_5min` contains timestamped 5-minute OHLCV bars and `contract_id`.
- Table/view `contracts` maps `contract_id` to `symbol_code`, `month_code`, `year`.
- Optional view `liquid_contract_daily` provides the active contract per `trade_date`.
- Timestamps are stored in UTC or consistently in a single timezone.

Output
------
CSV columns (example):
    trade_date, symbol_code, contract_id, prev_close, open_price, overnight_return

When aggregation is requested, additional columns for the chosen metrics will be included.

Examples
--------
1. Basic run on default DB for all symbols:
    python `calc_overnight_stats.py` --db data/database.db --start 2020-01-01 --end 2020-12-31 --output results.csv

2. Use liquid contract mapping and aggregate monthly:
    python `calc_overnight_stats.py` --db data/database.db --use-liquid --agg monthly --output monthly_stats.csv

3. Dry run and verbose:
    python `calc_overnight_stats.py` --db data/database.db --dry-run --verbose

Requirements
------------
- Python 3.9+
- pandas, numpy
- sqlite3 (or relevant DB connector) and any project-specific dependencies listed in `requirements.txt`

Exit codes
----------
0   Success
1   Invalid CLI arguments
2   Database connection or table not found
3   Data validation error (e.g., missing required columns)
4   Runtime error during computation

Troubleshooting
---------------
- "no such table: bars_5min": confirm `--db` points to the correct DB and the table exists.
- Missing contract metadata: validate `contracts` table contains required `contract_id` mappings.
- Timezone issues: ensure timestamps in `bars_5min` are normalized (prefer UTC).
- Small sample sizes: when aggregating, some metrics may be NaN if the group count is low.
