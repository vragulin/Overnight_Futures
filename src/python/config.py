"""  Parameter configuration for the project
V.Ragulin 2026-01-01
"""

FUTURES_DATA_FOLDER = r"C:\ProgramData\Kibot Agent\Data\5min"
DB_PATH = r"C:\Users\vragu\OneDrive\Desktop\Proj\Overnight_2026\data\futures.sqlite3"
RESULTS_FOLDER = r"C:\Users\vragu\OneDrive\Desktop\Proj\Overnight_2026\results"

# Futures filters
MAX_DAYS_TO_LAST_DAY = 100  # how many calendar days back from the last date to consider for analysis
MIN_DAILY_VOLUME = 1500  # minimum fut volume to be considered
REQUIRED_OPEN_START = "10:00"  # required market open time for the contract on the day
REQUIRED_OPEN_END = "10:30"  # required market open time for the contract on the day
