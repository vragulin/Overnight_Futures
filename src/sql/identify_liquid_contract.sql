/* Create daily liquidity view */
CREATE VIEW IF NOT EXISTS daily_volume AS
SELECT
    c.symbol_code,
    date(b.timestamp) AS trade_date,
    b.contract_id,
    c.month_code,
    c.year,
    SUM(COALESCE(b.volume, 0)) AS volume_sum
FROM bars_5min b
JOIN contracts c ON c.contract_id = b.contract_id
--where c.symbol_code like 'ES%'  /* You can change this filter to other symbols as needed */
GROUP BY c.symbol_code, date(b.timestamp), b.contract_id
ORDER BY c.symbol_code, trade_date;

/* Identify the most liquid contract per symbol_code and trade_date */
CREATE VIEW IF NOT EXISTS liquid_contract_daily AS
SELECT
    symbol_code,
    trade_date,
    contract_id
FROM (
    SELECT
        dv.symbol_code,
        dv.trade_date,
        dv.contract_id,
        ROW_NUMBER() OVER (
            PARTITION BY dv.symbol_code, dv.trade_date
            ORDER BY dv.volume_sum DESC, dv.contract_id
        ) AS rn
    FROM daily_volume dv
) t
WHERE rn = 1;

/* Check the liquid contract mapping for a specific symbol */
SELECT
    l.symbol_code,
    l.trade_date,
    l.contract_id,
    c.month_code,
    c.year
FROM liquid_contract_daily AS l
LEFT JOIN contracts AS c
    ON l.contract_id = c.contract_id
WHERE l.symbol_code = 'ES'
ORDER BY l.trade_date;

/* Count days for which we have the liquid contract */
SELECT count(l.trade_date) as num_days
FROM liquid_contract_daily AS l
LEFT JOIN contracts AS c
    ON l.contract_id = c.contract_id
WHERE l.symbol_code = 'FV'
ORDER BY l.trade_date;

/* Count contracts in the database */
SELECT count(distinct(symbol_code))
from liquid_contract_daily;

/* Show allowed dates for a specific contract */
SELECT DISTINCT date(b.timestamp) AS trade_date
FROM bars_5min b
JOIN contracts c ON c.contract_id = b.contract_id
WHERE c.symbol_code = 'BTC'
  AND time(b.timestamp) BETWEEN '10:00' AND '10:30'
ORDER BY trade_date desc
LIMIT 100;

/* Identify liquid contracts based on volume threshold within a specific open-time window */
SELECT
    date(b.timestamp) AS trade_date,
    b.contract_id,
    c.symbol_code,
    c.month_code,
    c.year,
    c.expiry_date,
    c.last_trade_date,
    SUM(COALESCE(b.volume, 0)) AS vol_sum
FROM bars_5min b
JOIN contracts c ON c.contract_id = b.contract_id
WHERE c.symbol_code = 'BTC'
  AND c.last_trade_date IS NOT NULL
  -- ensure contract still has <= MAX_DAYS_TO_LAST_DAY days left at date(b.timestamp)
  AND julianday(c.last_trade_date) - julianday(date(b.timestamp)) BETWEEN 0 AND 100
  -- only consider trade_dates that are in the allowed open-time window
  -- AND date(b.timestamp) IN (SELECT trade_date FROM allowed_dates)
GROUP BY date(b.timestamp), b.contract_id
HAVING vol_sum >= 2500;