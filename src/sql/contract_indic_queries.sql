/* Check the data loaded into the bars_5min table */
select * from bars_5min
where timestamp > '2017-09-25 12:00:00'
order by timestamp desc
limit 100;

/* See which contract_ids have been loaded */
SELECT DISTINCT contract_id
FROM bars_5min;

SELECT COUNT(DISTINCT contract_id) AS distinct_contracts
FROM bars_5min;

/* Show names of all available contracts */
SELECT DISTINCT b.contract_id,
       COALESCE(c.symbol_code, s.symbol_code) || c.month_code || printf('%02d', c.year % 100) AS full_symbol
FROM bars_5min b
LEFT JOIN contracts c ON b.contract_id = c.contract_id
LEFT JOIN symbols   s ON c.symbol_code = s.symbol_code
ORDER BY full_symbol;


/* Show only S&P500 futures contracts */
SELECT DISTINCT b.contract_id,
       COALESCE(c.symbol_code, s.symbol_code) || c.month_code || printf('%02d', c.year % 100) AS full_symbol,
       c.first_trade_date,
       c.last_trade_date
FROM bars_5min b
LEFT JOIN contracts c ON b.contract_id = c.contract_id
LEFT JOIN symbols   s ON c.symbol_code = s.symbol_code
WHERE COALESCE(c.symbol_code, s.symbol_code) LIKE 'ES%'
ORDER BY full_symbol;

/* Show symbols table */
select * from symbols limit 10;

/* Update contracts table with first and last trade dates from bars_5min */

/* Test query to find first and last trade dates for a specific contract_id */
SELECT contracts.contract_id,
       contracts.symbol_code,
       contracts.month_code,
       contracts.year,
       (SELECT MIN(date("timestamp")) FROM bars_5min WHERE bars_5min.contract_id = contracts.contract_id) AS first_trade_date,
       (SELECT MAX(date("timestamp")) FROM bars_5min WHERE bars_5min.contract_id = contracts.contract_id) AS last_trade_date
FROM contracts
ORDER BY contracts.contract_id
LIMIT 100;


/* Update first_trade_date and last_trade_date in contracts table */
UPDATE contracts
SET first_trade_date = (
    SELECT MIN(date("timestamp")) FROM bars_5min WHERE bars_5min.contract_id = contracts.contract_id
),
last_trade_date = (
    SELECT MAX(date("timestamp")) FROM bars_5min WHERE bars_5min.contract_id = contracts.contract_id
);
