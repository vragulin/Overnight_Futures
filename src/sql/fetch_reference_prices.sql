/* Show reference prices for a symbol */
select * from daily_reference_prices
where symbol_code = 'ES'
and trade_date > '2025-11-30'
order by trade_date;

/* Clear out existing reference prices for the symbol to allow re-calculation */
/*DELETE FROM daily_reference_prices
WHERE symbol_code = 'ES';*

/* Show bars for a specific contract as a check */
select * from bars_5min
where contract_id = (
    select contract_id from contracts
    where symbol_code = 'ES' and month_code = 'J' and year = 2022
)
and timestamp >= '2022-02-25 15:45:00' and timestamp <= '2022-02-25 16:20:00'
-- and timestamp >= '2022-02-28 09:20:00' and timestamp <= '2022-02-28 09:40:00'
order by timestamp;

