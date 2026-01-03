/* Scripts to build tables */
CREATE TABLE symbols (
    symbol_code      TEXT PRIMARY KEY,   -- e.g. 'AD', 'ES'
    description      TEXT NOT NULL
);

/*  Contract Details */
CREATE TABLE contracts (
    contract_id      INTEGER PRIMARY KEY,       -- surrogate key
    symbol_code      TEXT NOT NULL REFERENCES symbols(symbol_code),
    month_code       TEXT NOT NULL,            -- 'F','G','H',... single letter
    year             INTEGER NOT NULL,         -- 2009..2025
    kibot_filename   TEXT UNIQUE NOT NULL,     -- e.g. 'ADF18.txt'
    expiry_date      DATE,                     -- optional: true exchange expiry
    first_trade_date DATE,                     -- populated from data
    last_trade_date  DATE                      -- populated from data
);

/* Create bars table */
CREATE TABLE bars_5min (
    contract_id  INTEGER NOT NULL REFERENCES contracts(contract_id),
    timestamp    TEXT NOT NULL,      -- 'YYYY-MM-DD HH:MM' (ISO, New York local time)
    open         REAL NOT NULL,
    high         REAL NOT NULL,
    low          REAL NOT NULL,
    close        REAL NOT NULL,
    volume       INTEGER,            -- can be NULL if blank
    PRIMARY KEY (contract_id, timestamp)
);

/* Rollover rules */
CREATE TABLE rollover_rules (
    symbol_code   TEXT PRIMARY KEY REFERENCES symbols(symbol_code),
    description   TEXT NOT NULL,              -- e.g. 'JAPANESE YEN'
    rollover_days INTEGER NOT NULL,          -- e.g. 2, 5, 8, 0
    rollover_type TEXT NOT NULL              -- 'before contract expiration', 'from end of prior month', 'on contract expiration'
);

/*Daily liquid contract */
CREATE TABLE liquid_contract_daily (
    symbol_code TEXT NOT NULL,
    trade_date  TEXT NOT NULL,
    contract_id INTEGER NOT NULL,
    PRIMARY KEY (symbol_code, trade_date)
);


/* Table to hold daily reference prices for each symbol */
CREATE TABLE daily_reference_prices (
    symbol_code TEXT,
    trade_date  TEXT,
    price_open  REAL,   -- P(T, 09:30, T)
    price_close REAL,   -- P(T, 16:00, T)
    prev_close  REAL,   -- P(T-1*, 16:00, T)
    PRIMARY KEY (symbol_code, trade_date)
);