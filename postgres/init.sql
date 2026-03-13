CREATE TABLE IF NOT EXISTS crypto_table (
    timestamp   TIMESTAMP,
    id          TEXT,
    symbol      TEXT,
    price       DOUBLE PRECISION,
    change_1min DOUBLE PRECISION,
    change_5min DOUBLE PRECISION,
    sma         DOUBLE PRECISION,
    ema         DOUBLE PRECISION,
    volatility  DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS top_5_gainers (
    rank   INTEGER NOT NULL,
    id     TEXT,
    symbol TEXT
);

CREATE TABLE IF NOT EXISTS top_5_losers (
    rank   INTEGER NOT NULL,
    id     TEXT,
    symbol TEXT
);