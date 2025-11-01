CREATE DATABASE IF NOT EXISTS finance_dw;
USE finance_dw;

CREATE TABLE IF NOT EXISTS finance_prices (
    symbol VARCHAR(20),
    date DATE,
    open DECIMAL(18,6),
    high DECIMAL(18,6),
    low DECIMAL(18,6),
    close DECIMAL(18,6),
    adj_close DECIMAL(18,6),
    volume BIGINT,
    return_1d DECIMAL(18,8),
    moving_avg_7 DECIMAL(18,6),
    moving_avg_30 DECIMAL(18,6),
    PRIMARY KEY (symbol, date)
);