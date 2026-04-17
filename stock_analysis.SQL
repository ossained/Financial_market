CREATE SCHEMA EDW;

CREATE TABLE EDW.staging_all_data (
    symbol VARCHAR(50),
    company_name VARCHAR(255),
    sector VARCHAR(255),
    exchange_name VARCHAR(255),
    event_datetime DATETIME2,   
    [open] DECIMAL(18,4),
    high DECIMAL(18,4),
    low DECIMAL(18,4),
    [close] DECIMAL(18,4),
    volume BIGINT,
    daily_return DECIMAL(18,6),
    price_range DECIMAL(18,6),
    prev_close DECIMAL(18,4),
    price_direction INT         
);


CREATE TABLE EDW.dim_date (
    date_sk INT IDENTITY(1,1) PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week VARCHAR(20) NOT NULL
);

CREATE TABLE EDW.dim_company (
    company_sk INT IDENTITY(1,1) PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(255),
    exchange_name VARCHAR(255),

    effective_start_date DATE NOT NULL,
    effective_end_date DATE NULL,
    is_current BIT NOT NULL,

    CONSTRAINT UQ_dim_company UNIQUE (symbol, effective_start_date)
);

CREATE TABLE EDW.dim_exchange (
    exchange_sk INT IDENTITY(1,1) PRIMARY KEY,
    exchange_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE EDW.fact_prices (
    fact_sk INT IDENTITY(1,1) PRIMARY KEY,
    date_sk INT NOT NULL,
    company_sk INT NOT NULL,
    exchange_sk INT NOT NULL,

    open_price DECIMAL(18,4),
    high_price DECIMAL(18,4),
    low_price DECIMAL(18,4),
    close_price DECIMAL(18,4),
    volume BIGINT,

    daily_return DECIMAL(18,6),
    price_range DECIMAL(18,6),
    prev_close DECIMAL(18,4),
    price_direction VARCHAR(10),

    loaded_date DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),

    CONSTRAINT FK_fact_date FOREIGN KEY (date_sk) REFERENCES EDW.dim_date(date_sk),
    CONSTRAINT FK_fact_company FOREIGN KEY (company_sk) REFERENCES EDW.dim_company(company_sk),
    CONSTRAINT FK_fact_exchange FOREIGN KEY (exchange_sk) REFERENCES EDW.dim_exchange(exchange_sk)
);




--insert records
INSERT INTO EDW.dim_date (
    full_date,
    year,
    quarter,
    month,
    day,
    day_of_week
)
SELECT DISTINCT
    CAST(event_datetime AS DATE) AS full_date,
    YEAR(event_datetime),
    DATEPART(QUARTER, event_datetime),
    MONTH(event_datetime),
    DAY(event_datetime),
    DATENAME(WEEKDAY, event_datetime)
FROM EDW.staging_all_data;


INSERT INTO EDW.dim_company (
    symbol, company_name, sector, exchange_name,
    effective_start_date, effective_end_date, is_current
)
SELECT DISTINCT
    symbol,
    company_name,
    sector,
    exchange_name,
    CAST(GETDATE() AS DATE),
    NULL,
    1
FROM EDW.staging_all_data;


UPDATE c
SET c.effective_end_date = CAST(GETDATE() AS DATE),
    c.is_current = 0
FROM EDW.dim_company c
JOIN EDW.staging_all_data s
    ON c.symbol = s.symbol
WHERE c.is_current = 1
  AND (
        c.company_name <> s.company_name OR
        c.sector <> s.sector OR
        c.exchange_name <> s.exchange_name
      );

INSERT INTO EDW.dim_exchange (exchange_name)
SELECT DISTINCT exchange_name
FROM EDW.staging_all_data;


INSERT INTO EDW.fact_prices (
    date_sk, company_sk, exchange_sk,
    open_price, high_price, low_price, close_price,
    volume, loaded_date
)
SELECT
    d.date_sk,
    c.company_sk,
    e.exchange_sk,
    s.[open],
    s.[high],
    s.[low],
    s.[close],
    s.volume,
    GETDATE()
FROM EDW.staging_all_data s
JOIN EDW.dim_date d
    ON d.full_date = CAST(s.event_datetime AS DATE)
JOIN EDW.dim_company c
    ON c.symbol = s.symbol
    AND c.is_current = 1
JOIN EDW.dim_exchange e
    ON e.exchange_name = c.exchange_name;


    UPDATE f
SET
    f.price_range     = f.high_price - f.low_price,
    f.prev_close      = prev.close_price,
    f.daily_return    = ROUND((f.close_price - prev.close_price) / prev.close_price * 100, 4),
    f.price_direction = CASE 
                            WHEN f.close_price > prev.close_price THEN 'UP'
                            WHEN f.close_price < prev.close_price THEN 'DOWN'
                            ELSE 'FLAT'
                        END
FROM EDW.fact_prices f
JOIN EDW.dim_date d ON d.date_sk = f.date_sk
JOIN EDW.fact_prices prev 
    ON prev.company_sk = f.company_sk
    AND prev.date_sk = (
        SELECT MAX(date_sk) 
        FROM EDW.fact_prices 
        WHERE company_sk = f.company_sk 
        AND date_sk < f.date_sk
    )
WHERE f.daily_return IS NULL;