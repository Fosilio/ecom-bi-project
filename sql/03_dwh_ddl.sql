-- 03_dwh_ddl.sql

-- DIMENSIONS
CREATE TABLE IF NOT EXISTS ecom_dwh.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id TEXT UNIQUE, -- Natural Key
    is_unknown BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS ecom_dwh.dim_product (
    product_key SERIAL PRIMARY KEY,
    stock_code TEXT UNIQUE, -- Natural Key
    description TEXT
);

CREATE TABLE IF NOT EXISTS ecom_dwh.dim_country (
    country_key SERIAL PRIMARY KEY,
    country_name TEXT UNIQUE -- Natural Key
);

CREATE TABLE IF NOT EXISTS ecom_dwh.dim_date (
    date_key INTEGER PRIMARY KEY, -- YYYYMMDD
    full_date DATE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name TEXT,
    day INTEGER,
    weekday_name TEXT
);

-- FACT TABLE
CREATE TABLE IF NOT EXISTS ecom_dwh.fact_sales (
    sales_id SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES ecom_dwh.dim_date(date_key),
    product_key INTEGER REFERENCES ecom_dwh.dim_product(product_key),
    customer_key INTEGER REFERENCES ecom_dwh.dim_customer(customer_key),
    country_key INTEGER REFERENCES ecom_dwh.dim_country(country_key),
    invoice_no TEXT, -- Degenerate dimension
    quantity INTEGER,
    unit_price NUMERIC(10, 2),
    revenue NUMERIC(12, 2),
    is_cancelled BOOLEAN
);

-- Indexes for performance
CREATE INDEX idx_fact_sales_date ON ecom_dwh.fact_sales(date_key);
CREATE INDEX idx_fact_sales_product ON ecom_dwh.fact_sales(product_key);
CREATE INDEX idx_fact_sales_customer ON ecom_dwh.fact_sales(customer_key);
