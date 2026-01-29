-- 02_staging_ddl.sql
DROP TABLE IF EXISTS ecom_staging.stg_sales;

CREATE TABLE ecom_staging.stg_sales (
    stg_id SERIAL PRIMARY KEY,
    invoice_no TEXT,
    stock_code TEXT,
    description TEXT,
    quantity INTEGER,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(10, 2),
    customer_id TEXT,
    country TEXT,
    revenue NUMERIC(12, 2),
    is_cancelled BOOLEAN,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
