-- 01_source_ddl.sql
DROP TABLE IF EXISTS ecom_source.raw_invoices;

CREATE TABLE ecom_source.raw_invoices (
    InvoiceNo TEXT,
    StockCode TEXT,
    Description TEXT,
    Quantity TEXT,
    InvoiceDate TEXT,
    UnitPrice TEXT,
    CustomerID TEXT,
    Country TEXT
);
