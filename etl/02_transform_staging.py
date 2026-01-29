import pandas as pd
import numpy as np
from common import get_db_connection
from sqlalchemy import text

def transform_staging():
    print("ETL Step 2: Transforming to Staging...")
    engine = get_db_connection()
    
    # Extract Raw Data
    query = "SELECT * FROM ecom_source.raw_invoices"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("No data in source.")
        return

    # Transformations
    # 1. Clean Strings
    df['description'] = df['description'].str.strip()
    df['stock_code'] = df['stockcode'].str.strip()
    df['invoice_no'] = df['invoiceno'].str.strip()
    df['country'] = df['country'].str.strip()
    
    # 2. Key Handling
    df['customer_id'] = df['customerid'].fillna('Unknown')
    
    # 3. Type Conversion
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
    df['unit_price'] = pd.to_numeric(df['unitprice'], errors='coerce').fillna(0.0)
    df['invoice_date'] = pd.to_datetime(df['invoicedate'], errors='coerce')
    
    # 4. Metrics
    df['revenue'] = df['quantity'] * df['unit_price']
    
    # 5. Logic
    df['is_cancelled'] = df['invoice_no'].str.upper().str.startswith('C').fillna(False)
    
    # 6. Filter Invalid Data (Simple rule: Price must be >= 0)
    # Note: Refunds have negative quantity but positive price potentially, or negative revenue. 
    # Validating simple structural integrity here.
    df = df[df['unit_price'] >= 0]
    
    # Select and Rename for Staging
    staging_df = df[[
        'invoice_no', 'stock_code', 'description', 
        'quantity', 'invoice_date', 'unit_price', 
        'customer_id', 'country', 'revenue', 'is_cancelled'
    ]].copy()
    
    # Load to Staging
    try:
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE ecom_staging.stg_sales RESTART IDENTITY"))
            conn.commit()
            
        staging_df.to_sql('stg_sales', engine, schema='ecom_staging', if_exists='append', index=False)
        print(f"Successfully loaded {len(staging_df)} rows into ecom_staging.stg_sales")
    except Exception as e:
        print(f"Error loading staging: {e}")

if __name__ == "__main__":
    transform_staging()
