import pandas as pd
from common import get_db_connection
import os

def load_source():
    print("ETL Step 1: Loading Source Data...")
    
    file_path = os.path.join(os.path.dirname(__file__), '../data/data.csv')
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return

    # Read CSV with all columns as string to avoid type errors during raw load
    try:
        df = pd.read_csv(file_path, dtype=str, encoding='ISO-8859-1') # Typical encoding for this dataset
        print(f"Read {len(df)} rows from CSV.")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # Map columns to DB schema names (lowercase for Postgres)
    df.columns = df.columns.str.lower()
    # Now df columns are: invoiceno, stockcode, description, quantity, invoicedate, unitprice, customerid, country

    engine = get_db_connection()
    
    # Truncate and Load
    try:
        with engine.connect() as conn:
            from sqlalchemy import text
            conn.execute(text("TRUNCATE TABLE ecom_source.raw_invoices"))
            conn.commit()
        
        df.to_sql('raw_invoices', engine, schema='ecom_source', if_exists='append', index=False)
        print("Successfully loaded data into ecom_source.raw_invoices")
    except Exception as e:
        print(f"Error loading to DB: {e}")

if __name__ == "__main__":
    load_source()
