import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Get config
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
dbname = os.getenv("DB_NAME")

# Check if env vars are loaded
if not all([user, password, host, port, dbname]):
    print("Error: Missing database configuration in .env file.")
    exit(1)

connection_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
print(f"Connecting to {connection_str.replace(password, '******')}...")

try:
    engine = create_engine(connection_str)
    with engine.connect() as conn:
        print("Connection successful!")
        
        # Check tables in ecom_dwh
        query_tables = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'ecom_dwh'
            ORDER BY table_name;
        """)
        tables = pd.read_sql(query_tables, conn)
        
        if tables.empty:
            print("\nWARNING: No tables found in 'ecom_dwh' schema!")
            
            # Check if other schemas have data
            print("\nChecking other schemas...")
            query_schemas = text("SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema LIKE 'ecom_%'")
            schemas = pd.read_sql(query_schemas, conn)
            print(f"Found schemas: {schemas['table_schema'].tolist()}")

        else:
            print(f"\nFound {len(tables)} tables in 'ecom_dwh':")
            print(tables['table_name'].tolist())
            
            print("\nRow Counts:")
            for table in tables['table_name']:
                try:
                    count_query = text(f"SELECT COUNT(*) FROM ecom_dwh.{table}")
                    count = conn.execute(count_query).scalar()
                    print(f"- {table}: {count} rows")
                    
                    # Sample fact table
                    if table == 'fact_sales' and count > 0:
                         print(f"\nSample data from {table}:")
                         sample = pd.read_sql(text(f"SELECT * FROM ecom_dwh.{table} LIMIT 5"), conn)
                         print(sample.to_string())
                except Exception as table_e:
                    print(f"Error querying {table}: {table_e}")

except Exception as e:
    print(f"Database Error: {e}")
