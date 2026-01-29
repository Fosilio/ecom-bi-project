import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')

# Connect to 'postgres' db to create the new db
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres"
engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")

try:
    with engine.connect() as conn:
        # Check if database exists
        result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname='ecom_db'"))
        if not result.fetchone():
            conn.execute(text("CREATE DATABASE ecom_db"))
            print("Database 'ecom_db' created successfully.")
        else:
            print("Database 'ecom_db' already exists.")
except Exception as e:
    print(f"Failed to create database: {e}")
