import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'ecom_db')

def get_db_connection():
    """Returns a SQLAlchemy engine."""
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def execute_query(query):
    """Executes a raw SQL query and commits the transaction."""
    engine = get_db_connection()
    # engine.begin() automatically commits on success, rolls back on failure
    with engine.begin() as conn:
        conn.execute(query)
