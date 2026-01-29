import os
from sqlalchemy import text
from etl.common import get_db_connection

def init_db():
    print("Initializing Database Schemas...")
    engine = get_db_connection()
    
    scripts = [
        'sql/00_init_schemas.sql',
        'sql/01_source_ddl.sql',
        'sql/02_staging_ddl.sql',
        'sql/03_dwh_ddl.sql'
    ]
    
    try:
        with engine.connect() as conn:
            for script_path in scripts:
                full_path = os.path.join(os.path.dirname(__file__), script_path)
                print(f"Running {script_path}...")
                with open(full_path, 'r') as f:
                    sql_content = f.read()
                    # Split by semicolon to handle multiple statements if supported, 
                    # but sqlalchemy execute might handle the whole block if valid.
                    # Text wrapping is safer.
                    conn.execute(text(sql_content))
            conn.commit()
        print("Database initialized successfully.")
    except Exception as e:
        print(f"Error initializing database: {e}")

if __name__ == "__main__":
    init_db()
