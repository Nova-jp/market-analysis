import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from app.core.config import settings

def check_table():
    print(f"Connecting to database...")
    engine = create_engine(settings.database_url)
    
    with engine.connect() as conn:
        print("\n--- 1. Table Columns (ASW_data) ---")
        # Get column names
        result = conn.execute(text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'ASW_data'
            ORDER BY ordinal_position
        """))
        for row in result:
            print(f"{row[0]}: {row[1]}")

        print("\n--- 2. Sample Data (LIMIT 5) ---")
        try:
            # Query with existing columns only
            result = conn.execute(text('SELECT * FROM "ASW_data" LIMIT 5'))
            rows = result.fetchall()
            if not rows:
                print("No data found in table.")
            for row in rows:
                print(row)
        except Exception as e:
            print(f"Error querying data: {e}")

if __name__ == "__main__":
    check_table()
