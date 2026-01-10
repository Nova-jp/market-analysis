import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from app.core.config import settings

def setup_table():
    print(f"Connecting to database...")
    engine = create_engine(settings.database_url)
    
    sql_file = project_root / "scripts/sql/create_asw_data_table.sql"
    with open(sql_file, "r") as f:
        sql = f.read()
        
    print(f"Executing SQL from {sql_file}")
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
        print("✅ ASW_data table created successfully.")

if __name__ == "__main__":
    setup_table()
