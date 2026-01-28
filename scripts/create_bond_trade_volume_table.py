#!/usr/bin/env python3
"""
Create bond_trade_volume_by_investor table in the database.

Usage:
    ./venv/bin/python scripts/create_bond_trade_volume_table.py
"""
import sys
import psycopg2
from app.core.config import settings

def create_table():
    print("=" * 60)
    print("Creating bond_trade_volume_by_investor table")
    print("=" * 60)
    
    try:
        # Connect using the URL from settings (includes sslmode=require usually)
        print(f"Connecting to {settings.db_host}...")
        conn = psycopg2.connect(settings.database_url)
        conn.autocommit = True
        cursor = conn.cursor()
        
        sql_file = "scripts/sql/create_bond_trade_volume_table.sql"
        print(f"Reading SQL file: {sql_file}")
        
        with open(sql_file, 'r') as f:
            sql_script = f.read()
            
        print("Executing SQL script...")
        cursor.execute(sql_script)
        
        print("✅ Table created successfully.")
        
        # Verify
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'bond_trade_volume_by_investor'
            ORDER BY ordinal_position;
        """)
        columns = cursor.fetchall()
        print("\nColumns:")
        for col in columns:
            print(f"  - {col[0]}: {col[1]}")
            
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = create_table()
    sys.exit(0 if success else 1)
