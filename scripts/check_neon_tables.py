import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def list_tables():
    host = os.getenv("NEON_HOST")
    user = os.getenv("NEON_USER")
    password = os.getenv("NEON_PASSWORD")
    database = os.getenv("NEON_DATABASE", "neondb")
    port = os.getenv("NEON_PORT", "5432")

    try:
        conn = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )
        cur = conn.cursor()
        
        print("\nChecking tables in Neon database...")
        cur.execute("""
            SELECT schemaname, tablename 
            FROM pg_tables 
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schemaname, tablename;
        """)
        
        rows = cur.fetchall()
        if not rows:
            print("No user tables found.")
        else:
            print(f"Found {len(rows)} tables:")
            for schema, table in rows:
                print(f" - {schema}.{table}")
                
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_tables()
