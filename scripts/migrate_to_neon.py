import os
import subprocess
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def run_command(cmd, env):
    try:
        subprocess.run(cmd, env=env, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        return False

def main():
    # 環境変数の読み込み
    s_host = os.getenv("SUPABASE_DB_HOST")
    s_port = os.getenv("SUPABASE_DB_PORT", "5432")
    s_db = os.getenv("SUPABASE_DB_NAME", "postgres")
    s_user = os.getenv("SUPABASE_DB_USER", "postgres")
    s_pass = os.getenv("SUPABASE_DB_PASSWORD")

    n_host = os.getenv("NEON_HOST")
    n_port = os.getenv("NEON_PORT", "5432")
    n_db = os.getenv("NEON_DATABASE", "neondb")
    n_user = os.getenv("NEON_USER")
    n_pass = os.getenv("NEON_PASSWORD")

    if not all([s_host, s_pass, n_host, n_pass]):
        print("Error: Missing required environment variables in .env")
        return

    dump_file = "supabase_dump.bak"
    
    # 1. pg_dump from Supabase
    print(f"\n[{datetime.now()}] Dumping from Supabase ({s_host})...")
    dump_env = os.environ.copy()
    dump_env["PGPASSWORD"] = s_pass
    
    dump_cmd = [
        "pg_dump",
        "-h", s_host,
        "-p", s_port,
        "-U", s_user,
        "-d", s_db,
        "--no-owner",
        "--no-acl",
        "-F", "c",
        "-f", dump_file,
        "--schema=public"  # Only dump public schema
    ]
    
    if not run_command(dump_cmd, dump_env):
        print("Dump failed.")
        return

    file_size = os.path.getsize(dump_file)
    print(f"Dump successful. File size: {file_size / (1024*1024):.2f} MB")

    # 2. pg_restore to Neon
    print(f"\n[{datetime.now()}] Restoring to Neon ({n_host})...")
    restore_env = os.environ.copy()
    restore_env["PGPASSWORD"] = n_pass

    restore_cmd = [
        "pg_restore",
        "-h", n_host,
        "-p", n_port,
        "-U", n_user,
        "-d", n_db,
        "--no-owner",
        "--no-acl",
        "-v",  # Add verbose output
        dump_file
    ]

    if run_command(restore_cmd, restore_env):
        print("\nMigration completed successfully!")
    else:
        print("\nRestore finished with possible errors/warnings.")

if __name__ == "__main__":
    main()
