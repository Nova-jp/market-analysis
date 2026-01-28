#!/usr/bin/env python3
"""
対内対外証券投資（週次）テーブル作成スクリプト

Usage:
    python scripts/create_mof_international_transactions_table.py
"""
import os
import psycopg2
import sys
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# データベース接続情報（.envから取得）
# 優先順位: DB_... (Neon/Standard) -> CLOUD_SQL_... (Cloud SQL)
DB_HOST = os.getenv('DB_HOST') or os.getenv('CLOUD_SQL_HOST')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME') or os.getenv('CLOUD_SQL_DATABASE')
DB_USER = os.getenv('DB_USER') or os.getenv('CLOUD_SQL_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD') or os.getenv('CLOUD_SQL_PASSWORD')


def create_table():
    """SQLファイルを実行してテーブルを作成"""
    print("=" * 60)
    print("対内対外証券投資テーブル作成")
    print("=" * 60)
    
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        print("❌ エラー: データベース接続情報が不足しています。")
        print(".envファイルの DB_HOST, DB_NAME, DB_USER, DB_PASSWORD を確認してください。")
        return False

    print(f"接続先: {DB_HOST}:{DB_PORT}/{DB_NAME}")

    try:
        # データベースに接続
        print("\n接続中...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=10
        )
        conn.autocommit = True
        cursor = conn.cursor()

        print("✅ 接続成功")

        # SQLファイルを読み込み
        sql_path = "scripts/sql/schema/create_mof_international_transactions_table.sql"
        print(f"\nSQLファイル読み込み: {sql_path}")
        
        if not os.path.exists(sql_path):
            print(f"❌ エラー: ファイルが見つかりません: {sql_path}")
            return False

        with open(sql_path, 'r', encoding='utf-8') as f:
            create_sql = f.read()

        # SQL実行
        print("\nテーブル作成中...")
        cursor.execute(create_sql)

        print("✅ テーブル作成完了")

        # 確認
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'mof_international_transactions'
            ORDER BY ordinal_position;
        """ 
        )
        
        columns = cursor.fetchall()
        print(f"\n作成されたカラム ({len(columns)}個):")
        for col in columns:
            print(f"  - {col[0]} ({col[1]})")

        cursor.close()
        conn.close()

        print("\n" + "=" * 60)
        print("完了！")
        print("=" * 60)

        return True

    except Exception as e:
        print(f"\n❌ エラー: {e}")
        return False


if __name__ == '__main__':
    success = create_table()
    sys.exit(0 if success else 1)
