#!/usr/bin/env python3
"""
Cloud SQLにスキーマを作成

Usage:
    python scripts/create_cloud_sql_schema.py
"""
import os
import psycopg2
from psycopg2 import sql
import sys
from dotenv import load_dotenv

load_dotenv()

# Cloud SQL接続情報（環境変数から取得）
DB_HOST = os.getenv('CLOUD_SQL_HOST')
DB_PORT = int(os.getenv('CLOUD_SQL_PORT', '5432'))
DB_NAME = os.getenv('CLOUD_SQL_DATABASE')
DB_USER = os.getenv('CLOUD_SQL_USER')
DB_PASSWORD = os.getenv('CLOUD_SQL_PASSWORD')


def create_schema():
    """スキーマファイルを実行してテーブルを作成"""
    print("=" * 60)
    print("Cloud SQL スキーマ作成")
    print("=" * 60)
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

        # スキーマファイルを読み込み
        schema_file = "scripts/sql/schema/create_tables_cloudsql.sql"
        print(f"\nスキーマファイル読み込み: {schema_file}")

        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_sql = f.read()

        # スキーマ実行
        print("\nスキーマ作成中...")
        cursor.execute(schema_sql)

        print("✅ スキーマ作成完了")

        # 作成されたテーブルを確認
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)

        tables = cursor.fetchall()
        print(f"\n作成されたテーブル ({len(tables)}個):")
        for table in tables:
            print(f"  - {table[0]}")

        cursor.close()
        conn.close()

        print("\n" + "=" * 60)
        print("完了！")
        print("=" * 60)

        return True

    except psycopg2.OperationalError as e:
        print(f"\n❌ 接続エラー: {e}")
        print("\n確認事項:")
        print("1. Cloud SQLインスタンスが起動しているか")
        print("2. IPアドレスが許可リストに追加されているか")
        print("3. ユーザー名・パスワードが正しいか")
        return False

    except Exception as e:
        print(f"\n❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = create_schema()
    sys.exit(0 if success else 1)
