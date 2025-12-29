#!/usr/bin/env python3
"""データベース接続テストツール

Supabase PostgreSQLとCloud SQLの接続をテストします。

Usage:
    python scripts/helpers/connection_tester.py
"""
import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def test_supabase_connection():
    """Supabase PostgreSQL接続テスト"""
    print("\n" + "="*60)
    print("Supabase PostgreSQL 接続テスト")
    print("="*60)

    host = os.getenv('SUPABASE_DB_HOST')
    port = os.getenv('SUPABASE_DB_PORT', '5432')
    database = os.getenv('SUPABASE_DB_NAME', 'postgres')
    user = os.getenv('SUPABASE_DB_USER')
    password = os.getenv('SUPABASE_DB_PASSWORD')

    # 環境変数の確認
    if not all([host, user, password]):
        print("❌ 環境変数が設定されていません:")
        if not host:
            print("  - SUPABASE_DB_HOST")
        if not user:
            print("  - SUPABASE_DB_USER")
        if not password:
            print("  - SUPABASE_DB_PASSWORD")
        return False

    print(f"接続先: {host}:{port}/{database}")

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=10
        )

        # 簡単なクエリでテスト
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        print("✅ Supabase接続成功")
        print(f"バージョン: {version.split(',')[0]}")
        return True

    except psycopg2.OperationalError as e:
        print(f"❌ Supabase接続失敗: {e}")
        print("\n確認事項:")
        print("1. SUPABASE_DB_HOSTが正しいか")
        print("2. SUPABASE_DB_PASSWORDが正しいか")
        print("3. ネットワーク接続が正常か")
        return False

    except Exception as e:
        print(f"❌ Supabase接続失敗: {e}")
        return False


def test_cloudsql_connection():
    """Cloud SQL接続テスト"""
    print("\n" + "="*60)
    print("Cloud SQL 接続テスト")
    print("="*60)

    host = os.getenv('CLOUD_SQL_HOST')
    port = os.getenv('CLOUD_SQL_PORT', '5432')
    database = os.getenv('CLOUD_SQL_DATABASE')
    user = os.getenv('CLOUD_SQL_USER')
    password = os.getenv('CLOUD_SQL_PASSWORD')

    # 環境変数の確認
    if not all([host, database, user, password]):
        print("❌ 環境変数が設定されていません:")
        if not host:
            print("  - CLOUD_SQL_HOST")
        if not database:
            print("  - CLOUD_SQL_DATABASE")
        if not user:
            print("  - CLOUD_SQL_USER")
        if not password:
            print("  - CLOUD_SQL_PASSWORD")
        return False

    print(f"接続先: {host}:{port}/{database}")

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=10
        )

        # 簡単なクエリでテスト
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        print("✅ Cloud SQL接続成功")
        print(f"バージョン: {version.split(',')[0]}")
        return True

    except psycopg2.OperationalError as e:
        print(f"❌ Cloud SQL接続失敗: {e}")
        print("\n確認事項:")
        print("1. Cloud SQLインスタンスが起動しているか")
        print("2. IPアドレスが許可リストに追加されているか")
        print("3. CLOUD_SQL_PASSWORDが正しいか")
        return False

    except Exception as e:
        print(f"❌ Cloud SQL接続失敗: {e}")
        return False


def main():
    """メイン処理"""
    print("\n" + "="*60)
    print("データベース接続テスト")
    print("="*60)

    supabase_ok = test_supabase_connection()
    cloudsql_ok = test_cloudsql_connection()

    print("\n" + "="*60)
    print("テスト結果サマリー")
    print("="*60)
    print(f"Supabase PostgreSQL: {'✅ OK' if supabase_ok else '❌ NG'}")
    print(f"Cloud SQL:           {'✅ OK' if cloudsql_ok else '❌ NG'}")
    print("="*60)

    if supabase_ok and cloudsql_ok:
        print("\n✅ すべての接続テスト成功")
        sys.exit(0)
    else:
        print("\n❌ 接続テスト失敗 - .envファイルを確認してください")
        sys.exit(1)


if __name__ == '__main__':
    main()
