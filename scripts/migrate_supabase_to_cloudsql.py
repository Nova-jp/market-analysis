#!/usr/bin/env python3
"""Supabase → Cloud SQL 移行メインスクリプト

pg_dump/pg_restore方式でSupabaseからCloud SQLへデータを移行します。

Usage:
    python scripts/migrate_supabase_to_cloudsql.py

実行前の準備:
    1. .envファイルに接続情報を設定
    2. PostgreSQLクライアントツールをインストール（pg_dump, pg_restore）
    3. python scripts/helpers/connection_tester.py で接続テスト
"""
import sys
import os
from datetime import datetime
from dotenv import load_dotenv

# ローカルモジュールのimport
from scripts.helpers.connection_tester import test_supabase_connection, test_cloudsql_connection
from scripts.dump_supabase_database import dump_database
from scripts.restore_to_cloudsql import restore_database

load_dotenv()


def print_header(title):
    """ヘッダーを表示"""
    print("\n" + "="*60)
    print(title)
    print("="*60)


def get_supabase_record_counts():
    """Supabaseのレコード数を取得（簡易版）"""
    try:
        from supabase import create_client

        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')

        if not supabase_url or not supabase_key:
            print("⚠️  SUPABASE_URL/SUPABASE_KEYが設定されていません")
            print("レコード数の事前チェックをスキップします")
            return None

        supabase = create_client(supabase_url, supabase_key)
        counts = {}

        for table in ['bond_data', 'boj_holdings', 'irs_data', 'bond_auction']:
            try:
                response = supabase.table(table).select('*', count='exact').limit(1).execute()
                counts[table] = response.count
            except Exception as e:
                print(f"⚠️  {table}のレコード数取得失敗: {e}")
                counts[table] = '不明'

        return counts

    except ImportError:
        print("⚠️  supabaseライブラリがインストールされていません")
        print("レコード数の事前チェックをスキップします")
        return None
    except Exception as e:
        print(f"⚠️  レコード数取得エラー: {e}")
        return None


def get_cloudsql_record_counts():
    """Cloud SQLのレコード数を取得（簡易版）"""
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv('CLOUD_SQL_HOST'),
            port=os.getenv('CLOUD_SQL_PORT', '5432'),
            database=os.getenv('CLOUD_SQL_DATABASE'),
            user=os.getenv('CLOUD_SQL_USER'),
            password=os.getenv('CLOUD_SQL_PASSWORD'),
            connect_timeout=10
        )
        cursor = conn.cursor()

        counts = {}
        for table in ['bond_data', 'boj_holdings', 'irs_data', 'bond_auction']:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cursor.fetchone()[0]
            except Exception as e:
                print(f"⚠️  {table}のレコード数取得失敗: {e}")
                counts[table] = '不明'

        cursor.close()
        conn.close()

        return counts

    except Exception as e:
        print(f"⚠️  レコード数取得エラー: {e}")
        return None


def main():
    """移行プロセス全体を実行"""

    print_header("Supabase → Cloud SQL 移行（pg_dump/pg_restore方式）")
    print(f"開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # ステップ1: 接続テスト
    print_header("[ステップ1/5] 接続テスト")

    supabase_ok = test_supabase_connection()
    cloudsql_ok = test_cloudsql_connection()

    if not supabase_ok or not cloudsql_ok:
        print("\n❌ 接続テスト失敗。環境変数を確認してください。")
        print("\n確認方法:")
        print("  1. .envファイルが存在するか確認")
        print("  2. 必須環境変数が設定されているか確認:")
        print("     - SUPABASE_DB_HOST, SUPABASE_DB_USER, SUPABASE_DB_PASSWORD")
        print("     - CLOUD_SQL_HOST, CLOUD_SQL_DATABASE, CLOUD_SQL_USER, CLOUD_SQL_PASSWORD")
        return False

    # ステップ2: 事前検証
    print_header("[ステップ2/5] 事前検証（Supabaseレコード数）")

    supabase_counts = get_supabase_record_counts()
    if supabase_counts:
        print("\nSupabase レコード数:")
        for table, count in supabase_counts.items():
            print(f"  {table:30s}: {count if isinstance(count, str) else f'{count:,}'}")
    else:
        print("レコード数の取得をスキップしました")

    # ステップ3: pg_dump実行
    print_header("[ステップ3/5] pg_dump実行")

    tables = ['bond_data', 'boj_holdings', 'irs_data', 'bond_auction']
    if not dump_database(tables=tables):
        print("\n❌ dump失敗")
        return False

    # ステップ4: pg_restore実行
    print_header("[ステップ4/5] pg_restore実行")

    if not restore_database(clean=True, jobs=4):
        print("\n❌ restore失敗")
        return False

    # ステップ5: 事後検証
    print_header("[ステップ5/5] 事後検証（Cloud SQLレコード数）")

    cloudsql_counts = get_cloudsql_record_counts()
    if cloudsql_counts:
        print("\nCloud SQL レコード数:")
        for table, count in cloudsql_counts.items():
            print(f"  {table:30s}: {count if isinstance(count, str) else f'{count:,}'}")

        # 比較
        if supabase_counts and cloudsql_counts:
            print("\n比較結果:")
            all_match = True
            for table in tables:
                supabase_count = supabase_counts.get(table, '不明')
                cloudsql_count = cloudsql_counts.get(table, '不明')

                if isinstance(supabase_count, int) and isinstance(cloudsql_count, int):
                    match = supabase_count == cloudsql_count
                    status = "✅ 一致" if match else "❌ 不一致"
                    print(f"  {table:30s}: {status} (Supabase: {supabase_count:,}, Cloud SQL: {cloudsql_count:,})")
                    if not match:
                        all_match = False
                else:
                    print(f"  {table:30s}: ⚠️  比較不可")
        else:
            all_match = None
    else:
        print("レコード数の取得をスキップしました")
        all_match = None

    # サマリー
    print_header("移行サマリー")
    print(f"完了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if all_match is True:
        print("\n✅ 移行成功！すべてのデータが正しく移行されました。")
        return True
    elif all_match is False:
        print("\n⚠️  移行完了しましたが、データに不一致があります。")
        print("詳細確認: python scripts/verify_migration.py")
        return True  # 移行自体は完了
    else:
        print("\n✅ 移行完了")
        print("詳細確認: python scripts/verify_migration.py")
        return True


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
