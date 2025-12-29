#!/usr/bin/env python3
"""
SupabaseとCloud SQLのデータ整合性を確認
"""
import os
from dotenv import load_dotenv
from supabase import create_client
import psycopg2

load_dotenv()

# Supabase設定（環境変数から取得）
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Cloud SQL設定（環境変数から取得）
CLOUDSQL_HOST = os.getenv('CLOUD_SQL_HOST')
CLOUDSQL_PORT = int(os.getenv('CLOUD_SQL_PORT', '5432'))
CLOUDSQL_DB = os.getenv('CLOUD_SQL_DATABASE')
CLOUDSQL_USER = os.getenv('CLOUD_SQL_USER')
CLOUDSQL_PASSWORD = os.getenv('CLOUD_SQL_PASSWORD')


def get_supabase_counts():
    """Supabaseのレコード数を取得"""
    print("\n" + "="*60)
    print("Supabase レコード数")
    print("="*60)

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    counts = {}

    for table in ['bond_data', 'boj_holdings', 'bond_auction', 'irs_settlement_rates']:
        response = supabase.table(table).select('*', count='exact').limit(1).execute()
        counts[table] = response.count
        print(f"{table:25s}: {response.count:,}")

    return counts


def get_cloudsql_counts():
    """Cloud SQLのレコード数を取得"""
    print("\n" + "="*60)
    print("Cloud SQL レコード数")
    print("="*60)

    conn = psycopg2.connect(
        host=CLOUDSQL_HOST,
        port=CLOUDSQL_PORT,
        database=CLOUDSQL_DB,
        user=CLOUDSQL_USER,
        password=CLOUDSQL_PASSWORD
    )
    cursor = conn.cursor()

    counts = {}

    for table in ['bond_data', 'boj_holdings', 'bond_auction', 'irs_settlement_rates']:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        counts[table] = count
        print(f"{table:25s}: {count:,}")

    cursor.close()
    conn.close()

    return counts


def compare_counts(supabase_counts, cloudsql_counts):
    """レコード数を比較"""
    print("\n" + "="*60)
    print("比較結果")
    print("="*60)
    print(f"{'テーブル名':<25s} {'Supabase':>12s} {'Cloud SQL':>12s} {'差分':>12s} {'状態':>8s}")
    print("-"*60)

    all_match = True

    for table in supabase_counts.keys():
        supabase_count = supabase_counts[table]
        cloudsql_count = cloudsql_counts[table]
        diff = cloudsql_count - supabase_count
        status = "✅ OK" if diff == 0 else "⚠️ 不一致"

        if diff != 0:
            all_match = False

        print(f"{table:<25s} {supabase_count:>12,} {cloudsql_count:>12,} {diff:>12,} {status:>8s}")

    return all_match


def get_bond_data_details():
    """bond_dataの詳細を確認"""
    print("\n" + "="*60)
    print("bond_data 詳細比較")
    print("="*60)

    # Cloud SQL
    conn = psycopg2.connect(
        host=CLOUDSQL_HOST,
        port=CLOUDSQL_PORT,
        database=CLOUDSQL_DB,
        user=CLOUDSQL_USER,
        password=CLOUDSQL_PASSWORD
    )
    cursor = conn.cursor()

    print("\nCloud SQL:")
    cursor.execute("""
        SELECT
            MIN(trade_date) as earliest,
            MAX(trade_date) as latest,
            COUNT(DISTINCT trade_date) as unique_dates,
            COUNT(DISTINCT bond_code) as unique_bonds
        FROM bond_data
    """)
    result = cursor.fetchone()
    print(f"  期間: {result[0]} 〜 {result[1]}")
    print(f"  ユニーク取引日: {result[2]:,}")
    print(f"  ユニーク銘柄: {result[3]:,}")

    # 年別レコード数
    cursor.execute("""
        SELECT
            EXTRACT(YEAR FROM trade_date) as year,
            COUNT(*) as records
        FROM bond_data
        GROUP BY EXTRACT(YEAR FROM trade_date)
        ORDER BY year
    """)
    print("\n  年別レコード数:")
    for row in cursor.fetchall():
        print(f"    {int(row[0])}: {row[1]:,}")

    cursor.close()
    conn.close()

    # Supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("\nSupabase:")
    # サンプルで最小・最大を確認
    response_min = supabase.table('bond_data').select('trade_date').order('trade_date', desc=False).limit(1).execute()
    response_max = supabase.table('bond_data').select('trade_date').order('trade_date', desc=True).limit(1).execute()

    if response_min.data and response_max.data:
        print(f"  期間: {response_min.data[0]['trade_date']} 〜 {response_max.data[0]['trade_date']}")


def main():
    """メイン処理"""
    print("\n" + "="*60)
    print("Supabase → Cloud SQL データ移行検証")
    print("="*60)

    try:
        # レコード数取得
        supabase_counts = get_supabase_counts()
        cloudsql_counts = get_cloudsql_counts()

        # 比較
        all_match = compare_counts(supabase_counts, cloudsql_counts)

        # bond_dataの詳細
        get_bond_data_details()

        # 結果サマリー
        print("\n" + "="*60)
        if all_match:
            print("✅ すべてのデータが正しく移行されています！")
        else:
            print("⚠️ データに不一致があります。詳細を確認してください。")
        print("="*60)

    except Exception as e:
        print(f"\n❌ エラー: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
