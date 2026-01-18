#!/usr/bin/env python3
"""
エクスポートしたJSONファイルをCloud SQLにインポート

Usage:
    python scripts/import_to_cloudsql.py
"""
import os
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import glob
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Cloud SQL接続情報（環境変数から取得）
DB_HOST = os.getenv('CLOUD_SQL_HOST')
DB_PORT = int(os.getenv('CLOUD_SQL_PORT', '5432'))
DB_NAME = os.getenv('CLOUD_SQL_DATABASE')
DB_USER = os.getenv('CLOUD_SQL_USER')
DB_PASSWORD = os.getenv('CLOUD_SQL_PASSWORD')

EXPORT_DIR = "data_exports"


def get_table_columns(cursor, table_name):
    """テーブルのカラム一覧を取得"""
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    return [row[0] for row in cursor.fetchall()]


def import_table_data(cursor, table_name, json_file, batch_size=500):
    """JSONファイルからデータをインポート"""
    print(f"\n{'='*60}")
    print(f"テーブル: {table_name}")
    print(f"ファイル: {json_file}")
    print(f"{'='*60}")

    # JSONファイルを読み込み
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    total_count = len(data)
    print(f"総レコード数: {total_count:,}")

    if total_count == 0:
        print("⚠️  データが空です。スキップします。")
        return 0

    # テーブルのカラム一覧を取得
    table_columns = get_table_columns(cursor, table_name)
    print(f"カラム数: {len(table_columns)}")

    # データからカラム名を抽出（JSONキーとテーブルカラムの共通部分）
    if total_count > 0:
        json_keys = set(data[0].keys())
        columns_to_insert = [col for col in table_columns if col in json_keys]
        print(f"インポート対象カラム: {len(columns_to_insert)}/{len(table_columns)}")

    # INSERT文を構築
    placeholders = ', '.join(['%s'] * len(columns_to_insert))
    insert_query = sql.SQL(
        "INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    ).format(
        table=sql.Identifier(table_name),
        columns=sql.SQL(', ').join(map(sql.Identifier, columns_to_insert)),
        placeholders=sql.SQL(placeholders)
    )

    # バッチでデータを挿入
    inserted_count = 0
    for i in range(0, total_count, batch_size):
        batch = data[i:i+batch_size]
        values = [
            tuple(record.get(col) for col in columns_to_insert)
            for record in batch
        ]

        try:
            execute_batch(cursor, insert_query, values, page_size=batch_size)
            inserted_count += len(batch)
            print(f"挿入中... {inserted_count:,} / {total_count:,} ({(inserted_count/total_count*100):.1f}%)", end='\r')
        except Exception as e:
            print(f"\n❌ バッチ挿入エラー: {e}")
            # 個別に挿入を試みる
            for value in values:
                try:
                    cursor.execute(insert_query, value)
                    inserted_count += 1
                except Exception as e2:
                    print(f"\n⚠️  レコード挿入エラー (スキップ): {e2}")

    print(f"\n✅ インポート完了: {inserted_count:,} レコード")
    return inserted_count


def main():
    """メイン処理"""
    print("\n" + "="*60)
    print("Cloud SQL データインポート")
    print("="*60)
    print(f"インポート元: {EXPORT_DIR}/")

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
        conn.autocommit = False
        cursor = conn.cursor()
        print("✅ 接続成功")

        # エクスポートファイルを検索
        import_summary = {}

        # 各テーブルの最新エクスポートファイルを見つける
        for table_name in ['bond_data', 'boj_holdings', 'irs_data', 'bond_auction']:
            files = glob.glob(f"{EXPORT_DIR}/{table_name}_*.json")

            if not files:
                print(f"\n⚠️  {table_name} のエクスポートファイルが見つかりません")
                import_summary[table_name] = {'status': 'not_found', 'count': 0}
                continue

            # 最新のファイルを選択
            latest_file = max(files)

            try:
                count = import_table_data(cursor, table_name, latest_file)
                conn.commit()
                import_summary[table_name] = {'status': 'success', 'count': count}
            except Exception as e:
                print(f"\n❌ {table_name} のインポート中にエラー: {e}")
                conn.rollback()
                import_summary[table_name] = {'status': 'error', 'error': str(e)}

        # サマリー表示
        print("\n" + "="*60)
        print("インポート完了サマリー")
        print("="*60)
        total_imported = 0
        for table, info in import_summary.items():
            status_icon = {
                'success': '✅',
                'not_found': '⚠️ ',
                'error': '❌'
            }.get(info['status'], '?')
            count = info.get('count', 0)
            total_imported += count
            print(f"{status_icon} {table}: {info['status']} ({count:,} レコード)")

        print(f"\n総インポート数: {total_imported:,} レコード")

        cursor.close()
        conn.close()

        print("\n次のステップ:")
        print("1. requirements.txtを更新 (asyncpg, psycopg2追加)")
        print("2. .envにCloud SQL設定を追加")
        print("3. app/core/config.pyを修正")
        print("4. データベース接続層を書き換え")

        return True

    except psycopg2.OperationalError as e:
        print(f"\n❌ 接続エラー: {e}")
        return False

    except Exception as e:
        print(f"\n❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
