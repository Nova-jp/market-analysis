#!/usr/bin/env python3
"""
年別JSONファイルをCloud SQLにインポート

Usage:
    python scripts/import_bond_data_to_cloudsql.py
"""
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import glob
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# Cloud SQL接続情報（環境変数から取得）
DB_HOST = os.getenv('CLOUD_SQL_HOST')
DB_PORT = int(os.getenv('CLOUD_SQL_PORT', '5432'))
DB_NAME = os.getenv('CLOUD_SQL_DATABASE')
DB_USER = os.getenv('CLOUD_SQL_USER')
DB_PASSWORD = os.getenv('CLOUD_SQL_PASSWORD')

EXPORT_DIR = "data_exports/bond_data_by_year"


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


def import_json_file(cursor, json_file, table_columns, batch_size=500):
    """
    JSONファイルからデータをインポート

    Args:
        cursor: データベースカーソル
        json_file: JSONファイルパス
        table_columns: テーブルカラムリスト
        batch_size: バッチサイズ

    Returns:
        インポートしたレコード数
    """
    year = os.path.basename(json_file).replace('bond_data_', '').replace('.json', '')
    print(f"\n{'='*60}")
    print(f"ファイル: {json_file}")
    print(f"年: {year}")
    print(f"{'='*60}")

    # JSONファイルを読み込み
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    total_count = len(data)
    print(f"総レコード数: {total_count:,}")

    if total_count == 0:
        print("⚠️  データが空です。スキップします。")
        return 0

    # データからカラム名を抽出
    json_keys = set(data[0].keys())
    columns_to_insert = [col for col in table_columns if col in json_keys]
    print(f"インポート対象カラム: {len(columns_to_insert)}/{len(table_columns)}")

    # INSERT文を構築
    placeholders = ', '.join(['%s'] * len(columns_to_insert))
    insert_query = sql.SQL(
        "INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON CONFLICT (trade_date, bond_code) DO NOTHING"
    ).format(
        table=sql.Identifier('bond_data'),
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
            print(f"\n⚠️  バッチ挿入エラー: {e}")
            # 個別に挿入を試みる
            for value in values:
                try:
                    cursor.execute(insert_query, value)
                    inserted_count += 1
                except Exception as e2:
                    # 重複キーエラーは無視
                    if "duplicate key" not in str(e2).lower():
                        print(f"\n⚠️  レコード挿入エラー (スキップ): {e2}")

    print(f"\n✅ インポート完了: {inserted_count:,} レコード")
    return inserted_count


def main():
    """メイン処理"""
    print("\n" + "="*60)
    print("bond_data Cloud SQL インポート")
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

        # テーブルのカラム一覧を取得
        table_columns = get_table_columns(cursor, 'bond_data')
        print(f"\nbond_dataテーブル カラム数: {len(table_columns)}")

        # 年別JSONファイルを検索
        json_files = sorted(glob.glob(f"{EXPORT_DIR}/bond_data_*.json"))
        print(f"検出ファイル数: {len(json_files)}")

        if not json_files:
            print(f"\n⚠️  {EXPORT_DIR}/ にJSONファイルが見つかりません")
            return False

        # ファイルごとにインポート
        total_imported = 0
        import_summary = {}

        for json_file in json_files:
            year = os.path.basename(json_file).replace('bond_data_', '').replace('.json', '')

            try:
                count = import_json_file(cursor, json_file, table_columns)
                conn.commit()
                import_summary[year] = {'status': 'success', 'count': count}
                total_imported += count
            except Exception as e:
                print(f"\n❌ {year}年のインポート中にエラー: {e}")
                conn.rollback()
                import_summary[year] = {'status': 'error', 'error': str(e)}
                # エラーがあっても続行

        # サマリー表示
        print("\n" + "="*60)
        print("インポート完了サマリー")
        print("="*60)
        for year, info in sorted(import_summary.items()):
            status_icon = '✅' if info['status'] == 'success' else '❌'
            count = info.get('count', 0)
            print(f"{status_icon} {year}年: {count:,} レコード")

        print(f"\n総インポート数: {total_imported:,} レコード")

        # 最終確認
        cursor.execute("SELECT COUNT(*) FROM bond_data")
        final_count = cursor.fetchone()[0]
        print(f"\nbond_dataテーブル 総レコード数: {final_count:,}")

        cursor.close()
        conn.close()

        print("\n✅ すべてのインポート完了！")
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
