#!/usr/bin/env python3
"""
Supabaseからデータをエクスポートしてファイルに保存

Usage:
    python scripts/export_supabase_data.py
"""
import os
import json
import requests
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

# Supabase設定
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json'
}

# エクスポート対象テーブル
TABLES = ['bond_data', 'boj_holdings', 'irs_data', 'bond_auction']

# エクスポートディレクトリ
EXPORT_DIR = 'data_exports'
os.makedirs(EXPORT_DIR, exist_ok=True)


def get_total_count(table_name: str) -> int:
    """テーブルの総レコード数を取得"""
    headers = HEADERS.copy()
    headers['Prefer'] = 'count=exact'

    response = requests.head(
        f'{SUPABASE_URL}/rest/v1/{table_name}',
        headers=headers,
        timeout=30
    )

    if 'content-range' in response.headers:
        content_range = response.headers['content-range']
        parts = content_range.split('/')
        if len(parts) == 2 and parts[1] != '*':
            return int(parts[1])

    return 0


def export_table_data(table_name: str, batch_size: int = 1000) -> str:
    """
    テーブルデータをバッチでエクスポート

    Args:
        table_name: テーブル名
        batch_size: 1回のクエリで取得するレコード数

    Returns:
        エクスポートファイルパス
    """
    print(f"\n{'='*60}")
    print(f"テーブル: {table_name}")
    print(f"{'='*60}")

    # 総レコード数を取得
    total_count = get_total_count(table_name)
    print(f"総レコード数: {total_count:,}")

    if total_count == 0:
        print(f"⚠️  {table_name} にデータがありません。スキップします。")
        return None

    all_data = []
    offset = 0

    while offset < total_count:
        print(f"取得中... {offset:,} / {total_count:,} ({(offset/total_count*100):.1f}%)", end='\r')

        response = requests.get(
            f'{SUPABASE_URL}/rest/v1/{table_name}',
            params={
                'select': '*',
                'offset': offset,
                'limit': batch_size,
                'order': 'created_at.asc' if table_name != 'bond_data' else 'trade_date.asc'
            },
            headers=HEADERS,
            timeout=60
        )

        if response.status_code != 200:
            print(f"\n❌ エラー: {response.status_code} - {response.text}")
            break

        batch_data = response.json()
        if not batch_data:
            break

        all_data.extend(batch_data)
        offset += batch_size

        # 最後のバッチの場合は終了
        if len(batch_data) < batch_size:
            break

    print(f"\n✅ 取得完了: {len(all_data):,} レコード")

    # JSONファイルに保存
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{EXPORT_DIR}/{table_name}_{timestamp}.json"

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2, default=str)

    file_size_mb = os.path.getsize(filename) / (1024 * 1024)
    print(f"💾 保存完了: {filename} ({file_size_mb:.2f} MB)")

    return filename


def main():
    """メイン処理"""
    print("\n" + "="*60)
    print("Supabase データエクスポート")
    print("="*60)
    print(f"エクスポート先: {EXPORT_DIR}/")
    print(f"対象テーブル: {', '.join(TABLES)}")

    export_summary = {}

    for table_name in TABLES:
        try:
            filename = export_table_data(table_name)
            export_summary[table_name] = {
                'status': 'success' if filename else 'empty',
                'file': filename
            }
        except Exception as e:
            print(f"\n❌ {table_name} のエクスポート中にエラー: {e}")
            export_summary[table_name] = {
                'status': 'error',
                'error': str(e)
            }

    # サマリー表示
    print("\n" + "="*60)
    print("エクスポート完了サマリー")
    print("="*60)
    for table, info in export_summary.items():
        status_icon = {
            'success': '✅',
            'empty': '⚠️ ',
            'error': '❌'
        }.get(info['status'], '?')
        print(f"{status_icon} {table}: {info['status']}")

    print("\n次のステップ:")
    print("1. Cloud SQL Proxyを起動")
    print("2. psqlでスキーマを作成")
    print("3. scripts/import_to_cloudsql.py でデータをインポート")


if __name__ == '__main__':
    main()
