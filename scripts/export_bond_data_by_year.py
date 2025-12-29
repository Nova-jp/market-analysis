#!/usr/bin/env python3
"""
bond_dataを年別に分割してエクスポート

Usage:
    python scripts/export_bond_data_by_year.py
"""
import requests
import json
import os
from datetime import datetime
from typing import List, Dict, Any

# Supabase設定
SUPABASE_URL = 'https://yfravzuebsvkzjnabalj.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlmcmF2enVlYnN2a3pqbmFiYWxqIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NzEwNTQ1MCwiZXhwIjoyMDcyNjgxNDUwfQ.0-Qq9JKJ96LxKm5RGCWxZp3c9hs988sQ_0_G2-N9LAA'

HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json'
}

EXPORT_DIR = 'data_exports/bond_data_by_year'
os.makedirs(EXPORT_DIR, exist_ok=True)


def export_year_data(year: int, batch_size: int = 1000) -> int:
    """
    特定年のデータをエクスポート

    Args:
        year: 対象年
        batch_size: バッチサイズ

    Returns:
        エクスポートしたレコード数
    """
    print(f"\n{'='*60}")
    print(f"年: {year}")
    print(f"{'='*60}")

    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    all_data = []
    offset = 0

    while True:
        print(f"取得中... offset={offset:,}", end='\r')

        try:
            response = requests.get(
                f'{SUPABASE_URL}/rest/v1/bond_data',
                params={
                    'select': '*',
                    'trade_date': f'gte.{start_date}',
                    'trade_date': f'lte.{end_date}',
                    'order': 'trade_date.asc',
                    'offset': offset,
                    'limit': batch_size
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

        except requests.exceptions.Timeout:
            print(f"\n⚠️  タイムアウト発生 (offset={offset}). 5秒待機後リトライ...")
            import time
            time.sleep(5)
            continue
        except Exception as e:
            print(f"\n❌ エラー: {e}")
            break

    total_count = len(all_data)
    print(f"\n✅ 取得完了: {total_count:,} レコード")

    if total_count == 0:
        return 0

    # JSONファイルに保存
    filename = f"{EXPORT_DIR}/bond_data_{year}.json"

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2, default=str)

    file_size_mb = os.path.getsize(filename) / (1024 * 1024)
    print(f"💾 保存完了: {filename} ({file_size_mb:.2f} MB)")

    return total_count


def main():
    """メイン処理"""
    print("\n" + "="*60)
    print("bond_data 年別エクスポート")
    print("="*60)
    print(f"エクスポート先: {EXPORT_DIR}/")

    # データ範囲を確認
    print("\nデータ範囲確認中...")

    # 最古の年を取得
    resp = requests.get(
        f'{SUPABASE_URL}/rest/v1/bond_data',
        params={'select': 'trade_date', 'order': 'trade_date.asc', 'limit': 1},
        headers=HEADERS
    )

    if resp.status_code == 200 and resp.json():
        min_date = resp.json()[0]['trade_date']
        min_year = int(min_date.split('-')[0])
        print(f"最古日付: {min_date} (年: {min_year})")
    else:
        min_year = 2002
        print(f"最古日付取得失敗。デフォルト年: {min_year}")

    # 最新の年を取得
    resp = requests.get(
        f'{SUPABASE_URL}/rest/v1/bond_data',
        params={'select': 'trade_date', 'order': 'trade_date.desc', 'limit': 1},
        headers=HEADERS
    )

    if resp.status_code == 200 and resp.json():
        max_date = resp.json()[0]['trade_date']
        max_year = int(max_date.split('-')[0])
        print(f"最新日付: {max_date} (年: {max_year})")
    else:
        max_year = 2025
        print(f"最新日付取得失敗。デフォルト年: {max_year}")

    # 年ごとにエクスポート
    total_records = 0
    export_summary = {}

    for year in range(min_year, max_year + 1):
        try:
            count = export_year_data(year)
            export_summary[year] = {'status': 'success', 'count': count}
            total_records += count
        except Exception as e:
            print(f"\n❌ {year}年のエクスポート中にエラー: {e}")
            export_summary[year] = {'status': 'error', 'error': str(e)}

    # サマリー表示
    print("\n" + "="*60)
    print("エクスポート完了サマリー")
    print("="*60)
    for year, info in export_summary.items():
        status_icon = '✅' if info['status'] == 'success' else '❌'
        count = info.get('count', 0)
        print(f"{status_icon} {year}年: {count:,} レコード")

    print(f"\n総エクスポート数: {total_records:,} レコード")
    print(f"\n次のステップ:")
    print(f"python scripts/import_bond_data_to_cloudsql.py")


if __name__ == '__main__':
    main()
