#!/usr/bin/env python3
"""
IRSテーブル存在確認スクリプト
"""

import sys
from pathlib import Path
import requests
import os
from dotenv import load_dotenv

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

load_dotenv()


def check_table_exists():
    """irs_settlement_ratesテーブルの存在確認"""

    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')

    if not supabase_url or not supabase_key:
        print("エラー: 環境変数が設定されていません")
        sys.exit(1)

    # Supabase REST APIでテーブルにアクセスを試みる
    url = f"{supabase_url}/rest/v1/irs_settlement_rates"
    headers = {
        'apikey': supabase_key,
        'Authorization': f'Bearer {supabase_key}',
    }

    print("=" * 80)
    print("irs_settlement_rates テーブル確認中...")
    print("=" * 80)

    try:
        # HEADリクエストでテーブルの存在を確認
        response = requests.head(url, headers=headers, params={'limit': 0})

        if response.status_code == 200:
            print("\n✅ テーブルが存在します！")
            print("\nテーブル情報:")

            # GETリクエストでレコード数を確認
            headers_with_count = headers.copy()
            headers_with_count['Prefer'] = 'count=exact'

            response = requests.get(url, headers=headers_with_count, params={'limit': 0})
            if response.status_code == 200:
                count = response.headers.get('Content-Range', '').split('/')[-1]
                print(f"  レコード数: {count}")

            print("\n" + "=" * 80)
            print("データ収集スクリプトを実行できます:")
            print("=" * 80)
            print("\npython scripts/collect_irs_data.py")
            print("または")
            print("python scripts/collect_irs_data.py --date 2025-12-22")
            print()

        elif response.status_code == 404:
            print("\n❌ テーブルが存在しません")
            print("\nテーブルを作成してください:")
            print("=" * 80)
            print("1. Supabase管理画面にアクセス")
            print("2. SQL Editorを開く")
            print("3. 以下のSQLファイルを実行:")
            print("   scripts/sql/schema/create_irs_settlement_rates_table.sql")
            print("=" * 80)
            print()

        else:
            print(f"\n⚠️  予期しないレスポンス: {response.status_code}")
            print(f"詳細: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"\n❌ エラー: {e}")
        sys.exit(1)


if __name__ == "__main__":
    check_table_exists()
