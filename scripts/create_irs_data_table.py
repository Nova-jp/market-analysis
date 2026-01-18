#!/usr/bin/env python3
"""
金利スワップテーブル作成スクリプト
Supabaseにirs_dataテーブルを作成
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


def create_irs_table():
    """Supabaseにirs_dataテーブルを作成"""

    # SQLファイルを読み込む
    sql_file = project_root / "scripts" / "sql" / "schema" / "create_irs_data_table.sql"

    if not sql_file.exists():
        print(f"エラー: SQLファイルが見つかりません: {sql_file}")
        sys.exit(1)

    with open(sql_file, 'r', encoding='utf-8') as f:
        sql = f.read()

    print(f"SQLファイルを読み込みました: {sql_file}")
    print(f"\n実行するSQL:\n{'-'*80}")
    print(sql[:500])
    print(f"{'...' if len(sql) > 500 else ''}\n{'-'*80}\n")

    # Supabase接続情報
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')

    if not supabase_url or not supabase_key:
        print("エラー: SUPABASE_URLまたはSUPABASE_KEYが設定されていません")
        print(".envファイルを確認してください")
        sys.exit(1)

    # Supabase REST APIを使用してSQLを実行
    # Note: Supabaseの postgrest は直接SQLを実行できないため、
    # PostgreSQL接続または管理画面での実行が必要です

    print("=" * 80)
    print("次のステップ:")
    print("=" * 80)
    print("\n1. Supabase管理画面にアクセス:")
    print(f"   {supabase_url.replace('https://', 'https://supabase.com/dashboard/project/')}")
    print("\n2. 左メニューから「SQL Editor」を選択")
    print("\n3. 上記のSQLファイルの内容をコピー&ペースト:")
    print(f"   {sql_file}")
    print("\n4. 「Run」ボタンをクリックしてテーブルを作成")
    print("\n" + "=" * 80)

    # 代替案: psqlを使用してテーブル作成
    print("\n代替案: psqlコマンドでテーブルを作成する場合:")
    print("-" * 80)
    print(f"psql '{supabase_url}/postgres' -c \"$(cat {sql_file})\"")
    print("-" * 80)
    print("\nNote: データベースのパスワードが必要です")
    print()


if __name__ == "__main__":
    create_irs_table()
