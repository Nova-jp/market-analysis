#!/usr/bin/env python3
"""Supabaseデータベースをpg_dumpでエクスポート

pg_dumpを使用してSupabase PostgreSQLデータベースからデータをエクスポートします。

Usage:
    python scripts/dump_supabase_database.py
"""
import os
import sys
import subprocess
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def dump_database(output_file=None, tables=None):
    """pg_dumpでエクスポート

    Args:
        output_file: 出力ファイルパス（指定しない場合は自動生成）
        tables: エクスポートするテーブルのリスト（Noneの場合は全テーブル）

    Returns:
        bool: 成功時True、失敗時False
    """

    # 環境変数取得
    host = os.getenv('SUPABASE_DB_HOST')
    port = os.getenv('SUPABASE_DB_PORT', '5432')
    database = os.getenv('SUPABASE_DB_NAME', 'postgres')
    user = os.getenv('SUPABASE_DB_USER')
    password = os.getenv('SUPABASE_DB_PASSWORD')

    # 環境変数チェック
    if not all([host, user, password]):
        print("❌ 必須環境変数が設定されていません:")
        if not host:
            print("  - SUPABASE_DB_HOST")
        if not user:
            print("  - SUPABASE_DB_USER")
        if not password:
            print("  - SUPABASE_DB_PASSWORD")
        return False

    # 出力ファイル名
    if not output_file:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'data_exports/database_dumps/supabase_{timestamp}.dump'

    # ディレクトリ作成
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    print("="*60)
    print("Supabase データベース dump")
    print("="*60)
    print(f"接続先: {host}:{port}/{database}")
    print(f"出力先: {output_file}")

    if tables:
        print(f"テーブル: {', '.join(tables)}")
    else:
        print("テーブル: 全テーブル")

    # pg_dumpコマンド構築
    cmd = [
        'pg_dump',
        f'--host={host}',
        f'--port={port}',
        f'--username={user}',
        f'--dbname={database}',
        '--format=custom',  # 圧縮形式（pg_restore用）
        '--verbose',
        '--no-owner',       # オーナー情報除外
        '--no-acl',         # 権限情報除外
        f'--file={output_file}'
    ]

    # 特定テーブルのみエクスポート
    if tables:
        for table in tables:
            cmd.append(f'--table={table}')

    # 環境変数でパスワード設定
    env = os.environ.copy()
    env['PGPASSWORD'] = password

    try:
        print("\npg_dump実行中...")
        print("（このプロセスには数分かかる場合があります）")

        result = subprocess.run(
            cmd,
            env=env,
            check=True,
            capture_output=True,
            text=True
        )

        # ファイルサイズ確認
        if os.path.exists(output_file):
            file_size = os.path.getsize(output_file)
            print(f"\n✅ dump完了")
            print(f"ファイルサイズ: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
            print(f"出力ファイル: {output_file}")
            return True
        else:
            print("\n❌ dumpファイルが作成されませんでした")
            return False

    except subprocess.CalledProcessError as e:
        print(f"\n❌ pg_dump失敗: {e}")
        if e.stderr:
            print(f"エラー詳細: {e.stderr}")
        return False

    except FileNotFoundError:
        print("\n❌ pg_dumpコマンドが見つかりません")
        print("PostgreSQLクライアントツールをインストールしてください:")
        print("  macOS: brew install postgresql")
        print("  確認: pg_dump --version")
        return False

    except Exception as e:
        print(f"\n❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """メイン処理"""

    # 移行対象テーブル
    tables = ['bond_data', 'boj_holdings', 'irs_settlement_rates', 'bond_auction']

    print("\n" + "="*60)
    print("Supabase データベース エクスポート")
    print("="*60)
    print(f"開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    success = dump_database(tables=tables)

    print(f"完了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
