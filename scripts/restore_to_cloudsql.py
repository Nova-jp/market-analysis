#!/usr/bin/env python3
"""Cloud SQLにpg_restoreでインポート

pg_restoreを使用してダンプファイルをCloud SQLにインポートします。

Usage:
    python scripts/restore_to_cloudsql.py
"""
import os
import sys
import subprocess
import glob
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def restore_database(dump_file=None, clean=True, jobs=4):
    """pg_restoreでインポート

    Args:
        dump_file: インポートするdumpファイルパス（Noneの場合は最新ファイル使用）
        clean: True=既存データを削除してから復元
        jobs: 並列ジョブ数（高速化）

    Returns:
        bool: 成功時True、失敗時False
    """

    # 環境変数取得
    host = os.getenv('CLOUD_SQL_HOST')
    port = os.getenv('CLOUD_SQL_PORT', '5432')
    database = os.getenv('CLOUD_SQL_DATABASE')
    user = os.getenv('CLOUD_SQL_USER')
    password = os.getenv('CLOUD_SQL_PASSWORD')

    # 環境変数チェック
    if not all([host, database, user, password]):
        print("❌ 必須環境変数が設定されていません:")
        if not host:
            print("  - CLOUD_SQL_HOST")
        if not database:
            print("  - CLOUD_SQL_DATABASE")
        if not user:
            print("  - CLOUD_SQL_USER")
        if not password:
            print("  - CLOUD_SQL_PASSWORD")
        return False

    # dumpファイル指定がなければ最新を使用
    if not dump_file:
        dumps = glob.glob('data_exports/database_dumps/supabase_*.dump')
        if not dumps:
            print("❌ dumpファイルが見つかりません")
            print("先にdump_supabase_database.pyを実行してください")
            return False
        dump_file = max(dumps)  # 最新ファイル
        print(f"最新のdumpファイルを使用: {dump_file}")

    # ファイル存在確認
    if not os.path.exists(dump_file):
        print(f"❌ dumpファイルが存在しません: {dump_file}")
        return False

    print("="*60)
    print("Cloud SQL データベース restore")
    print("="*60)
    print(f"接続先: {host}:{port}/{database}")
    print(f"入力ファイル: {dump_file}")
    print(f"クリーンモード: {'有効' if clean else '無効'}")
    print(f"並列ジョブ数: {jobs}")

    # pg_restoreコマンド構築
    cmd = [
        'pg_restore',
        f'--host={host}',
        f'--port={port}',
        f'--username={user}',
        f'--dbname={database}',
        '--verbose',
        '--no-owner',
        '--no-acl',
        f'--jobs={jobs}',  # 並列処理で高速化
    ]

    if clean:
        cmd.append('--clean')  # 既存オブジェクトを削除

    cmd.append(dump_file)

    # 環境変数でパスワード設定
    env = os.environ.copy()
    env['PGPASSWORD'] = password

    try:
        print("\npg_restore実行中...")
        print("（このプロセスには数分かかる場合があります）")

        result = subprocess.run(
            cmd,
            env=env,
            check=True,
            capture_output=True,
            text=True
        )

        print("\n✅ restore完了")
        return True

    except subprocess.CalledProcessError as e:
        # pg_restoreは一部のエラーでもreturn code != 0になることがあるため、
        # エラー内容を確認
        if e.stderr:
            # 致命的なエラーかどうかチェック
            if 'FATAL' in e.stderr or 'could not connect' in e.stderr:
                print(f"\n❌ pg_restore失敗（致命的エラー）: {e}")
                print(f"エラー詳細: {e.stderr}")
                return False
            else:
                # 警告レベルのエラーは無視（既存データがない場合のDROPエラーなど）
                print(f"\n⚠️  pg_restore完了（警告あり）")
                if '--clean' in cmd:
                    print("（既存データがない場合のDROPエラーは正常です）")
                return True
        else:
            print(f"\n❌ pg_restore失敗: {e}")
            return False

    except FileNotFoundError:
        print("\n❌ pg_restoreコマンドが見つかりません")
        print("PostgreSQLクライアントツールをインストールしてください:")
        print("  macOS: brew install postgresql")
        print("  確認: pg_restore --version")
        return False

    except Exception as e:
        print(f"\n❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """メイン処理"""

    print("\n" + "="*60)
    print("Cloud SQL データベース インポート")
    print("="*60)
    print(f"開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    success = restore_database(clean=True, jobs=4)

    print(f"完了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
