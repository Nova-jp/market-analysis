#!/usr/bin/env python3
"""
日銀保有国債 銘柄別残高データ 定期更新スクリプト
データソース: https://www.boj.or.jp/statistics/boj/other/mei/index.htm

更新頻度: 毎日実行推奨（新規データがなければスキップ）
公表日: 月3回（10日、20日、月末営業日の翌営業日）

使用方法:
    # 最新データのみ取得（デフォルト）
    python scripts/update_boj_holdings.py

    # 直近N件のファイルをチェック
    python scripts/update_boj_holdings.py --count 3

    # テストモード（DBに保存しない）
    python scripts/update_boj_holdings.py --test

    # Cloud Scheduler / cron用（非対話モード、JSON出力）
    python scripts/update_boj_holdings.py --non-interactive

    # 静かモード（エラー時のみ出力）
    python scripts/update_boj_holdings.py --quiet
"""

import argparse
import sys
import os
import json
import importlib.util
from datetime import datetime

# プロジェクトルートをパスに追加
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


def load_collector():
    """コレクターを直接ロード（__init__.pyを経由しない）"""
    spec = importlib.util.spec_from_file_location(
        "holdings_collector",
        os.path.join(PROJECT_ROOT, "data", "collectors", "boj", "holdings_collector.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.BOJHoldingsCollector


def get_latest_date_in_db():
    """データベース内の最新日付を取得"""
    from dotenv import load_dotenv
    from supabase import create_client

    load_dotenv()
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')
    supabase = create_client(url, key)

    try:
        result = supabase.table('boj_holdings') \
            .select('data_date') \
            .order('data_date', desc=True) \
            .limit(1) \
            .execute()

        if result.data:
            return result.data[0]['data_date']
        return None
    except Exception as e:
        return None


def main():
    parser = argparse.ArgumentParser(
        description='日銀保有国債 銘柄別残高データ 定期更新',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--count', type=int, default=3,
                        help='チェックするファイル数（デフォルト: 3）')
    parser.add_argument('--delay', type=float, default=5.0,
                        help='リクエスト間隔（秒）(デフォルト: 5.0)')
    parser.add_argument('--test', action='store_true',
                        help='テストモード（DBに保存しない）')
    parser.add_argument('--non-interactive', action='store_true',
                        help='非対話モード（確認プロンプトをスキップ）')
    parser.add_argument('--quiet', '-q', action='store_true',
                        help='静かモード（結果のみ出力）')
    parser.add_argument('--json', action='store_true',
                        help='JSON形式で結果を出力')

    args = parser.parse_args()

    current_year = datetime.now().year
    result = {
        'status': 'success',
        'timestamp': datetime.now().isoformat(),
        'latest_db_date': None,
        'new_files': 0,
        'new_records': 0,
        'skipped': 0,
        'message': ''
    }

    try:
        # 現在のDB状態を確認
        latest_db_date = get_latest_date_in_db()
        result['latest_db_date'] = latest_db_date

        if not args.quiet and not args.json:
            print("=" * 60)
            print("日銀保有国債 銘柄別残高データ 定期更新")
            print("=" * 60)
            print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"DB内最新データ: {latest_db_date or 'なし'}")
            print(f"チェック対象: 最新{args.count}件")
            print(f"モード: {'テスト' if args.test else '本番'}")
            print("=" * 60)

        # 確認（非対話モード以外）
        if not args.test and not args.non_interactive and not args.quiet:
            try:
                response = input("\n更新を開始しますか？ (y/N): ")
                if response.lower() != 'y':
                    print("キャンセルしました。")
                    return
            except EOFError:
                pass

        # コレクター初期化
        BOJHoldingsCollector = load_collector()
        collector = BOJHoldingsCollector(delay_seconds=args.delay)

        # 今年のファイルリストを取得
        if not args.quiet and not args.json:
            print(f"\n{current_year}年のファイル一覧を取得中...")

        file_links = collector.get_file_links_for_year(current_year)

        if not file_links:
            result['status'] = 'error'
            result['message'] = 'ファイルリストを取得できませんでした'
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                print("エラー: ファイルリストを取得できませんでした")
            sys.exit(1)

        # 最新N件をチェック
        target_files = file_links[:args.count]

        # 新規データのみ抽出
        new_files = []
        for f in target_files:
            if latest_db_date and f['date_str'] <= latest_db_date:
                result['skipped'] += 1
            else:
                new_files.append(f)

        if not new_files:
            result['message'] = '新規データはありません'
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            elif not args.quiet:
                print("\n新規データはありません。")
            return

        if not args.quiet and not args.json:
            print(f"\n{len(new_files)}件の新規データを取得します:")
            for f in new_files:
                print(f"  - {f['date_str']}")

        # データ取得・保存
        total_records = 0
        for i, file_info in enumerate(new_files):
            if not args.quiet and not args.json:
                print(f"\n処理中 ({i+1}/{len(new_files)}): {file_info['date_str']}")

            records = collector.download_and_parse(file_info)
            if records:
                if not args.test:
                    saved = collector.save_to_database(records)
                    total_records += saved
                    if not args.quiet and not args.json:
                        print(f"  → {saved}件保存")
                else:
                    total_records += len(records)
                    if not args.quiet and not args.json:
                        print(f"  → {len(records)}件（テストモード）")

        result['new_files'] = len(new_files)
        result['new_records'] = total_records
        result['message'] = f'{len(new_files)}ファイル、{total_records}件を保存しました'

        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif not args.quiet:
            print("\n" + "=" * 60)
            print("更新完了")
            print("=" * 60)
            print(f"新規ファイル数: {len(new_files)}")
            print(f"保存レコード数: {total_records}")

    except Exception as e:
        result['status'] = 'error'
        result['message'] = str(e)
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"エラー: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
