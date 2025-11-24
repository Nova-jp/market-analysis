#!/usr/bin/env python3
"""
日銀保有国債 銘柄別残高データ収集スクリプト（ローカル実行用）
進捗バー付きで見やすく表示

使用方法:
    # 全期間収集（2001年〜現在）- 既存データはスキップ
    python3 scripts/collect_boj_holdings_local.py

    # 指定年のみ収集
    python3 scripts/collect_boj_holdings_local.py --year 2024

    # 期間指定
    python3 scripts/collect_boj_holdings_local.py --start-year 2020 --end-year 2024

    # 強制再収集（既存データも上書き）
    python3 scripts/collect_boj_holdings_local.py --force

    # テストモード（DBに保存しない）
    python3 scripts/collect_boj_holdings_local.py --test
"""

import argparse
import sys
import os
from datetime import datetime
import time

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()


def get_existing_dates():
    """DBに既存のデータ日付を取得"""
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')
    supabase = create_client(url, key)

    try:
        result = supabase.table('boj_holdings').select('data_date').execute()
        return set(r['data_date'] for r in result.data)
    except Exception as e:
        print(f"DB確認エラー: {e}")
        return set()


def print_progress_bar(current, total, prefix='', suffix='', length=40):
    """プログレスバーを表示"""
    percent = current / total * 100 if total > 0 else 0
    filled = int(length * current // total) if total > 0 else 0
    bar = '█' * filled + '░' * (length - filled)
    print(f'\r{prefix} |{bar}| {percent:5.1f}% {suffix}', end='', flush=True)


def print_summary_table(results):
    """結果サマリーをテーブル形式で表示"""
    print("\n" + "=" * 60)
    print("収集結果サマリー")
    print("=" * 60)
    print(f"{'年':<8} {'ファイル数':<12} {'レコード数':<12} {'スキップ':<10}")
    print("-" * 60)

    total_files = 0
    total_records = 0
    total_skipped = 0

    for year in sorted(results.keys()):
        data = results[year]
        files = data.get('files', 0)
        records = data.get('records', 0)
        skipped = data.get('skipped', 0)
        total_files += files
        total_records += records
        total_skipped += skipped
        print(f"{year}年    {files:<12} {records:<12} {skipped:<10}")

    print("-" * 60)
    print(f"{'合計':<8} {total_files:<12} {total_records:<12} {total_skipped:<10}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description='日銀保有国債 銘柄別残高データ収集（ローカル実行用）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--year', type=int, help='特定の年のみ収集')
    parser.add_argument('--start-year', type=int, default=2001, help='開始年 (デフォルト: 2001)')
    parser.add_argument('--end-year', type=int, help='終了年 (デフォルト: 現在の年)')
    parser.add_argument('--delay', type=float, default=5.0,
                        help='リクエスト間隔（秒）。日銀サーバー保護のため5秒以上推奨 (デフォルト: 5.0)')
    parser.add_argument('--test', action='store_true', help='テストモード（DBに保存しない）')
    parser.add_argument('--force', action='store_true', help='既存データも再収集')

    args = parser.parse_args()

    # 収集期間の設定
    if args.year:
        start_year = args.year
        end_year = args.year
    else:
        start_year = args.start_year
        end_year = args.end_year or datetime.now().year

    years_to_collect = list(range(start_year, end_year + 1))
    total_years = len(years_to_collect)

    # ヘッダー表示
    print()
    print("╔" + "═" * 58 + "╗")
    print("║" + " 日銀保有国債 銘柄別残高データ収集".center(56) + "║")
    print("╠" + "═" * 58 + "╣")
    print(f"║  収集期間    : {start_year}年 〜 {end_year}年 ({total_years}年分)".ljust(57) + "║")
    print(f"║  リクエスト間隔: {args.delay}秒".ljust(57) + "║")
    mode = 'テスト（DB保存なし）' if args.test else '本番（DB保存あり）'
    print(f"║  モード      : {mode}".ljust(57) + "║")
    skip_mode = '強制再収集' if args.force else '既存データはスキップ'
    print(f"║  スキップ    : {skip_mode}".ljust(57) + "║")
    print("╚" + "═" * 58 + "╝")
    print()

    # 既存データの確認
    existing_dates = set() if args.force else get_existing_dates()
    if existing_dates:
        print(f"DB内既存データ: {len(existing_dates)}日分")

    # コレクター初期化（直接インポートで__init__.pyを経由しない）
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "holdings_collector",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "data", "collectors", "boj", "holdings_collector.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    BOJHoldingsCollector = module.BOJHoldingsCollector
    collector = BOJHoldingsCollector(delay_seconds=args.delay)

    # 結果格納
    results = {}
    start_time = time.time()

    # 年ごとに処理
    for year_idx, year in enumerate(years_to_collect):
        year_start_time = time.time()

        print(f"\n{'─' * 60}")
        print(f"  {year}年 処理中... ({year_idx + 1}/{total_years})")
        print(f"{'─' * 60}")

        # ファイルリスト取得
        file_links = collector.get_file_links_for_year(year)
        if not file_links:
            print(f"  ⚠ ファイルが見つかりません")
            results[year] = {'files': 0, 'records': 0, 'skipped': 0}
            continue

        total_files = len(file_links)
        year_records = 0
        year_skipped = 0
        processed_files = 0

        for i, file_info in enumerate(file_links):
            data_date = file_info['date_str']

            # 既存データのスキップ判定
            if data_date in existing_dates and not args.force:
                year_skipped += 1
                processed_files += 1
                print_progress_bar(
                    processed_files, total_files,
                    prefix=f'  進捗',
                    suffix=f'{processed_files}/{total_files} (スキップ: {data_date})'
                )
                continue

            # データ取得
            records = collector.download_and_parse(file_info)

            if records:
                if not args.test:
                    saved = collector.save_to_database(records)
                    year_records += saved
                else:
                    year_records += len(records)

            processed_files += 1
            print_progress_bar(
                processed_files, total_files,
                prefix=f'  進捗',
                suffix=f'{processed_files}/{total_files} ({data_date}: {len(records) if records else 0}件)'
            )

            # リクエスト間隔
            if i < len(file_links) - 1 and data_date not in existing_dates:
                time.sleep(args.delay)

        # 改行（プログレスバー終了）
        print()

        year_elapsed = time.time() - year_start_time
        print(f"  ✓ 完了: {processed_files - year_skipped}ファイル処理, {year_records}件保存, {year_skipped}件スキップ ({year_elapsed:.1f}秒)")

        results[year] = {
            'files': processed_files - year_skipped,
            'records': year_records,
            'skipped': year_skipped
        }

    # 総経過時間
    total_elapsed = time.time() - start_time

    # サマリー表示
    print_summary_table(results)

    # 経過時間
    hours, remainder = divmod(int(total_elapsed), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        elapsed_str = f"{hours}時間{minutes}分{seconds}秒"
    elif minutes > 0:
        elapsed_str = f"{minutes}分{seconds}秒"
    else:
        elapsed_str = f"{seconds}秒"

    print(f"\n総経過時間: {elapsed_str}")
    print()


if __name__ == '__main__':
    main()
