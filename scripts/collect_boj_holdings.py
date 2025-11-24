#!/usr/bin/env python3
"""
日銀保有国債 銘柄別残高データ収集スクリプト
データソース: https://www.boj.or.jp/statistics/boj/other/mei/index.htm

使用方法:
    # 全期間収集（2001年〜現在）
    python scripts/collect_boj_holdings.py

    # 指定年のみ収集
    python scripts/collect_boj_holdings.py --year 2024

    # 期間指定
    python scripts/collect_boj_holdings.py --start-year 2020 --end-year 2024

    # テストモード（DBに保存しない）
    python scripts/collect_boj_holdings.py --test

    # リクエスト間隔変更（秒）
    python scripts/collect_boj_holdings.py --delay 10
"""

import argparse
import sys
import os
from datetime import datetime

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.collectors.boj import BOJHoldingsCollector


def main():
    parser = argparse.ArgumentParser(
        description='日銀保有国債 銘柄別残高データ収集',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--year', type=int, help='特定の年のみ収集')
    parser.add_argument('--start-year', type=int, default=2001, help='開始年 (デフォルト: 2001)')
    parser.add_argument('--end-year', type=int, help='終了年 (デフォルト: 現在の年)')
    parser.add_argument('--delay', type=float, default=5.0,
                        help='リクエスト間隔（秒）。日銀サーバー保護のため5秒以上推奨 (デフォルト: 5.0)')
    parser.add_argument('--test', action='store_true', help='テストモード（DBに保存しない）')
    parser.add_argument('--verbose', '-v', action='store_true', help='詳細ログ出力')

    args = parser.parse_args()

    # 収集期間の設定
    if args.year:
        start_year = args.year
        end_year = args.year
    else:
        start_year = args.start_year
        end_year = args.end_year or datetime.now().year

    print("=" * 60)
    print("日銀保有国債 銘柄別残高データ収集")
    print("=" * 60)
    print(f"収集期間: {start_year}年 〜 {end_year}年")
    print(f"リクエスト間隔: {args.delay}秒")
    print(f"モード: {'テスト（DB保存なし）' if args.test else '本番（DB保存あり）'}")
    print("=" * 60)
    print()

    # 確認
    if not args.test:
        try:
            response = input("収集を開始しますか？ (y/N): ")
            if response.lower() != 'y':
                print("キャンセルしました。")
                return
        except EOFError:
            # 非対話モードの場合はそのまま実行
            pass

    # コレクター初期化
    collector = BOJHoldingsCollector(delay_seconds=args.delay)

    # 収集実行
    print("\n収集を開始します...\n")

    try:
        results = collector.collect_all(
            start_year=start_year,
            end_year=end_year,
            save_to_db=not args.test
        )

        # 結果出力
        print("\n" + "=" * 60)
        print("収集結果")
        print("=" * 60)
        print(f"総ファイル数: {results['total_files']}")
        print(f"総レコード数: {results['total_records']}")
        print()
        print("年別内訳:")
        for year, data in sorted(results['by_year'].items()):
            print(f"  {year}年: {data['files']}ファイル, {data['records']}レコード")

    except KeyboardInterrupt:
        print("\n\n収集が中断されました。")
        sys.exit(1)
    except Exception as e:
        print(f"\nエラーが発生しました: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
