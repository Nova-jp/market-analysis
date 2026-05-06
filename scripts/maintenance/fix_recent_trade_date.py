#!/usr/bin/env python3
"""
最近のtrade_date修正スクリプト

目的:
- 指定日以降のtrade_dateを1営業日前にずらす
- trade_date修正後に旧方式で追加されたデータを修正

アルゴリズム:
1. 指定日以降のユニークなtrade_dateを取得
2. 各日付について、1営業日前の日付を計算
3. 該当するすべてのレコードを更新

使用方法:
    python scripts/fix_recent_trade_date.py --from-date 2025-12-05
"""

import os
import sys
import argparse
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/fix_recent_trade_date.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 環境変数読み込み
load_dotenv()

# 営業日判定関数を共通モジュールからインポート
from core.utils.date_utils import (
    is_business_day,
    get_previous_business_day
)


def main():
    parser = argparse.ArgumentParser(description='最近のtrade_dateを1営業日前に修正')
    parser.add_argument('--from-date', required=True, help='修正開始日 (YYYY-MM-DD)')
    parser.add_argument('--dry-run', action='store_true', help='実行せずに確認のみ')

    args = parser.parse_args()

    from_date_str = args.from_date
    dry_run = args.dry_run

    logger.info("======================================================================")
    logger.info("trade_date 修正スクリプト（最近のデータ）")
    logger.info("======================================================================")
    logger.info(f"  修正開始日: {from_date_str} 以降")
    logger.info(f"  モード: {'DRY RUN（確認のみ）' if dry_run else '本番実行'}")
    logger.info("======================================================================")
    logger.info("")

    # 日付形式チェック
    try:
        from_date = datetime.strptime(from_date_str, '%Y-%m-%d').date()
    except ValueError:
        logger.error("❌ 日付形式が不正です。YYYY-MM-DD形式で指定してください")
        return

    # Supabase接続
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')

    if not url or not key:
        logger.error("❌ 環境変数 SUPABASE_URL, SUPABASE_KEY が設定されていません")
        return

    supabase: Client = create_client(url, key)
    logger.info("✅ Supabase接続成功")
    logger.info("")

    # 対象日付を取得
    logger.info(f"📥 {from_date_str} 以降のtrade_date一覧を取得中...")

    try:
        response = supabase.table('bond_data') \
            .select('trade_date') \
            .gte('trade_date', from_date_str) \
            .execute()

        # ユニークな日付リストを作成（ソート）
        unique_dates = sorted(list(set([row['trade_date'] for row in response.data])))

        logger.info(f"  ✅ 取得完了: {len(unique_dates)}日分")
        logger.info("")

        if not unique_dates:
            logger.warning("⚠️  対象データがありません")
            return

        # 日付マッピングを作成
        logger.info("🔧 日付マッピングを作成中...")
        logger.info("-" * 70)

        date_mappings = []

        for date_str in unique_dates:
            current_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            previous_business_day = get_previous_business_day(current_date)

            mapping = {
                'old_date': date_str,
                'new_date': str(previous_business_day)
            }
            date_mappings.append(mapping)

            logger.info(f"  {date_str} → {previous_business_day}")

        logger.info("")
        logger.info(f"  ✅ マッピング作成完了: {len(date_mappings)}日分")
        logger.info("")

        # 確認
        if dry_run:
            logger.info("=" * 70)
            logger.info("🔍 DRY RUN モード - 実行内容の確認")
            logger.info("=" * 70)
            logger.info(f"  修正対象日数: {len(date_mappings)}日")
            logger.info(f"  最初の日付: {date_mappings[0]['old_date']} → {date_mappings[0]['new_date']}")
            logger.info(f"  最後の日付: {date_mappings[-1]['old_date']} → {date_mappings[-1]['new_date']}")
            logger.info("")
            logger.info("本番実行する場合は --dry-run オプションを外してください")
            logger.info("=" * 70)
            return

        # 実行確認
        logger.info("=" * 70)
        logger.warning("⚠️  以下の処理を実行します:")
        logger.info("=" * 70)
        logger.info(f"  修正対象日数: {len(date_mappings)}日")
        logger.info(f"  最初の日付: {date_mappings[0]['old_date']} → {date_mappings[0]['new_date']}")
        logger.info(f"  最後の日付: {date_mappings[-1]['old_date']} → {date_mappings[-1]['new_date']}")
        logger.info("")

        response = input("実行してよろしいですか? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            logger.info("処理を中断しました")
            return

        logger.info("")

        # 各日付を更新
        logger.info("🔄 trade_date更新中...")
        logger.info("-" * 70)

        total_updated = 0

        for i, mapping in enumerate(date_mappings, 1):
            old_date = mapping['old_date']
            new_date = mapping['new_date']

            try:
                # 該当日付のレコード数を確認
                count_response = supabase.table('bond_data') \
                    .select('*', count='exact') \
                    .eq('trade_date', old_date) \
                    .limit(1) \
                    .execute()

                record_count = count_response.count

                # 更新
                update_response = supabase.table('bond_data') \
                    .update({'trade_date': new_date}) \
                    .eq('trade_date', old_date) \
                    .execute()

                total_updated += record_count

                logger.info(f"  [{i}/{len(date_mappings)}] {old_date} → {new_date}: {record_count:,}件更新")

            except Exception as e:
                logger.error(f"  ❌ エラー: {old_date} - {e}")
                continue

        logger.info("")
        logger.info("=" * 70)
        logger.info("✅ 処理完了！")
        logger.info("=" * 70)
        logger.info(f"  更新日数: {len(date_mappings)}日")
        logger.info(f"  更新レコード数: {total_updated:,}件")
        logger.info("=" * 70)
        logger.info("")

        # 検証
        logger.info("🔍 検証中...")
        logger.info(f"  {from_date_str} 以降のデータが残っていないか確認...")

        verify_response = supabase.table('bond_data') \
            .select('*', count='exact') \
            .gte('trade_date', from_date_str) \
            .limit(1) \
            .execute()

        remaining_count = verify_response.count

        if remaining_count > 0:
            logger.warning(f"  ⚠️  {from_date_str} 以降のデータが {remaining_count}件残っています")
            logger.warning(f"  これは重複または新しいデータの可能性があります")
        else:
            logger.info(f"  ✅ {from_date_str} 以降のデータなし（正常）")

        logger.info("")

    except Exception as e:
        logger.error(f"❌ エラー: {e}")
        raise


if __name__ == '__main__':
    main()
