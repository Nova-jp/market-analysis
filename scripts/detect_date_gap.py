#!/usr/bin/env python3
"""
データギャップ検出スクリプト

目的:
- 最近1ヶ月のデータで、1営業日データが飛んでいる箇所を検出
- trade_date修正後に旧方式で追加されたデータの境界を特定

アルゴリズム:
1. 最近1ヶ月のtrade_dateをすべて取得
2. 連続する営業日をチェック
3. 1営業日飛んでいる箇所を報告

使用方法:
    python scripts/detect_date_gap.py
"""

import os
import sys
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 環境変数読み込み
load_dotenv()

# 営業日判定関数を共通モジュールからインポート
from data.utils.date_utils import (
    is_business_day,
    get_next_business_day,
    get_previous_business_day
)


def main():
    logger.info("======================================================================")
    logger.info("データギャップ検出スクリプト")
    logger.info("======================================================================")
    logger.info("")

    # Supabase接続
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')

    if not url or not key:
        logger.error("❌ 環境変数 SUPABASE_URL, SUPABASE_KEY が設定されていません")
        return

    supabase: Client = create_client(url, key)
    logger.info("✅ Supabase接続成功")
    logger.info("")

    # 最近1ヶ月の日付範囲を計算
    today = datetime.now().date()
    one_month_ago = today - timedelta(days=30)

    logger.info(f"📅 検索期間: {one_month_ago} ～ {today}")
    logger.info("")

    # 最近1ヶ月のユニークなtrade_dateを取得
    logger.info("📥 データベースからtrade_date一覧を取得中...")

    try:
        response = supabase.table('bond_data') \
            .select('trade_date') \
            .gte('trade_date', str(one_month_ago)) \
            .lte('trade_date', str(today)) \
            .execute()

        # ユニークな日付リストを作成（ソート）
        unique_dates = sorted(list(set([row['trade_date'] for row in response.data])))

        logger.info(f"  ✅ 取得完了: {len(unique_dates)}日分のデータ")
        logger.info("")

        if not unique_dates:
            logger.warning("⚠️  最近1ヶ月のデータがありません")
            return

        # 日付リストを表示
        logger.info("📋 取得した日付一覧（最新10件）:")
        for date_str in unique_dates[-10:]:
            logger.info(f"  - {date_str}")
        logger.info("")

        # ギャップ検出
        logger.info("🔍 営業日ギャップを検出中...")
        logger.info("-" * 70)

        gaps_found = []

        for i in range(len(unique_dates) - 1):
            current_date_str = unique_dates[i]
            next_date_str = unique_dates[i + 1]

            current_date = datetime.strptime(current_date_str, '%Y-%m-%d').date()
            next_date = datetime.strptime(next_date_str, '%Y-%m-%d').date()

            # 次の営業日を計算
            expected_next = get_next_business_day(current_date)

            # 実際の次の日付と比較
            if next_date != expected_next:
                # ギャップ発見
                missing_date = expected_next
                gap_info = {
                    'before': current_date_str,
                    'missing': str(missing_date),
                    'after': next_date_str,
                    'gap_days': (next_date - current_date).days
                }
                gaps_found.append(gap_info)

                logger.warning(f"⚠️  ギャップ検出!")
                logger.warning(f"    前の日付: {current_date_str}")
                logger.warning(f"    欠落日付: {missing_date} (営業日)")
                logger.warning(f"    次の日付: {next_date_str}")
                logger.warning(f"    日数差: {gap_info['gap_days']}日")
                logger.warning("")

        # 結果サマリー
        logger.info("=" * 70)
        if gaps_found:
            logger.warning(f"⚠️  {len(gaps_found)}箇所のギャップを検出しました")
            logger.info("")
            logger.info("【検出されたギャップ一覧】")
            for i, gap in enumerate(gaps_found, 1):
                logger.info(f"{i}. {gap['before']} → [欠落: {gap['missing']}] → {gap['after']}")

            logger.info("")
            logger.info("=" * 70)
            logger.info("🔧 推奨される対応:")
            logger.info("=" * 70)

            # 最も新しいギャップを特定
            latest_gap = gaps_found[-1]
            logger.info(f"最新のギャップ: {latest_gap['after']} 以降")
            logger.info("")
            logger.info("次のステップ:")
            logger.info(f"  1. {latest_gap['after']} 以降のデータを1営業日前にずらす")
            logger.info(f"  2. または {latest_gap['after']} 以降のデータを削除して再取得")
            logger.info("")
            logger.info(f"修正スクリプト実行例:")
            logger.info(f"  python scripts/fix_recent_trade_date.py --from-date {latest_gap['after']}")
            logger.info("")

        else:
            logger.info("✅ ギャップは検出されませんでした")
            logger.info("  → 最近1ヶ月のデータは正常です")
            logger.info("")

        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"❌ エラー: {e}")
        raise


if __name__ == '__main__':
    main()
