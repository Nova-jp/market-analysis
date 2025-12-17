#!/usr/bin/env python3
"""
bond_dataテーブルのtrade_date 1営業日ずれ修正スクリプト（シンプル版）

戦略:
1. DBから全ユニーク trade_date を取得
2. 古い順（昇順）でソート
3. 各日付を1営業日前に更新
   - 最古の日付も含めて全て更新
   - jpholidayで営業日計算
   - 古い順に更新するのでユニーク制約違反なし

処理時間: 10-20分（1,200日分）
"""

import sys
import os
from datetime import date, timedelta
import logging
import jpholiday

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from supabase import create_client
from data.utils.database_manager import DatabaseManager

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_previous_business_day(d: date) -> date:
    """
    1営業日前を取得（土日・祝日スキップ）

    Args:
        d: 基準日

    Returns:
        1営業日前の日付
    """
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5 or jpholiday.is_holiday(prev):
        prev -= timedelta(days=1)
    return prev


def main():
    from datetime import datetime
    start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("trade_date 1営業日ずれ修正（シンプル版） - 開始")
    logger.info("=" * 70)
    logger.info("")

    # Supabase接続
    supabase = create_client(
        os.getenv('SUPABASE_URL'),
        os.getenv('SUPABASE_KEY')
    )

    try:
        # Step 1: 全ユニーク trade_date を取得
        logger.info("=" * 70)
        logger.info("Step 1: 全ユニークtrade_dateを取得中...")
        logger.info("=" * 70)

        db = DatabaseManager()
        existing_dates = db.get_all_existing_dates()

        logger.info(f"  ✅ 取得完了: {len(existing_dates)}件のユニーク日付")
        logger.info("")

        # Step 2: 古い順にソート
        logger.info("=" * 70)
        logger.info("Step 2: 古い順にソート...")
        logger.info("=" * 70)

        sorted_dates = sorted(existing_dates)  # 昇順（古い→新しい）

        logger.info(f"  最古の日付: {sorted_dates[0]}")
        logger.info(f"  最新の日付: {sorted_dates[-1]}")
        logger.info("")

        # Step 3: 各日付を1営業日前に更新
        logger.info("=" * 70)
        logger.info("Step 3: trade_date 更新中（古い順）...")
        logger.info("=" * 70)
        logger.info("")

        total_updated = 0
        errors = 0

        for i, old_date_str in enumerate(sorted_dates):
            old_date = date.fromisoformat(old_date_str)
            new_date = get_previous_business_day(old_date)

            try:
                # Supabase UPDATE
                response = supabase.table('bond_data') \
                    .update({
                        'trade_date': new_date.isoformat(),
                        'updated_at': 'now()'
                    }) \
                    .eq('trade_date', old_date_str) \
                    .execute()

                total_updated += 1

                # 進捗表示
                if (i + 1) % 50 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    speed = total_updated / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"  進捗: {i + 1}/{len(sorted_dates)} 日付更新済み "
                        f"({speed:.1f}日/秒, エラー: {errors}件)"
                    )

            except Exception as e:
                errors += 1
                logger.error(f"  ❌ 日付 {old_date_str} 更新エラー: {e}")
                # エラーがあっても続行（デバッグ用）

        # 完了
        elapsed = (datetime.now() - start_time).total_seconds()

        logger.info("")
        logger.info("=" * 70)
        logger.info("✅ 処理完了")
        logger.info("=" * 70)
        logger.info(f"処理日付数: {total_updated}日分")
        logger.info(f"エラー件数: {errors}件")
        logger.info(f"処理時間: {elapsed/60:.1f}分 ({elapsed:.0f}秒)")
        if elapsed > 0:
            logger.info(f"処理速度: {total_updated/elapsed:.1f}日/秒")
        logger.info("=" * 70)
        logger.info("")
        logger.info("次のステップ:")
        logger.info("  1. 検証SQLを実行（土日データがないことを確認）")
        logger.info("     SELECT DATE_PART('dow', trade_date) as dow, COUNT(*)")
        logger.info("     FROM bond_data GROUP BY dow ORDER BY dow;")
        logger.info("")
        logger.info("  2. market_amount再計算を実行")
        logger.info("     python scripts/calculate_market_amount_rpc_optimized.py")
        logger.info("=" * 70)

        return total_updated

    except Exception as e:
        logger.error(f"❌ エラー発生: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == '__main__':
    main()
