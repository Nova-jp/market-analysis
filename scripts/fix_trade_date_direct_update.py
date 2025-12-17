#!/usr/bin/env python3
"""
bond_dataテーブルのtrade_date 直接UPDATE版

前提条件:
- ユニーク制約が削除済み（fix_trade_date_manual.sql Step 1, 2実行済み）

処理内容:
- 全レコードのtrade_dateを直接UPDATE（ユニーク制約を気にせず）
- バッチ処理で効率的に更新

処理時間: 5-10分
"""

import sys
import os
from datetime import date, timedelta
from typing import List, Dict
import logging

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from supabase import create_client
from data.utils.database_manager import DatabaseManager
from data.utils.date_utils import get_previous_business_day

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def build_date_mapping() -> List[Dict[str, str]]:
    """全trade_dateのマッピング作成"""
    logger.info("=" * 70)
    logger.info("Step 1: 全ユニークtrade_dateを取得中...")
    logger.info("=" * 70)

    db = DatabaseManager()
    existing_dates = db.get_all_existing_dates()

    logger.info(f"  取得完了: {len(existing_dates)}件のユニーク日付")

    logger.info("")
    logger.info("=" * 70)
    logger.info("Step 2: 営業日計算中（jpholiday使用）...")
    logger.info("=" * 70)

    mapping = []
    for old_date_str in sorted(existing_dates):
        old_date = date.fromisoformat(old_date_str)
        new_date = get_previous_business_day(old_date)
        mapping.append({
            'old_date': old_date_str,
            'new_date': new_date.isoformat()
        })

    logger.info(f"  ✅ 計算完了: {len(mapping)}件のマッピング作成")

    # サンプル表示
    logger.info("")
    logger.info("=" * 70)
    logger.info("マッピングサンプル（最初の10件）:")
    logger.info("=" * 70)
    for item in mapping[:10]:
        old = date.fromisoformat(item['old_date'])
        new = date.fromisoformat(item['new_date'])
        diff = (old - new).days
        logger.info(f"  {item['old_date']} → {item['new_date']} (差: {diff}日)")

    return mapping


def execute_direct_update(mapping: List[Dict[str, str]]) -> int:
    """
    直接UPDATE（制約削除済み前提）

    Args:
        mapping: 日付マッピング

    Returns:
        総更新件数
    """
    logger.info("")
    logger.info("=" * 70)
    logger.info("Step 3: trade_date 直接UPDATE")
    logger.info("=" * 70)
    logger.info("")
    logger.info("⚠️  重要: ユニーク制約が削除されていることを確認")
    logger.info("   （fix_trade_date_manual.sql Step 1, 2実行済み前提で続行）")
    logger.info("")

    supabase = create_client(
        os.getenv('SUPABASE_URL'),
        os.getenv('SUPABASE_KEY')
    )

    total_updated = 0
    batch_size = 50  # 小さめのバッチで安全に処理
    total_batches = (len(mapping) + batch_size - 1) // batch_size

    logger.info(f"  バッチサイズ: {batch_size}日/バッチ")
    logger.info(f"  総バッチ数: {total_batches}")
    logger.info("")

    for i in range(0, len(mapping), batch_size):
        batch = mapping[i:i + batch_size]
        batch_num = i // batch_size + 1

        # 各日付に対してUPDATE実行
        batch_updated = 0
        for item in batch:
            try:
                # 直接UPDATEクエリを実行
                result = supabase.table('bond_data') \
                    .update({'trade_date': item['new_date'], 'updated_at': 'now()'}) \
                    .eq('trade_date', item['old_date']) \
                    .execute()

                # 更新件数を取得（Supabaseは件数を返さないため推定）
                batch_updated += 1  # 少なくとも1件以上更新されたとみなす

            except Exception as e:
                logger.error(f"  ❌ 日付 {item['old_date']} 更新エラー: {e}")
                continue

        total_updated += batch_updated

        logger.info(
            f"  バッチ {batch_num}/{total_batches}: "
            f"{len(batch)}日分処理 (累計: {total_updated}日分)"
        )

    logger.info(f"  ✅ 全バッチ完了: {total_updated}日分更新")
    logger.info("")
    logger.info("=" * 70)
    logger.info("次のステップ:")
    logger.info("  1. fix_trade_date_manual.sql Step 4, 5を実行")
    logger.info("     （ユニーク制約とインデックスを再作成）")
    logger.info("  2. Step 6で検証SQLを実行")
    logger.info("=" * 70)

    return total_updated


def main():
    from datetime import datetime
    start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("trade_date 直接UPDATE - 開始")
    logger.info("=" * 70)
    logger.info("")

    try:
        # Step 1-2: マッピング作成
        mapping = build_date_mapping()

        # Step 3: 直接UPDATE
        updated = execute_direct_update(mapping)

        # 完了
        elapsed = (datetime.now() - start_time).total_seconds()

        logger.info("")
        logger.info("=" * 70)
        logger.info("✅ 処理完了")
        logger.info("=" * 70)
        logger.info(f"マッピング件数: {len(mapping)}日分")
        logger.info(f"更新日付数: {updated}日分")
        logger.info(f"処理時間: {elapsed/60:.1f}分 ({elapsed:.0f}秒)")
        logger.info("=" * 70)

        return updated

    except Exception as e:
        logger.error(f"❌ エラー発生: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == '__main__':
    main()
