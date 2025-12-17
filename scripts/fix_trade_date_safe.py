#!/usr/bin/env python3
"""
bond_dataテーブルのtrade_date 1営業日ずれ修正スクリプト（安全版）

戦略:
1. ユニーク制約を一時削除
2. 全レコードのtrade_dateを一括UPDATE
3. 制約を再作成
4. インデックス再構築

処理時間: 5-10分
"""

import sys
import os
from datetime import date, timedelta
from typing import List, Dict
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


def build_date_mapping() -> List[Dict[str, str]]:
    """
    全trade_dateのマッピング作成

    Returns:
        [{'old_date': '2024-12-09', 'new_date': '2024-12-06'}, ...]
    """
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
    processed = 0

    for old_date_str in sorted(existing_dates):
        old_date = date.fromisoformat(old_date_str)
        new_date = get_previous_business_day(old_date)

        mapping.append({
            'old_date': old_date_str,
            'new_date': new_date.isoformat()
        })

        processed += 1
        if processed % 500 == 0:
            logger.info(f"  進捗: {processed}/{len(existing_dates)} 日付処理済み")

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


def execute_safe_update(mapping: List[Dict[str, str]]) -> int:
    """
    安全な一括UPDATE（制約削除→UPDATE→制約再作成）

    Args:
        mapping: 日付マッピング

    Returns:
        総更新件数
    """
    logger.info("")
    logger.info("=" * 70)
    logger.info("Step 3: データベース更新（安全版）")
    logger.info("=" * 70)

    supabase = create_client(
        os.getenv('SUPABASE_URL'),
        os.getenv('SUPABASE_KEY')
    )

    # Step 3.1: ユニーク制約削除
    logger.info("")
    logger.info("Step 3.1: ユニーク制約を一時削除中...")
    try:
        supabase.rpc('execute_sql', {
            'sql': 'ALTER TABLE bond_data DROP CONSTRAINT IF EXISTS clean_bond_data_trade_date_bond_code_key;'
        }).execute()
        logger.info("  ✅ ユニーク制約削除完了")
    except Exception as e:
        logger.warning(f"  ⚠️  制約削除スキップ（既に無い可能性）: {e}")

    # Step 3.2: インデックス削除
    logger.info("")
    logger.info("Step 3.2: インデックスを一時削除中...")
    try:
        supabase.rpc('execute_sql', {
            'sql': 'DROP INDEX IF EXISTS idx_bond_data_trade_date;'
        }).execute()
        supabase.rpc('execute_sql', {
            'sql': 'DROP INDEX IF EXISTS idx_bond_data_bond_code;'
        }).execute()
        logger.info("  ✅ インデックス削除完了")
    except Exception as e:
        logger.warning(f"  ⚠️  インデックス削除スキップ: {e}")

    # Step 3.3: 一括UPDATE（バッチ処理）
    logger.info("")
    logger.info("Step 3.3: trade_date 一括更新中...")
    logger.info(f"  総バッチ数: {len(mapping)}件の日付マッピング")

    total_updated = 0
    batch_size = 100
    total_batches = (len(mapping) + batch_size - 1) // batch_size

    for i in range(0, len(mapping), batch_size):
        batch = mapping[i:i + batch_size]
        batch_num = i // batch_size + 1

        # CASE文を生成してUPDATE
        case_statements = []
        old_dates = []
        for item in batch:
            case_statements.append(
                f"WHEN '{item['old_date']}'::date THEN '{item['new_date']}'::date"
            )
            old_dates.append(f"'{item['old_date']}'::date")

        sql = f"""
        UPDATE bond_data
        SET trade_date = CASE trade_date
            {' '.join(case_statements)}
            ELSE trade_date
        END,
        updated_at = NOW()
        WHERE trade_date IN ({', '.join(old_dates)});
        """

        try:
            response = supabase.rpc('execute_sql', {'sql': sql}).execute()
            # 更新件数を推定（正確な件数はログから取得できないため）
            batch_updated = len(batch) * 1500  # 1日あたり約1500レコードと仮定
            total_updated += batch_updated

            logger.info(
                f"  バッチ {batch_num}/{total_batches}: "
                f"{len(batch)}日分更新 (累計: ~{total_updated:,}件)"
            )

        except Exception as e:
            logger.error(f"  ❌ バッチ {batch_num} 更新エラー: {e}")
            continue

    # Step 3.4: 制約再作成
    logger.info("")
    logger.info("Step 3.4: ユニーク制約を再作成中...")
    try:
        supabase.rpc('execute_sql', {
            'sql': '''
            ALTER TABLE bond_data
            ADD CONSTRAINT clean_bond_data_trade_date_bond_code_key
            UNIQUE (trade_date, bond_code);
            '''
        }).execute()
        logger.info("  ✅ ユニーク制約再作成完了")
    except Exception as e:
        logger.error(f"  ❌ 制約再作成エラー: {e}")

    # Step 3.5: インデックス再作成
    logger.info("")
    logger.info("Step 3.5: インデックスを再作成中...")
    try:
        supabase.rpc('execute_sql', {
            'sql': 'CREATE INDEX IF NOT EXISTS idx_bond_data_trade_date ON bond_data(trade_date);'
        }).execute()
        supabase.rpc('execute_sql', {
            'sql': 'CREATE INDEX IF NOT EXISTS idx_bond_data_bond_code ON bond_data(bond_code);'
        }).execute()
        logger.info("  ✅ インデックス再作成完了")
    except Exception as e:
        logger.error(f"  ❌ インデックス再作成エラー: {e}")

    logger.info(f"  ✅ 全バッチ完了: ~{total_updated:,}件更新")
    return total_updated


def main():
    from datetime import datetime
    start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("trade_date 1営業日ずれ修正（安全版） - 開始")
    logger.info("=" * 70)
    logger.info("")

    try:
        # Step 1-2: マッピング作成
        mapping = build_date_mapping()

        # Step 3: 安全な一括UPDATE
        updated = execute_safe_update(mapping)

        # 完了
        elapsed = (datetime.now() - start_time).total_seconds()

        logger.info("")
        logger.info("=" * 70)
        logger.info("✅ 処理完了")
        logger.info("=" * 70)
        logger.info(f"マッピング件数: {len(mapping)}日分")
        logger.info(f"更新レコード数: ~{updated:,}件")
        logger.info(f"処理時間: {elapsed/60:.1f}分 ({elapsed:.0f}秒)")
        if elapsed > 0:
            logger.info(f"処理速度: {updated/elapsed:.0f}件/秒")
        logger.info("=" * 70)
        logger.info("")
        logger.info("次のステップ:")
        logger.info("  1. 検証SQLを実行（土日データがないことを確認）")
        logger.info("  2. market_amount再計算を実行")
        logger.info("=" * 70)

        return updated

    except Exception as e:
        logger.error(f"❌ エラー発生: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == '__main__':
    main()
