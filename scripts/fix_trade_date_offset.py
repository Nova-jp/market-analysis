#!/usr/bin/env python3
"""
bond_dataテーブルのtrade_date 1営業日ずれ修正スクリプト

問題:
- JSDAサイトのデータは、表示日付の1営業日前のデータ
- 既存のDBデータが1営業日ずれている（178万件）

解決策:
1. 全ユニークtrade_dateを取得
2. jpholidayで各日付の1営業日前を計算
3. RPC関数で一括UPDATE

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


def execute_update_via_rpc(mapping: List[Dict[str, str]], batch_size: int = 50) -> int:
    """
    RPC関数で一括UPDATE

    Args:
        mapping: 日付マッピング
        batch_size: 1回のRPC呼び出しで処理する日付数

    Returns:
        総更新件数
    """
    logger.info("")
    logger.info("=" * 70)
    logger.info("Step 3: データベース更新（RPC関数使用）")
    logger.info("=" * 70)
    logger.info(f"  バッチサイズ: {batch_size}日/バッチ")
    logger.info(f"  総バッチ数: {(len(mapping) + batch_size - 1) // batch_size}")
    logger.info("")

    supabase = create_client(
        os.getenv('SUPABASE_URL'),
        os.getenv('SUPABASE_KEY')
    )

    total_updated = 0
    total_batches = (len(mapping) + batch_size - 1) // batch_size

    for i in range(0, len(mapping), batch_size):
        batch = mapping[i:i + batch_size]
        batch_num = i // batch_size + 1

        try:
            response = supabase.rpc('update_trade_dates_batch', {
                'updates': batch
            }).execute()

            batch_updated = response.data['total_updated']
            total_updated += batch_updated

            logger.info(
                f"  バッチ {batch_num}/{total_batches}: "
                f"{batch_updated:,}件更新 (累計: {total_updated:,}件)"
            )

        except Exception as e:
            logger.error(f"  ❌ バッチ {batch_num} 更新エラー: {e}")
            logger.error(f"     バッチ範囲: {i} - {i + len(batch)}")
            # エラーがあっても続行
            continue

    logger.info(f"  ✅ 全バッチ完了: {total_updated:,}件更新")
    return total_updated


def verify_rpc_function_exists(supabase) -> bool:
    """RPC関数が存在するか確認"""
    try:
        # 空配列でテスト呼び出し
        response = supabase.rpc('update_trade_dates_batch', {'updates': []}).execute()
        logger.info("✅ RPC関数 'update_trade_dates_batch' が存在します")
        return True
    except Exception as e:
        logger.error("❌ RPC関数 'update_trade_dates_batch' が見つかりません")
        logger.error(f"   エラー: {e}")
        logger.error("")
        logger.error("=" * 70)
        logger.error("scripts/create_update_trade_date_rpc.sql を参照してください")
        logger.error("=" * 70)
        return False


def main():
    from datetime import datetime
    start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("trade_date 1営業日ずれ修正 - 開始")
    logger.info("=" * 70)
    logger.info("")

    # RPC関数の存在確認
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')
    supabase = create_client(url, key)

    if not verify_rpc_function_exists(supabase):
        logger.error("")
        logger.error("RPC関数を作成してから再実行してください。")
        sys.exit(1)

    logger.info("")

    try:
        # Step 1-2: マッピング作成
        mapping = build_date_mapping()

        # Step 3: RPC関数で一括UPDATE
        updated = execute_update_via_rpc(mapping)

        # 完了
        elapsed = (datetime.now() - start_time).total_seconds()

        logger.info("")
        logger.info("=" * 70)
        logger.info("✅ 処理完了")
        logger.info("=" * 70)
        logger.info(f"マッピング件数: {len(mapping)}日分")
        logger.info(f"更新レコード数: {updated:,}件")
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
