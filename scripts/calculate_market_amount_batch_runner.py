#!/usr/bin/env python3
"""
market_amount バッチ計算ランナー
年ごとに分割して計算（タイムアウト対策）

使用方法:
    python scripts/calculate_market_amount_batch_runner.py
"""

import os
import sys
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
import logging
from datetime import datetime

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/market_amount_batch.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 環境変数読み込み
load_dotenv()


def get_date_range(supabase: Client):
    """未計算データの日付範囲を取得"""
    logger.info("📅 未計算データの日付範囲を取得中...")

    # 最小・最大日付を取得
    result = supabase.table('bond_data') \
        .select('trade_date') \
        .is_('market_amount', 'null') \
        .order('trade_date', desc=False) \
        .limit(1) \
        .execute()

    if not result.data:
        return None, None

    min_date = result.data[0]['trade_date']

    result = supabase.table('bond_data') \
        .select('trade_date') \
        .is_('market_amount', 'null') \
        .order('trade_date', desc=True) \
        .limit(1) \
        .execute()

    max_date = result.data[0]['trade_date']

    logger.info(f"  最小日付: {min_date}")
    logger.info(f"  最大日付: {max_date}")

    return min_date, max_date


def calculate_batch(supabase: Client, start_date: str, end_date: str) -> dict:
    """指定期間のバッチ計算を実行"""
    try:
        result = supabase.rpc('calculate_market_amount_batch', {
            'start_date': start_date,
            'end_date': end_date
        }).execute()

        if result.data and len(result.data) > 0:
            return result.data[0]
        return None

    except Exception as e:
        logger.error(f"  ❌ エラー: {e}")
        return None


def main():
    """メイン処理"""
    logger.info("=" * 70)
    logger.info("market_amount バッチ計算ランナー")
    logger.info("=" * 70)
    logger.info("  方式: 年ごとにバッチ処理（タイムアウト対策）")
    logger.info("  推定時間: 1年あたり10-30秒")
    logger.info("=" * 70)
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

    # 日付範囲を取得
    min_date, max_date = get_date_range(supabase)

    if not min_date or not max_date:
        logger.info("✅ すべて計算済みです！")
        return

    # 開始年・終了年を取得
    start_year = int(min_date.split('-')[0])
    end_year = int(max_date.split('-')[0])

    logger.info(f"📊 処理対象期間: {start_year}年 ～ {end_year}年")
    logger.info(f"   ({end_year - start_year + 1}年分)")
    logger.info("")

    # 年ごとに処理
    total_updated = 0
    total_time = 0

    for year in range(start_year, end_year + 1):
        batch_start = f"{year}-01-01"
        batch_end = f"{year}-12-31"

        logger.info(f"🔄 {year}年を処理中...")
        logger.info(f"   期間: {batch_start} ～ {batch_end}")

        result = calculate_batch(supabase, batch_start, batch_end)

        if result:
            updated = result.get('updated_count', 0)
            exec_time = result.get('execution_time_seconds', 0)

            total_updated += updated
            total_time += exec_time

            logger.info(f"   ✅ 完了: {updated:,}件更新 ({exec_time:.2f}秒)")
        else:
            logger.warning(f"   ⚠️  スキップ")

        logger.info("")

    # 完了サマリー
    logger.info("=" * 70)
    logger.info("🎉 処理完了！")
    logger.info("=" * 70)
    logger.info(f"  処理年数: {end_year - start_year + 1}年")
    logger.info(f"  更新レコード数: {total_updated:,}件")
    logger.info(f"  合計実行時間: {total_time:.2f}秒 ({total_time/60:.1f}分)")

    if total_time > 0:
        logger.info(f"  平均速度: {total_updated/total_time:,.0f}件/秒")

    logger.info("=" * 70)
    logger.info("")

    # 検証
    logger.info("🔍 最終検証中...")
    result = supabase.table('bond_data').select('*', count='exact').limit(1).execute()
    total_count = result.count

    result = supabase.table('bond_data').select('*', count='exact').not_.is_('market_amount', 'null').limit(1).execute()
    calculated_count = result.count

    null_count = total_count - calculated_count
    completion_pct = 100.0 * calculated_count / total_count if total_count > 0 else 0

    logger.info(f"  総レコード数: {total_count:,}件")
    logger.info(f"  計算済み: {calculated_count:,}件")
    logger.info(f"  NULL: {null_count:,}件")
    logger.info(f"  完了率: {completion_pct:.2f}%")
    logger.info("")


if __name__ == '__main__':
    main()
