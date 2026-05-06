#!/usr/bin/env python3
"""
market_amount 月単位バッチ計算ランナー
月ごとに分割して計算（タイムアウト対策）

使用方法:
    python scripts/calculate_market_amount_monthly_runner.py
"""

import os
import sys
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/market_amount_monthly.log'),
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


def calculate_monthly(supabase: Client, start_date: str, end_date: str) -> dict:
    """指定期間の月単位バッチ計算を実行"""
    try:
        result = supabase.rpc('calculate_market_amount_monthly', {
            'start_date': start_date,
            'end_date': end_date
        }).execute()

        if result.data and len(result.data) > 0:
            return result.data[0]
        return None

    except Exception as e:
        logger.error(f"  ❌ エラー: {e}")
        return None


def generate_month_ranges(start_date_str: str, end_date_str: str):
    """月ごとの日付範囲を生成"""
    start = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end = datetime.strptime(end_date_str, '%Y-%m-%d').date()

    current = date(start.year, start.month, 1)
    ranges = []

    while current <= end:
        # 月の最終日を計算
        next_month = current + relativedelta(months=1)
        month_end = next_month - relativedelta(days=1)

        # 範囲に含まれる部分のみ
        range_start = max(current, start)
        range_end = min(month_end, end)

        if range_start <= range_end:
            ranges.append((str(range_start), str(range_end)))

        current = next_month

    return ranges


def main():
    """メイン処理"""
    logger.info("=" * 70)
    logger.info("market_amount 月単位バッチ計算ランナー")
    logger.info("=" * 70)
    logger.info("  方式: 月ごとにバッチ処理（タイムアウト対策）")
    logger.info("  推定時間: 1ヶ月あたり1-5秒")
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

    # 月ごとの範囲を生成
    month_ranges = generate_month_ranges(min_date, max_date)

    logger.info(f"📊 処理対象: {len(month_ranges)}ヶ月分")
    logger.info(f"   期間: {min_date} ～ {max_date}")
    logger.info("")

    # 月ごとに処理
    total_updated = 0
    total_time = 0
    success_count = 0
    fail_count = 0

    for i, (start, end) in enumerate(month_ranges, 1):
        logger.info(f"🔄 [{i}/{len(month_ranges)}] {start} ～ {end}")

        result = calculate_monthly(supabase, start, end)

        if result:
            updated = result.get('updated_count', 0)
            exec_time = result.get('execution_time_seconds', 0)

            total_updated += updated
            total_time += exec_time
            success_count += 1

            logger.info(f"   ✅ 完了: {updated:,}件更新 ({exec_time:.2f}秒)")
        else:
            fail_count += 1
            logger.warning(f"   ⚠️  スキップ（タイムアウト）")

        # 10件ごとにサマリー表示
        if i % 10 == 0:
            logger.info("")
            logger.info(f"   📈 進捗: {i}/{len(month_ranges)}ヶ月 ({100*i/len(month_ranges):.1f}%)")
            logger.info(f"   累計更新: {total_updated:,}件")
            logger.info(f"   成功/失敗: {success_count}/{fail_count}")
            logger.info("")

    # 完了サマリー
    logger.info("")
    logger.info("=" * 70)
    logger.info("🎉 処理完了！")
    logger.info("=" * 70)
    logger.info(f"  処理月数: {len(month_ranges)}ヶ月")
    logger.info(f"  成功: {success_count}ヶ月")
    logger.info(f"  失敗（タイムアウト）: {fail_count}ヶ月")
    logger.info(f"  更新レコード数: {total_updated:,}件")
    logger.info(f"  合計実行時間: {total_time:.2f}秒 ({total_time/60:.1f}分)")

    if total_time > 0 and total_updated > 0:
        logger.info(f"  平均速度: {total_updated/total_time:,.0f}件/秒")

    logger.info("=" * 70)
    logger.info("")

    # 検証
    if fail_count == 0:
        logger.info("🔍 最終検証中...")
        try:
            result = supabase.table('bond_data') \
                .select('trade_date') \
                .is_('market_amount', 'null') \
                .limit(1) \
                .execute()

            if result.data:
                logger.warning(f"⚠️  まだ未計算データが残っています")
                logger.info(f"   最初の未計算日: {result.data[0]['trade_date']}")
            else:
                logger.info("✅ すべて計算完了！")
        except Exception as e:
            logger.error(f"❌ 検証エラー: {e}")
    else:
        logger.warning(f"⚠️  {fail_count}ヶ月でタイムアウトが発生しました")
        logger.info("   より細かい粒度（週単位・日単位）が必要かもしれません")


if __name__ == '__main__':
    main()
