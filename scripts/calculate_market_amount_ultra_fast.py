#!/usr/bin/env python3
"""
market_amount 超高速再計算スクリプト (Supabase PRO版)
PostgreSQL RPC関数を使用してDB内で一括計算

特徴:
- DB内で完結するため、ネットワークオーバーヘッドなし
- 1回のUPDATEで全レコード更新
- Supabase PRO版の60秒タイムアウトに最適化

使用方法:
    python scripts/calculate_market_amount_ultra_fast.py
"""

import os
import sys
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
import logging

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


def read_sql_file() -> str:
    """SQLファイルを読み込み"""
    sql_file = project_root / "scripts" / "sql" / "calculate_market_amount_ultra_fast.sql"

    if not sql_file.exists():
        raise FileNotFoundError(f"SQLファイルが見つかりません: {sql_file}")

    with open(sql_file, 'r', encoding='utf-8') as f:
        return f.read()


def execute_rpc_calculation(supabase: Client) -> dict:
    """
    RPC関数を実行してmarket_amountを計算

    Returns:
        dict: 実行結果 {updated_count, execution_time_seconds}
    """
    logger.info("🚀 超高速計算を開始します...")
    logger.info("⏱️  PRO版タイムアウト: 60秒")
    logger.info("")

    try:
        # RPC関数を呼び出し
        result = supabase.rpc('calculate_market_amount_ultra_fast').execute()

        if result.data and len(result.data) > 0:
            data = result.data[0]
            updated_count = data.get('updated_count', 0)
            execution_time = data.get('execution_time_seconds', 0)

            logger.info("✅ 計算完了！")
            logger.info(f"📊 更新レコード数: {updated_count:,}件")
            logger.info(f"⏱️  実行時間: {execution_time:.2f}秒")

            return {
                'updated_count': updated_count,
                'execution_time_seconds': execution_time
            }
        else:
            logger.error("❌ RPC関数が結果を返しませんでした")
            return None

    except Exception as e:
        logger.error(f"❌ RPC実行エラー: {e}")
        logger.error("")
        logger.error("考えられる原因:")
        logger.error("  1. RPC関数がまだ作成されていない")
        logger.error("  2. タイムアウト（60秒超過）")
        logger.error("  3. メモリ不足")
        logger.error("")
        return None


def verify_results(supabase: Client):
    """計算結果を検証"""
    logger.info("")
    logger.info("=" * 70)
    logger.info("🔍 結果の検証")
    logger.info("=" * 70)

    try:
        # 簡易チェック
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

    except Exception as e:
        logger.error(f"❌ 検証エラー: {e}")


def main():
    """メイン処理"""
    logger.info("=" * 70)
    logger.info("market_amount 超高速再計算 (Supabase PRO版)")
    logger.info("=" * 70)
    logger.info("  方式: PostgreSQL RPC関数（DB内一括処理）")
    logger.info("  対象: market_amount が NULL のレコードのみ")
    logger.info("  推定時間: 10秒～60秒")
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

    # SQLファイル読み込み
    logger.info("📄 SQLファイル読み込み中...")
    try:
        sql_content = read_sql_file()
        logger.info("✅ SQLファイル読み込み成功")
        logger.info("")
    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        return

    # RPC関数作成の案内
    logger.info("=" * 70)
    logger.info("⚠️  手動操作が必要です")
    logger.info("=" * 70)
    logger.info("")
    logger.info("以下の手順で RPC関数を作成してください:")
    logger.info("")
    logger.info("1. Supabase ダッシュボードを開く")
    logger.info("   https://supabase.com/dashboard")
    logger.info("")
    logger.info("2. SQL Editor を開く")
    logger.info("")
    logger.info("3. 以下のファイルの内容をコピー&ペーストして実行:")
    logger.info("   scripts/sql/calculate_market_amount_ultra_fast.sql")
    logger.info("")
    logger.info("4. 実行後、このスクリプトを続行してください")
    logger.info("")
    logger.info("=" * 70)
    logger.info("")

    response = input("RPC関数の作成が完了しましたか? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        logger.info("処理を中断しました")
        return

    logger.info("")

    # RPC関数実行
    result = execute_rpc_calculation(supabase)

    if result:
        logger.info("")
        logger.info("=" * 70)
        logger.info("🎉 処理が正常に完了しました！")
        logger.info("=" * 70)
        logger.info(f"  更新レコード数: {result['updated_count']:,}件")
        logger.info(f"  実行時間: {result['execution_time_seconds']:.2f}秒")

        if result['execution_time_seconds'] > 0:
            speed = result['updated_count'] / result['execution_time_seconds']
            logger.info(f"  処理速度: {speed:,.0f}件/秒")

        logger.info("=" * 70)

        # 検証
        verify_results(supabase)
    else:
        logger.error("")
        logger.error("=" * 70)
        logger.error("❌ 処理が失敗しました")
        logger.error("=" * 70)


if __name__ == '__main__':
    main()
