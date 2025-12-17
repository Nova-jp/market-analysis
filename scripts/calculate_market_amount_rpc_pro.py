#!/usr/bin/env python3
"""
market_amount 高速再計算スクリプト (Supabase PRO版)
PostgreSQL RPC関数を使用してDB内で一括計算

使用方法:
    python scripts/calculate_market_amount_rpc_pro.py
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

def create_rpc_function(supabase: Client) -> bool:
    """
    RPC関数をデータベースに作成

    Returns:
        bool: 成功した場合True
    """
    logger.info("======================================================================")
    logger.info("Step 1: RPC関数の作成")
    logger.info("======================================================================")

    # SQL関数定義を読み込み
    sql_file = project_root / "scripts" / "sql" / "calculate_market_amount_rpc.sql"

    if not sql_file.exists():
        logger.error(f"SQLファイルが見つかりません: {sql_file}")
        return False

    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    logger.info(f"  📄 SQLファイル読み込み: {sql_file.name}")

    try:
        # Supabase SQL Editorで実行する必要があるため、手動実行を促す
        logger.warning("=" * 70)
        logger.warning("⚠️  手動操作が必要です")
        logger.warning("=" * 70)
        logger.warning("")
        logger.warning("以下の手順で RPC関数を作成してください:")
        logger.warning("")
        logger.warning("1. Supabase ダッシュボードを開く")
        logger.warning("   https://supabase.com/dashboard")
        logger.warning("")
        logger.warning("2. SQL Editor を開く")
        logger.warning("")
        logger.warning("3. 以下のファイルの内容をコピー&ペーストして実行:")
        logger.warning(f"   {sql_file}")
        logger.warning("")
        logger.warning("4. 実行後、このスクリプトを続行してください")
        logger.warning("")
        logger.warning("=" * 70)

        response = input("\nRPC関数の作成が完了しましたか? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            logger.info("処理を中断しました")
            return False

        return True

    except Exception as e:
        logger.error(f"  ❌ エラー: {e}")
        return False


def execute_rpc_calculation(supabase: Client) -> dict:
    """
    RPC関数を実行してmarket_amountを計算

    Returns:
        dict: 実行結果 {updated_count, execution_time_seconds}
    """
    logger.info("")
    logger.info("======================================================================")
    logger.info("Step 2: RPC関数の実行")
    logger.info("======================================================================")
    logger.info("  🚀 高速計算を開始します...")
    logger.info("  ⏱️  PRO版タイムアウト: 60秒")
    logger.info("")

    try:
        # RPC関数を呼び出し
        result = supabase.rpc('calculate_market_amount_fast').execute()

        if result.data and len(result.data) > 0:
            data = result.data[0]
            updated_count = data.get('updated_count', 0)
            execution_time = data.get('execution_time_seconds', 0)

            logger.info("  ✅ 計算完了！")
            logger.info(f"  📊 更新レコード数: {updated_count:,}件")
            logger.info(f"  ⏱️  実行時間: {execution_time:.2f}秒")

            return {
                'updated_count': updated_count,
                'execution_time_seconds': execution_time
            }
        else:
            logger.error("  ❌ RPC関数が結果を返しませんでした")
            return None

    except Exception as e:
        logger.error(f"  ❌ RPC実行エラー: {e}")
        logger.error("")
        logger.error("考えられる原因:")
        logger.error("  1. RPC関数がまだ作成されていない")
        logger.error("  2. タイムアウト（60秒超過）")
        logger.error("  3. メモリ不足")
        logger.error("")
        return None


def verify_results(supabase: Client):
    """
    計算結果を検証
    """
    logger.info("")
    logger.info("======================================================================")
    logger.info("Step 3: 結果の検証")
    logger.info("======================================================================")

    try:
        # market_amount の統計情報を取得
        result = supabase.rpc('check_market_amount_stats').execute()

        if result.data and len(result.data) > 0:
            stats = result.data[0]
            logger.info(f"  総レコード数: {stats.get('total_records', 0):,}件")
            logger.info(f"  計算済み: {stats.get('calculated_records', 0):,}件")
            logger.info(f"  NULL: {stats.get('null_records', 0):,}件")
            logger.info(f"  完了率: {stats.get('completion_percentage', 0):.2f}%")
            logger.info(f"  最小値: {stats.get('min_value', 0):,}")
            logger.info(f"  最大値: {stats.get('max_value', 0):,}")
            logger.info(f"  平均値: {stats.get('avg_value', 0):,.0f}")
        else:
            logger.warning("  ⚠️  統計情報を取得できませんでした")
            logger.info("  代わりに簡易チェックを実行します...")

            # 簡易チェック
            result = supabase.table('bond_data').select('*', count='exact').limit(1).execute()
            total_count = result.count

            result = supabase.table('bond_data').select('*', count='exact').not_.is_('market_amount', 'null').limit(1).execute()
            calculated_count = result.count

            logger.info(f"  総レコード数: {total_count:,}件")
            logger.info(f"  計算済み: {calculated_count:,}件")
            logger.info(f"  完了率: {100.0 * calculated_count / total_count:.2f}%")

    except Exception as e:
        logger.error(f"  ❌ 検証エラー: {e}")


def main():
    """メイン処理"""
    logger.info("======================================================================")
    logger.info("market_amount 高速再計算 (Supabase PRO版)")
    logger.info("======================================================================")
    logger.info("  方式: PostgreSQL RPC関数（DB内一括処理）")
    logger.info("  対象: 全レコード (~1,875,982件)")
    logger.info("  推定時間: 30秒～2分")
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

    # Step 1: RPC関数作成の確認
    if not create_rpc_function(supabase):
        return

    # Step 2: RPC関数実行
    result = execute_rpc_calculation(supabase)

    if result:
        logger.info("")
        logger.info("=" * 70)
        logger.info("🎉 処理が正常に完了しました！")
        logger.info("=" * 70)
        logger.info(f"  更新レコード数: {result['updated_count']:,}件")
        logger.info(f"  実行時間: {result['execution_time_seconds']:.2f}秒")
        logger.info(f"  処理速度: {result['updated_count'] / result['execution_time_seconds']:,.0f}件/秒")
        logger.info("=" * 70)

        # Step 3: 検証
        verify_results(supabase)
    else:
        logger.error("")
        logger.error("=" * 70)
        logger.error("❌ 処理が失敗しました")
        logger.error("=" * 70)
        logger.error("")
        logger.error("代替案:")
        logger.error("  1. Supabase SQL Editorで直接SQLを実行")
        logger.error("  2. より小さいバッチサイズで実行")
        logger.error("  3. scripts/calculate_market_amount_micro_batch.py を使用")
        logger.error("")


if __name__ == '__main__':
    main()
