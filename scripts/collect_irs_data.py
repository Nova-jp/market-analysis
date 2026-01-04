#!/usr/bin/env python3
"""
金利スワップデータ収集スクリプト
JPXのPDFから金利スワップ清算値段を取得してDBに保存
"""

import sys
from pathlib import Path
from datetime import datetime, date
import logging

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data.collectors.jscc.irs_collector import IRSCollector

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """メイン関数"""
    import argparse

    parser = argparse.ArgumentParser(description='金利スワップデータ収集')
    parser.add_argument(
        '--date',
        type=str,
        help='対象日 (YYYY-MM-DD形式)。指定しない場合は今日'
    )

    args = parser.parse_args()

    # 対象日を解析
    target_date = date.today()
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print(f"エラー: 日付形式が不正です: {args.date}")
            print("正しい形式: YYYY-MM-DD (例: 2025-12-22)")
            sys.exit(1)

    logger.info(f"{'='*80}")
    logger.info(f"金利スワップデータ収集開始")
    logger.info(f"対象日: {target_date}")
    logger.info(f"{'='*80}")

    # データ収集実行
    collector = IRSCollector()
    try:
        saved_count = collector.collect_data(target_date)
        
        if saved_count >= 0:
            logger.info(f"{'='*80}")
            logger.info(f"データ収集完了: {saved_count}件")
            logger.info(f"{'='*80}")
            sys.exit(0)
        else:
            logger.error("データ収集に失敗しました")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"データ収集エラー: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
