#!/usr/bin/env python3
"""
market_amount バッチ計算スクリプト

戦略: 銘柄を小バッチに分割してタイムアウト回避
"""

import sys
import os
from datetime import datetime
from typing import Set, List, Dict
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# ロガー設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MarketAmountBatchCalculator:
    def __init__(self, batch_size: int = 50):
        """
        Args:
            batch_size: 一度に処理する銘柄数（デフォルト50）
        """
        url = os.getenv('SUPABASE_URL')
        key = os.getenv('SUPABASE_KEY')
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")

        self.supabase = create_client(url, key)
        self.batch_size = batch_size

    def get_all_bond_codes(self) -> List[str]:
        """全銘柄コードを取得"""
        logger.info("  📋 全銘柄コードを取得中...")

        bond_codes = set()
        offset = 0
        limit = 1000

        while True:
            try:
                response = self.supabase.table('bond_data') \
                    .select('bond_code') \
                    .range(offset, offset + limit - 1) \
                    .execute()

                if not response.data:
                    break

                for record in response.data:
                    bond_codes.add(record['bond_code'])

                if len(response.data) < limit:
                    break

                offset += limit

                if offset % 50000 == 0:
                    logger.info(f"    進捗: {offset:,}件スキャン済み（ユニーク: {len(bond_codes):,}銘柄）")

            except Exception as e:
                logger.error(f"    ❌ エラー（offset={offset}）: {e}")
                break

        logger.info(f"  ✅ 全銘柄取得完了: {len(bond_codes):,}銘柄")
        return sorted(list(bond_codes))

    def calculate_for_bonds(self, bond_codes: List[str]) -> Dict:
        """
        指定銘柄のmarket_amountを計算（RPC使用）

        Args:
            bond_codes: 銘柄コードのリスト

        Returns:
            処理結果の辞書
        """
        try:
            # RPC関数呼び出し
            response = self.supabase.rpc(
                'update_market_amounts',
                {'target_bond_codes': bond_codes}
            ).execute()

            return {
                'success': True,
                'bonds_processed': len(bond_codes),
                'data': response.data
            }

        except Exception as e:
            logger.error(f"    ❌ バッチ処理エラー: {e}")
            return {
                'success': False,
                'bonds_processed': 0,
                'error': str(e)
            }

    def run(self):
        """メイン処理"""
        start_time = datetime.now()

        logger.info("=" * 70)
        logger.info("市中残存額（market_amount）バッチ計算 - 開始")
        logger.info("=" * 70)
        logger.info(f"  バッチサイズ: {self.batch_size}銘柄/バッチ")
        logger.info("")

        # Step 1: 全銘柄コード取得
        logger.info("=" * 70)
        logger.info("Step 1: 全銘柄コード取得")
        logger.info("=" * 70)

        all_bonds = self.get_all_bond_codes()
        total_bonds = len(all_bonds)

        logger.info("")
        logger.info("=" * 70)
        logger.info(f"Step 2: バッチ処理開始（{total_bonds}銘柄 → {self.batch_size}銘柄/バッチ）")
        logger.info("=" * 70)
        logger.info("")

        # バッチに分割
        total_batches = (total_bonds + self.batch_size - 1) // self.batch_size
        success_count = 0
        error_count = 0

        for i in range(0, total_bonds, self.batch_size):
            batch_num = (i // self.batch_size) + 1
            batch = all_bonds[i:i + self.batch_size]

            logger.info(f"  🔄 バッチ {batch_num}/{total_batches} 処理中... ({len(batch)}銘柄)")

            result = self.calculate_for_bonds(batch)

            if result['success']:
                success_count += result['bonds_processed']
                logger.info(f"    ✅ 完了: {result['bonds_processed']}銘柄")
            else:
                error_count += len(batch)
                logger.error(f"    ❌ 失敗: {result.get('error', 'Unknown error')}")

            # 進捗表示（10バッチごと）
            if batch_num % 10 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                processed = batch_num * self.batch_size
                speed = processed / elapsed if elapsed > 0 else 0
                remaining = (total_bonds - processed) / speed if speed > 0 else 0

                logger.info("")
                logger.info(f"  📊 進捗: {batch_num}/{total_batches}バッチ ({processed}/{total_bonds}銘柄)")
                logger.info(f"     成功: {success_count}銘柄, エラー: {error_count}銘柄")
                logger.info(f"     速度: {speed:.1f}銘柄/秒, 残り時間: 約{remaining/60:.1f}分")
                logger.info("")

        # 完了サマリー
        elapsed = (datetime.now() - start_time).total_seconds()

        logger.info("")
        logger.info("=" * 70)
        logger.info("✅ バッチ処理完了")
        logger.info("=" * 70)
        logger.info(f"  処理銘柄数: {success_count}/{total_bonds}銘柄")
        logger.info(f"  成功: {success_count}銘柄")
        logger.info(f"  エラー: {error_count}銘柄")
        logger.info(f"  処理時間: {elapsed/60:.1f}分 ({elapsed:.0f}秒)")
        logger.info(f"  平均速度: {success_count/elapsed:.1f}銘柄/秒")
        logger.info("=" * 70)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='market_amount バッチ計算')
    parser.add_argument('--batch-size', type=int, default=50,
                        help='バッチサイズ（デフォルト: 50銘柄）')

    args = parser.parse_args()

    calculator = MarketAmountBatchCalculator(batch_size=args.batch_size)
    calculator.run()


if __name__ == '__main__':
    main()
