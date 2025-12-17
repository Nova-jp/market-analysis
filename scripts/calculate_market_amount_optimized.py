#!/usr/bin/env python3
"""
市中残存額（market_amount）高速計算スクリプト（チャンク処理 + RPC版）

アルゴリズム:
1. 銘柄を100個ずつチャンクに分割
2. 各チャンクごとに:
   - データ取得（3クエリ）
   - メモリ内計算（O(N)）
   - RPC関数で一括UPDATE（1000件ずつバッチ）
   - メモリ解放

メモリ使用量: 最大20MB程度
処理時間: 5-10分（従来の数十時間から大幅短縮）

前提条件:
Supabaseに update_market_amounts RPC関数が作成されていること
（scripts/create_rpc_function.sql 参照）
"""

import sys
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
from bisect import bisect_right
import logging
import gc

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OptimizedMarketAmountCalculator:
    """最適化された市中残存額計算クラス（チャンク処理版）"""

    def __init__(self, chunk_size: int = 100):
        url = os.getenv('SUPABASE_URL')
        key = os.getenv('SUPABASE_KEY')
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        self.supabase = create_client(url, key)
        self.chunk_size = chunk_size

    def get_all_bond_codes(self) -> List[str]:
        """全銘柄コードを取得（bond_auctionテーブルから）"""
        logger.info("📋 銘柄コード一覧取得中...")

        # bond_auctionテーブルから取得（レコード数が少ない: 10-20k件）
        all_codes = set()
        offset = 0
        limit = 1000

        while True:
            response = self.supabase.table('bond_auction') \
                .select('bond_code') \
                .order('bond_code', desc=False) \
                .range(offset, offset + limit - 1) \
                .execute()

            if not response.data:
                break

            for record in response.data:
                all_codes.add(record['bond_code'])

            if len(response.data) < limit:
                break

            offset += limit

            # 進捗表示
            if offset % 10000 == 0:
                logger.info(f"  進捗: {offset}件スキャン済み（ユニーク: {len(all_codes)}銘柄）")

        unique_codes = sorted(all_codes)
        logger.info(f"✅ 銘柄数: {len(unique_codes)}件")
        return unique_codes

    def fetch_chunk_data(
        self,
        bond_codes: List[str]
    ) -> Tuple[Dict, Dict, Dict]:
        """
        指定銘柄のデータを取得

        Returns:
            (auction_by_bond, boj_by_bond, trades_by_bond)
        """
        # 発行履歴
        auction_by_bond = defaultdict(list)
        offset = 0
        limit = 1000

        while True:
            response = self.supabase.table('bond_auction') \
                .select('bond_code, auction_date, allocated_amount') \
                .in_('bond_code', bond_codes) \
                .order('bond_code', desc=False) \
                .order('auction_date', desc=False) \
                .range(offset, offset + limit - 1) \
                .execute()

            if not response.data:
                break

            for record in response.data:
                bond_code = record['bond_code']
                auction_date = record['auction_date']
                allocated_amount = record.get('allocated_amount', 0) or 0
                auction_by_bond[bond_code].append((auction_date, int(allocated_amount)))

            if len(response.data) < limit:
                break

            offset += limit

        # 日銀保有履歴
        boj_by_bond = defaultdict(list)
        offset = 0

        while True:
            response = self.supabase.table('boj_holdings') \
                .select('bond_code, data_date, face_value') \
                .in_('bond_code', bond_codes) \
                .order('bond_code', desc=False) \
                .order('data_date', desc=False) \
                .range(offset, offset + limit - 1) \
                .execute()

            if not response.data:
                break

            for record in response.data:
                bond_code = record['bond_code']
                data_date = record['data_date']
                face_value = record.get('face_value', 0) or 0
                boj_by_bond[bond_code].append((data_date, int(face_value)))

            if len(response.data) < limit:
                break

            offset += limit

        # 取引日一覧
        trades_by_bond = defaultdict(list)
        offset = 0

        while True:
            response = self.supabase.table('bond_data') \
                .select('bond_code, trade_date') \
                .in_('bond_code', bond_codes) \
                .order('bond_code', desc=False) \
                .order('trade_date', desc=False) \
                .range(offset, offset + limit - 1) \
                .execute()

            if not response.data:
                break

            for record in response.data:
                bond_code = record['bond_code']
                trade_date = record['trade_date']
                trades_by_bond[bond_code].append(trade_date)

            if len(response.data) < limit:
                break

            offset += limit

        return auction_by_bond, boj_by_bond, trades_by_bond

    def calculate_in_memory(
        self,
        auction_by_bond: Dict,
        boj_by_bond: Dict,
        trades_by_bond: Dict
    ) -> List[Tuple[str, str, int]]:
        """
        メモリ内で市中残存額を計算

        Returns:
            [(bond_code, trade_date, market_amount), ...]
        """
        results = []

        for bond_code, trade_dates in trades_by_bond.items():
            auctions = auction_by_bond.get(bond_code, [])
            boj_holdings = boj_by_bond.get(bond_code, [])

            # 累積発行額を計算しながら各取引日の市中残存額を計算
            cumulative_issuance = 0
            auction_idx = 0

            for trade_date in trade_dates:
                # その日までの累積発行額
                while auction_idx < len(auctions) and auctions[auction_idx][0] <= trade_date:
                    cumulative_issuance += auctions[auction_idx][1]
                    auction_idx += 1

                # その日時点の日銀保有額（二分探索）
                boj_holding = 0
                if boj_holdings:
                    dates = [h[0] for h in boj_holdings]
                    idx = bisect_right(dates, trade_date) - 1
                    if idx >= 0:
                        boj_holding = boj_holdings[idx][1]

                # 市中残存額
                market_amount = cumulative_issuance - boj_holding

                results.append((bond_code, trade_date, market_amount))

        return results

    def bulk_update(self, results: List[Tuple[str, str, int]]) -> int:
        """
        計算結果をRPC関数で一括UPDATE

        Returns:
            更新件数
        """
        # タプルをDictに変換
        updates_list = [
            {'bond_code': bc, 'trade_date': td, 'market_amount': ma}
            for bc, td, ma in results
        ]

        updated = 0
        batch_size = 1000

        # 1000件ずつRPC呼び出し
        for i in range(0, len(updates_list), batch_size):
            batch = updates_list[i:i + batch_size]
            try:
                response = self.supabase.rpc('update_market_amounts', {
                    'updates': batch
                }).execute()

                batch_updated = response.data if response.data else 0
                updated += batch_updated

            except Exception as e:
                logger.error(f"  ❌ RPC更新エラー (バッチ {i//batch_size + 1}): {e}")
                # エラーがあっても続行

        return updated

    def process_chunk(
        self,
        chunk_codes: List[str],
        chunk_num: int,
        total_chunks: int
    ) -> int:
        """
        1チャンク分を処理

        Returns:
            更新件数
        """
        logger.info(f"📦 チャンク {chunk_num}/{total_chunks} 処理中（{len(chunk_codes)}銘柄）")

        # データ取得
        logger.info(f"  - データ取得中...")
        auction_by_bond, boj_by_bond, trades_by_bond = self.fetch_chunk_data(chunk_codes)

        # 計算
        logger.info(f"  - 計算中...")
        results = self.calculate_in_memory(auction_by_bond, boj_by_bond, trades_by_bond)

        # UPDATE
        logger.info(f"  - DB更新中（{len(results)}件）...")
        updated = self.bulk_update(results)

        # メモリ解放
        del auction_by_bond, boj_by_bond, trades_by_bond, results
        gc.collect()

        logger.info(f"  ✅ 完了: {updated}件更新")
        return updated

    def run(self, from_bond_code: Optional[str] = None):
        """
        メイン処理

        Args:
            from_bond_code: 指定した銘柄コードから再開（省略時は最初から）
        """
        start_time = datetime.now()

        logger.info("=" * 70)
        logger.info("市中残存額（market_amount）高速計算 - チャンク処理版")
        logger.info("=" * 70)
        logger.info(f"チャンクサイズ: {self.chunk_size}銘柄")
        logger.info("=" * 70)

        try:
            # Step 1: 全銘柄コード取得
            all_bond_codes = self.get_all_bond_codes()

            # 再開位置を特定
            start_idx = 0
            if from_bond_code:
                try:
                    start_idx = all_bond_codes.index(from_bond_code)
                    logger.info(f"🔄 再開: {from_bond_code} から処理再開")
                except ValueError:
                    logger.warning(f"⚠️  銘柄コード {from_bond_code} が見つかりません。最初から処理します。")

            # チャンクに分割
            total_updated = 0
            total_chunks = (len(all_bond_codes) - start_idx + self.chunk_size - 1) // self.chunk_size

            logger.info(f"📊 処理対象: {len(all_bond_codes) - start_idx}銘柄（{total_chunks}チャンク）")
            logger.info("=" * 70)

            # Step 2: チャンクごとに処理
            for i in range(start_idx, len(all_bond_codes), self.chunk_size):
                chunk_codes = all_bond_codes[i:i+self.chunk_size]
                chunk_num = (i - start_idx) // self.chunk_size + 1

                updated = self.process_chunk(chunk_codes, chunk_num, total_chunks)
                total_updated += updated

                # 進捗表示
                progress = ((i - start_idx + len(chunk_codes)) / (len(all_bond_codes) - start_idx)) * 100
                elapsed = (datetime.now() - start_time).total_seconds()
                logger.info(f"📈 全体進捗: {progress:.1f}% | 累計: {total_updated:,}件 | 経過時間: {elapsed:.0f}秒")
                logger.info("-" * 70)

            # Step 3: 完了
            elapsed = (datetime.now() - start_time).total_seconds()

            logger.info("=" * 70)
            logger.info("✅ 処理完了")
            logger.info("=" * 70)
            logger.info(f"更新レコード数: {total_updated:,}件")
            logger.info(f"処理時間: {elapsed/60:.1f}分 ({elapsed:.0f}秒)")
            logger.info(f"処理速度: {total_updated/elapsed:.0f}件/秒")
            logger.info("=" * 70)

            return total_updated

        except Exception as e:
            logger.error(f"❌ エラー発生: {e}")
            import traceback
            traceback.print_exc()
            raise


def main():
    import argparse
    parser = argparse.ArgumentParser(description='市中残存額の高速計算（チャンク処理版）')
    parser.add_argument('--chunk-size', type=int, default=100, help='チャンクサイズ（デフォルト: 100）')
    parser.add_argument('--from-bond-code', type=str, help='再開する銘柄コード')
    args = parser.parse_args()

    calculator = OptimizedMarketAmountCalculator(chunk_size=args.chunk_size)
    calculator.run(from_bond_code=args.from_bond_code)


if __name__ == '__main__':
    main()
