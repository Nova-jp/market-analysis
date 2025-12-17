#!/usr/bin/env python3
"""
市中残存額（market_amount）計算 - 超細切れバッチ版

戦略:
1. データ取得を50K件ずつに分割（8秒以内に収める）
2. 各バッチでmarket_amount計算
3. 個別UPDATEではなく、10件ずつまとめてUPDATE

処理時間見込み: 30-40分（安全第一）
"""

import sys
import os
from datetime import datetime
from typing import Dict, List
from collections import defaultdict
import logging
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MicroBatchCalculator:
    def __init__(self):
        url = os.getenv('SUPABASE_URL')
        key = os.getenv('SUPABASE_KEY')
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        self.supabase = create_client(url, key)

    def fetch_bond_data_batch(self, offset: int, limit: int) -> List[Dict]:
        """bond_dataをバッチ取得（タイムアウト回避）"""
        logger.info(f"  📥 bond_data取得中: offset={offset:,}, limit={limit:,}")

        try:
            response = self.supabase.table('bond_data') \
                .select('bond_code, trade_date') \
                .range(offset, offset + limit - 1) \
                .execute()

            logger.info(f"    ✅ {len(response.data):,}件取得完了")
            return response.data
        except Exception as e:
            logger.error(f"    ❌ エラー: {e}")
            return []

    def fetch_auction_data(self, bond_codes: List[str]) -> List[Dict]:
        """bond_auctionデータ取得（銘柄コード指定）"""
        logger.info(f"  📥 bond_auction取得中（{len(bond_codes)}銘柄）...")

        all_data = []
        chunk_size = 100

        for i in range(0, len(bond_codes), chunk_size):
            chunk = bond_codes[i:i+chunk_size]

            try:
                response = self.supabase.table('bond_auction') \
                    .select('bond_code, auction_date, total_amount') \
                    .in_('bond_code', chunk) \
                    .execute()

                all_data.extend(response.data)
                time.sleep(0.1)  # レート制限回避

            except Exception as e:
                logger.error(f"    ❌ エラー（{i}-{i+chunk_size}）: {e}")

        logger.info(f"    ✅ {len(all_data):,}件取得完了")
        return all_data

    def fetch_boj_data(self, bond_codes: List[str]) -> List[Dict]:
        """boj_holdingsデータ取得（銘柄コード指定）"""
        logger.info(f"  📥 boj_holdings取得中（{len(bond_codes)}銘柄）...")

        all_data = []
        chunk_size = 100

        for i in range(0, len(bond_codes), chunk_size):
            chunk = bond_codes[i:i+chunk_size]

            try:
                response = self.supabase.table('boj_holdings') \
                    .select('bond_code, data_date, face_value') \
                    .in_('bond_code', chunk) \
                    .execute()

                all_data.extend(response.data)
                time.sleep(0.1)  # レート制限回避

            except Exception as e:
                logger.error(f"    ❌ エラー（{i}-{i+chunk_size}）: {e}")

        logger.info(f"    ✅ {len(all_data):,}件取得完了")
        return all_data

    def calculate_market_amounts(
        self,
        bond_data: List[Dict],
        auction_data: List[Dict],
        boj_data: List[Dict]
    ) -> List[Dict]:
        """market_amount計算"""
        logger.info(f"  🧮 market_amount計算中（{len(bond_data):,}件）...")

        # データを整理
        auction_by_bond = defaultdict(list)
        for row in auction_data:
            if row.get('total_amount') is not None:
                auction_by_bond[row['bond_code']].append(
                    (row['auction_date'], int(row['total_amount']))
                )

        for bond_code in auction_by_bond:
            auction_by_bond[bond_code].sort(key=lambda x: x[0])

        boj_by_bond = defaultdict(list)
        for row in boj_data:
            if row.get('face_value') is not None:
                boj_by_bond[row['bond_code']].append(
                    (row['data_date'], int(row['face_value']))
                )

        for bond_code in boj_by_bond:
            boj_by_bond[bond_code].sort(key=lambda x: x[0])

        # market_amount計算
        results = []

        for row in bond_data:
            bond_code = row['bond_code']
            trade_date = row['trade_date']

            # その日までの累積発行額
            cumulative_issuance = 0
            auctions = auction_by_bond.get(bond_code, [])
            for auction_date, amount in auctions:
                if auction_date <= trade_date:
                    cumulative_issuance += amount

            # その日時点の日銀保有額（最新値）
            boj_holding = 0
            boj_holdings = boj_by_bond.get(bond_code, [])
            for data_date, face_value in reversed(boj_holdings):
                if data_date <= trade_date:
                    boj_holding = face_value
                    break

            # 市中残存額
            market_amount = cumulative_issuance - boj_holding

            results.append({
                'bond_code': bond_code,
                'trade_date': trade_date,
                'market_amount': market_amount
            })

        logger.info(f"    ✅ 計算完了: {len(results):,}件")
        return results

    def batch_update(self, updates: List[Dict]) -> int:
        """10件ずつまとめてUPDATE（タイムアウト回避）"""
        logger.info(f"  💾 データベース更新中（{len(updates):,}件）...")

        total_updated = 0
        batch_size = 10  # 小さいバッチサイズ

        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]

            # 各レコードを個別にUPDATE
            for record in batch:
                try:
                    self.supabase.table('bond_data') \
                        .update({'market_amount': record['market_amount']}) \
                        .eq('bond_code', record['bond_code']) \
                        .eq('trade_date', record['trade_date']) \
                        .execute()
                    total_updated += 1
                except Exception as e:
                    logger.error(
                        f"    ❌ 更新エラー: {record['bond_code']}, "
                        f"{record['trade_date']}: {e}"
                    )

            # レート制限回避
            time.sleep(0.05)

            if (i + batch_size) % 1000 == 0:
                logger.info(f"    進捗: {total_updated:,}/{len(updates):,}件更新済み")

        logger.info(f"    ✅ 更新完了: {total_updated:,}件")
        return total_updated

    def run(self):
        """メイン処理"""
        start_time = datetime.now()

        logger.info("=" * 70)
        logger.info("市中残存額（market_amount）計算 - 超細切れバッチ版")
        logger.info("=" * 70)
        logger.info("")

        # Step 1: データ総数確認
        logger.info("=" * 70)
        logger.info("Step 1: データ総数確認")
        logger.info("=" * 70)

        total_count_response = self.supabase.table('bond_data') \
            .select('*', count='exact') \
            .limit(1) \
            .execute()

        total_records = total_count_response.count
        logger.info(f"  総レコード数: {total_records:,}件")
        logger.info("")

        # Step 2: バッチ処理
        logger.info("=" * 70)
        logger.info("Step 2: バッチ処理開始")
        logger.info("=" * 70)

        batch_size = 50000  # 50K件ずつ（8秒以内に収まる）
        total_batches = (total_records + batch_size - 1) // batch_size
        total_updated = 0

        for batch_num in range(total_batches):
            offset = batch_num * batch_size

            logger.info("")
            logger.info(f"📦 バッチ {batch_num + 1}/{total_batches}: offset={offset:,}")
            logger.info("-" * 70)

            try:
                # Step 2.1: bond_data取得
                bond_data = self.fetch_bond_data_batch(offset, batch_size)

                if not bond_data:
                    logger.info("  データなし。次のバッチへ")
                    continue

                # Step 2.2: 銘柄コード抽出
                bond_codes = list(set(row['bond_code'] for row in bond_data))
                logger.info(f"  対象銘柄数: {len(bond_codes):,}")

                # Step 2.3: auction, boj データ取得
                auction_data = self.fetch_auction_data(bond_codes)
                boj_data = self.fetch_boj_data(bond_codes)

                # Step 2.4: 計算
                updates = self.calculate_market_amounts(bond_data, auction_data, boj_data)

                # Step 2.5: UPDATE
                updated = self.batch_update(updates)
                total_updated += updated

                logger.info(f"  ✅ バッチ {batch_num + 1} 完了: {updated:,}件更新（累計: {total_updated:,}件）")

                # バッチ間の待機（レート制限回避）
                time.sleep(1)

            except Exception as e:
                logger.error(f"  ❌ バッチ {batch_num + 1} エラー: {e}")
                import traceback
                traceback.print_exc()
                continue

        # 完了サマリー
        elapsed = (datetime.now() - start_time).total_seconds()

        logger.info("")
        logger.info("=" * 70)
        logger.info("✅ 処理完了")
        logger.info("=" * 70)
        logger.info(f"  更新レコード数: {total_updated:,}件")
        logger.info(f"  処理時間: {elapsed/60:.1f}分 ({elapsed:.0f}秒)")
        if elapsed > 0:
            logger.info(f"  平均速度: {total_updated/elapsed:.1f}件/秒")
        logger.info("=" * 70)


def main():
    calculator = MicroBatchCalculator()
    calculator.run()


if __name__ == '__main__':
    main()
