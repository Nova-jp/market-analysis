#!/usr/bin/env python3
"""
市中残存額（market_amount）日付ベース計算スクリプト

戦略: シンプルに日付ごとに処理
- 日付を100日ずつバッチ処理
- 各バッチで必要なデータを一括取得
- メモリ内で計算してまとめてUPDATE
"""

import sys
import os
from datetime import datetime
from typing import Dict, List, Set
from collections import defaultdict
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DateBasedMarketAmountCalculator:
    def __init__(self, batch_days: int = 100):
        """
        Args:
            batch_days: 一度に処理する日数（デフォルト100日）
        """
        url = os.getenv('SUPABASE_URL')
        key = os.getenv('SUPABASE_KEY')
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")

        self.supabase = create_client(url, key)
        self.batch_days = batch_days

    def get_all_dates(self) -> List[str]:
        """全取引日を取得（古い順）"""
        logger.info("  📋 全取引日を取得中...")

        dates = set()
        offset = 0
        limit = 1000

        while True:
            try:
                response = self.supabase.table('bond_data') \
                    .select('trade_date') \
                    .range(offset, offset + limit - 1) \
                    .execute()

                if not response.data:
                    break

                for record in response.data:
                    dates.add(record['trade_date'])

                if len(response.data) < limit:
                    break

                offset += limit

                if offset % 10000 == 0:
                    logger.info(f"    進捗: {offset:,}件スキャン済み（ユニーク日数: {len(dates):,}日）")

            except Exception as e:
                logger.error(f"    ❌ エラー（offset={offset}）: {e}")
                break

        sorted_dates = sorted(list(dates))
        logger.info(f"  ✅ 全取引日取得完了: {len(sorted_dates):,}日")
        return sorted_dates

    def get_bond_data_for_dates(self, dates: List[str]) -> List[Dict]:
        """指定日付範囲のbond_dataを取得"""
        logger.info(f"  📥 bond_data取得中（{len(dates)}日分）...")

        all_data = []
        offset = 0
        limit = 1000

        while True:
            response = self.supabase.table('bond_data') \
                .select('bond_code, trade_date') \
                .in_('trade_date', dates) \
                .range(offset, offset + limit - 1) \
                .execute()

            if not response.data:
                break

            all_data.extend(response.data)

            if len(response.data) < limit:
                break

            offset += limit

        logger.info(f"  ✅ bond_data: {len(all_data):,}件取得完了")
        return all_data

    def get_auction_data(self, bond_codes: Set[str], max_date: str) -> List[Dict]:
        """指定銘柄・日付以前のbond_auctionデータを取得"""
        logger.info(f"  📥 bond_auction取得中（{len(bond_codes)}銘柄, ~{max_date}）...")

        all_data = []
        bond_codes_list = sorted(list(bond_codes))

        # 100銘柄ずつチャンクで取得
        chunk_size = 100
        for i in range(0, len(bond_codes_list), chunk_size):
            chunk = bond_codes_list[i:i+chunk_size]
            offset = 0
            limit = 1000

            while True:
                response = self.supabase.table('bond_auction') \
                    .select('bond_code, auction_date, total_amount') \
                    .in_('bond_code', chunk) \
                    .lte('auction_date', max_date) \
                    .range(offset, offset + limit - 1) \
                    .execute()

                if not response.data:
                    break

                all_data.extend(response.data)

                if len(response.data) < limit:
                    break

                offset += limit

            if (i + chunk_size) % 500 == 0:
                logger.info(f"    進捗: {i + chunk_size}/{len(bond_codes_list)}銘柄処理済み")

        logger.info(f"  ✅ bond_auction: {len(all_data):,}件取得完了")
        return all_data

    def get_boj_holdings(self, bond_codes: Set[str], max_date: str) -> List[Dict]:
        """指定銘柄・日付以前のboj_holdingsデータを取得"""
        logger.info(f"  📥 boj_holdings取得中（{len(bond_codes)}銘柄, ~{max_date}）...")

        all_data = []
        bond_codes_list = sorted(list(bond_codes))

        # 100銘柄ずつチャンクで取得
        chunk_size = 100
        for i in range(0, len(bond_codes_list), chunk_size):
            chunk = bond_codes_list[i:i+chunk_size]
            offset = 0
            limit = 1000

            while True:
                response = self.supabase.table('boj_holdings') \
                    .select('bond_code, data_date, face_value') \
                    .in_('bond_code', chunk) \
                    .lte('data_date', max_date) \
                    .range(offset, offset + limit - 1) \
                    .execute()

                if not response.data:
                    break

                all_data.extend(response.data)

                if len(response.data) < limit:
                    break

                offset += limit

            if (i + chunk_size) % 500 == 0:
                logger.info(f"    進捗: {i + chunk_size}/{len(bond_codes_list)}銘柄処理済み")

        logger.info(f"  ✅ boj_holdings: {len(all_data):,}件取得完了")
        return all_data

    def calculate_market_amounts(
        self,
        bond_data: List[Dict],
        auction_data: List[Dict],
        boj_data: List[Dict]
    ) -> List[Dict]:
        """market_amountを計算"""
        logger.info(f"  🧮 market_amount計算中（{len(bond_data):,}件）...")

        # データを整理
        auction_by_bond = defaultdict(list)
        for row in auction_data:
            if row.get('total_amount') is not None:
                auction_by_bond[row['bond_code']].append(
                    (row['auction_date'], int(row['total_amount']))
                )

        # 日付順にソート
        for bond_code in auction_by_bond:
            auction_by_bond[bond_code].sort(key=lambda x: x[0])

        boj_by_bond = defaultdict(list)
        for row in boj_data:
            if row.get('face_value') is not None:
                boj_by_bond[row['bond_code']].append(
                    (row['data_date'], int(row['face_value']))
                )

        # 日付順にソート
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

        logger.info(f"  ✅ 計算完了: {len(results):,}件")
        return results

    def batch_update(self, updates: List[Dict]) -> int:
        """一時テーブルを使った高速一括UPDATE"""
        logger.info(f"  💾 データベース更新中（{len(updates):,}件）...")

        import psycopg2
        from urllib.parse import urlparse

        # Supabase接続情報から直接PostgreSQL接続を作成
        url_parsed = urlparse(os.getenv('SUPABASE_URL'))
        db_host = url_parsed.hostname
        db_name = 'postgres'  # Supabaseのデフォルトデータベース名
        db_user = 'postgres'
        db_password = os.getenv('SUPABASE_KEY')  # Service Role Keyを使用

        # 実際にはSupabase URLから接続情報を抽出する必要がある
        # 今回はSupabaseクライアント経由で実行するため、RPC関数を作成

        # 一時テーブル作成とUPDATE用のRPC関数を呼び出す
        # バッチサイズを1000件ずつに分割
        batch_size = 1000
        total_updated = 0

        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]

            try:
                # JSONB形式でRPC関数に送信
                response = self.supabase.rpc('batch_update_market_amounts', {
                    'updates': batch
                }).execute()

                updated_count = response.data if response.data else 0
                total_updated += updated_count

                if (i + batch_size) % 10000 == 0:
                    logger.info(f"    進捗: {total_updated:,}/{len(updates):,}件更新済み")

            except Exception as e:
                logger.error(f"    ❌ バッチ更新エラー（{i}-{i+len(batch)}）: {e}")
                # エラー時は個別UPDATE試行
                logger.info(f"    個別UPDATEにフォールバック中...")
                for record in batch:
                    try:
                        self.supabase.table('bond_data') \
                            .update({'market_amount': record['market_amount']}) \
                            .eq('bond_code', record['bond_code']) \
                            .eq('trade_date', record['trade_date']) \
                            .execute()
                        total_updated += 1
                    except Exception as e2:
                        logger.error(
                            f"      ❌ {record['bond_code']}, {record['trade_date']}: {e2}"
                        )

        logger.info(f"  ✅ 更新完了: {total_updated:,}件")
        return total_updated

    def run(self):
        """メイン処理"""
        start_time = datetime.now()

        logger.info("=" * 70)
        logger.info("市中残存額（market_amount）日付ベース計算")
        logger.info("=" * 70)
        logger.info(f"  バッチサイズ: {self.batch_days}日/バッチ")
        logger.info("")

        # Step 1: 全取引日取得
        logger.info("=" * 70)
        logger.info("Step 1: 全取引日取得")
        logger.info("=" * 70)
        all_dates = self.get_all_dates()
        total_dates = len(all_dates)

        logger.info("")
        logger.info("=" * 70)
        logger.info(f"Step 2: 日付バッチ処理開始（{total_dates}日 → {self.batch_days}日/バッチ）")
        logger.info("=" * 70)
        logger.info("")

        total_updated = 0
        total_batches = (total_dates + self.batch_days - 1) // self.batch_days

        for i in range(0, total_dates, self.batch_days):
            batch_num = (i // self.batch_days) + 1
            date_batch = all_dates[i:i + self.batch_days]
            min_date = date_batch[0]
            max_date = date_batch[-1]

            logger.info(f"📦 バッチ {batch_num}/{total_batches}: {min_date} ~ {max_date} ({len(date_batch)}日)")
            logger.info("")

            try:
                # Step 2.1: この期間のbond_data取得
                bond_data = self.get_bond_data_for_dates(date_batch)

                # Step 2.2: 必要な銘柄コードを特定
                bond_codes = set(row['bond_code'] for row in bond_data)
                logger.info(f"  📊 対象銘柄数: {len(bond_codes):,}")

                # Step 2.3: auction, boj データ取得
                auction_data = self.get_auction_data(bond_codes, max_date)
                boj_data = self.get_boj_holdings(bond_codes, max_date)

                # Step 2.4: 計算
                updates = self.calculate_market_amounts(bond_data, auction_data, boj_data)

                # Step 2.5: UPDATE
                updated = self.batch_update(updates)
                total_updated += updated

                logger.info(f"  ✅ バッチ {batch_num} 完了: {updated:,}件更新（累計: {total_updated:,}件）")
                logger.info("")

            except Exception as e:
                logger.error(f"  ❌ バッチ {batch_num} エラー: {e}")
                import traceback
                traceback.print_exc()
                logger.info("")
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
    import argparse

    parser = argparse.ArgumentParser(description='market_amount 日付ベース計算')
    parser.add_argument('--batch-days', type=int, default=100,
                        help='バッチサイズ（日数、デフォルト: 100日）')

    args = parser.parse_args()

    calculator = DateBasedMarketAmountCalculator(batch_days=args.batch_days)
    calculator.run()


if __name__ == '__main__':
    main()
