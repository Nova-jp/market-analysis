#!/usr/bin/env python3
"""
市中残存額（market_amount）計算 - 超シンプル版

戦略:
1. 全データを一度にメモリに読み込む
2. Python でmarket_amount計算
3. UPDATE SQL文を生成してファイルに書き出し
4. psql で実行

処理時間見込み: 15-20分（計算5分 + SQL実行10-15分）
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


class SimpleMarketAmountCalculator:
    def __init__(self):
        url = os.getenv('SUPABASE_URL')
        key = os.getenv('SUPABASE_KEY')
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        self.supabase = create_client(url, key)

    def fetch_all_data(self, table: str, columns: str) -> List[Dict]:
        """テーブル全データ取得"""
        logger.info(f"  📥 {table} 取得中...")
        all_data = []
        offset = 0
        limit = 1000

        while True:
            response = self.supabase.table(table) \
                .select(columns) \
                .range(offset, offset + limit - 1) \
                .execute()

            if not response.data:
                break

            all_data.extend(response.data)

            if len(response.data) < limit:
                break

            offset += limit

            if offset % 100000 == 0:
                logger.info(f"    進捗: {offset:,}件...")

        logger.info(f"  ✅ {table}: {len(all_data):,}件取得完了")
        return all_data

    def calculate_all(self) -> List[Dict]:
        """全market_amount計算"""
        logger.info("=" * 70)
        logger.info("Step 1: データ取得")
        logger.info("=" * 70)

        # 全データ取得
        bond_data = self.fetch_all_data('bond_data', 'bond_code, trade_date')
        auction_data = self.fetch_all_data('bond_auction', 'bond_code, auction_date, total_amount')
        boj_data = self.fetch_all_data('boj_holdings', 'bond_code, data_date, face_value')

        logger.info("")
        logger.info("=" * 70)
        logger.info("Step 2: データ整理")
        logger.info("=" * 70)

        # 銘柄ごとにグループ化
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

        logger.info(f"  銘柄数（発行データ）: {len(auction_by_bond):,}")
        logger.info(f"  銘柄数（日銀保有）: {len(boj_by_bond):,}")
        logger.info(f"  取引レコード数: {len(bond_data):,}")

        logger.info("")
        logger.info("=" * 70)
        logger.info("Step 3: market_amount計算")
        logger.info("=" * 70)

        results = []
        processed = 0

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

            processed += 1
            if processed % 100000 == 0:
                logger.info(f"  進捗: {processed:,}/{len(bond_data):,}件計算済み")

        logger.info(f"  ✅ 計算完了: {len(results):,}件")
        return results

    def generate_update_sql(self, results: List[Dict], output_file: str):
        """UPDATE SQL生成"""
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"Step 4: UPDATE SQL生成（{output_file}）")
        logger.info("=" * 70)

        with open(output_file, 'w') as f:
            f.write("-- market_amount 一括UPDATE\n")
            f.write("-- 生成日時: {}\n".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            f.write("-- 更新レコード数: {:,}件\n\n".format(len(results)))

            # 一時テーブル作成
            f.write("-- 一時テーブル作成\n")
            f.write("CREATE TEMP TABLE temp_market_amounts (\n")
            f.write("    bond_code TEXT,\n")
            f.write("    trade_date DATE,\n")
            f.write("    market_amount BIGINT\n")
            f.write(");\n\n")

            # データ挿入（1000件ずつ）
            f.write("-- データ挿入\n")
            batch_size = 1000
            for i in range(0, len(results), batch_size):
                batch = results[i:i + batch_size]

                f.write("INSERT INTO temp_market_amounts (bond_code, trade_date, market_amount) VALUES\n")

                values = []
                for row in batch:
                    values.append(
                        f"('{row['bond_code']}', '{row['trade_date']}', {row['market_amount']})"
                    )

                f.write(",\n".join(values))
                f.write(";\n\n")

                if (i + batch_size) % 100000 == 0:
                    logger.info(f"  進捗: {i + batch_size:,}/{len(results):,}行書き込み済み")

            # 一括UPDATE
            f.write("-- 一括UPDATE\n")
            f.write("UPDATE bond_data\n")
            f.write("SET market_amount = temp_market_amounts.market_amount\n")
            f.write("FROM temp_market_amounts\n")
            f.write("WHERE bond_data.bond_code = temp_market_amounts.bond_code\n")
            f.write("  AND bond_data.trade_date = temp_market_amounts.trade_date;\n\n")

            # クリーンアップ
            f.write("-- クリーンアップ\n")
            f.write("DROP TABLE temp_market_amounts;\n")

        logger.info(f"  ✅ SQL生成完了: {output_file}")
        logger.info(f"  ファイルサイズ: {os.path.getsize(output_file) / 1024 / 1024:.1f} MB")

    def run(self):
        """メイン処理"""
        start_time = datetime.now()

        logger.info("=" * 70)
        logger.info("市中残存額（market_amount）計算 - 超シンプル版")
        logger.info("=" * 70)
        logger.info("")

        # Step 1-3: データ取得 → 整理 → 計算
        results = self.calculate_all()

        # Step 4: SQL生成
        output_file = '/tmp/update_market_amounts.sql'
        self.generate_update_sql(results, output_file)

        # 完了
        elapsed = (datetime.now() - start_time).total_seconds()

        logger.info("")
        logger.info("=" * 70)
        logger.info("✅ 処理完了")
        logger.info("=" * 70)
        logger.info(f"  計算レコード数: {len(results):,}件")
        logger.info(f"  処理時間: {elapsed/60:.1f}分 ({elapsed:.0f}秒)")
        logger.info("")
        logger.info("次のステップ:")
        logger.info(f"  1. 生成されたSQLファイルを確認: {output_file}")
        logger.info(f"  2. Supabase SQL Editorで実行")
        logger.info("=" * 70)


def main():
    calculator = SimpleMarketAmountCalculator()
    calculator.run()


if __name__ == '__main__':
    main()
