#!/usr/bin/env python3
"""
市中残存額（market_amount）超高速計算スクリプト（RPC版）

戦略:
1. 全データをPythonメモリに読み込み（約100MB）
2. メモリ内で全market_amountを計算（O(N)）
3. Supabase RPC関数で一括UPDATE（1000件ずつバッチ）

処理時間: 5-10分（従来の数十時間から大幅短縮）

前提条件:
Supabaseに以下のRPC関数が作成されていること:

CREATE OR REPLACE FUNCTION update_market_amounts(
    updates jsonb
) RETURNS int AS $$
DECLARE
    update_count int := 0;
    rows_affected int;
    item jsonb;
BEGIN
    FOR item IN SELECT * FROM jsonb_array_elements(updates)
    LOOP
        UPDATE bond_data
        SET market_amount = (item->>'market_amount')::bigint
        WHERE bond_code = item->>'bond_code'
          AND trade_date = (item->>'trade_date')::date;

        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        update_count := update_count + rows_affected;
    END LOOP;

    RETURN update_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
"""

import sys
import os
from datetime import datetime
from typing import Dict, List, Tuple
from collections import defaultdict
from bisect import bisect_right
import logging

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


class RPCMarketAmountCalculator:
    """RPC関数を使った超高速市中残存額計算"""

    def __init__(self):
        url = os.getenv('SUPABASE_URL')
        key = os.getenv('SUPABASE_KEY')
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        self.supabase = create_client(url, key)

    def fetch_all_table_data(self, table_name: str, columns: str) -> List[Dict]:
        """テーブルから全データ取得"""
        logger.info(f"  📥 {table_name} データ取得中...")
        all_data = []
        offset = 0
        limit = 1000

        while True:
            response = self.supabase.table(table_name) \
                .select(columns) \
                .range(offset, offset + limit - 1) \
                .execute()

            if not response.data:
                break

            all_data.extend(response.data)

            if len(response.data) < limit:
                break

            offset += limit

            if offset % 10000 == 0:
                logger.info(f"    進捗: {offset:,}件取得済み...")

        logger.info(f"  ✅ {table_name}: {len(all_data):,}件取得完了")
        return all_data

    def calculate_all_market_amounts(self) -> List[Dict]:
        """
        全銘柄・全日付のmarket_amountを計算

        Returns:
            [{'bond_code': str, 'trade_date': str, 'market_amount': int}, ...]
        """
        logger.info("=" * 70)
        logger.info("Step 1: データ取得")
        logger.info("=" * 70)

        # 全データ取得
        auction_data = self.fetch_all_table_data(
            'bond_auction',
            'bond_code, auction_date, allocated_amount'
        )
        boj_data = self.fetch_all_table_data(
            'boj_holdings',
            'bond_code, data_date, face_value'
        )
        trade_data = self.fetch_all_table_data(
            'bond_data',
            'bond_code, trade_date'
        )

        logger.info("")
        logger.info("=" * 70)
        logger.info("Step 2: データ整理")
        logger.info("=" * 70)

        # 銘柄ごとにグループ化
        auction_by_bond = defaultdict(list)
        for row in auction_data:
            auction_by_bond[row['bond_code']].append(
                (row['auction_date'], row.get('allocated_amount', 0) or 0)
            )

        for bond_code in auction_by_bond:
            auction_by_bond[bond_code].sort(key=lambda x: x[0])

        boj_by_bond = defaultdict(list)
        for row in boj_data:
            boj_by_bond[row['bond_code']].append(
                (row['data_date'], row.get('face_value', 0) or 0)
            )

        for bond_code in boj_by_bond:
            boj_by_bond[bond_code].sort(key=lambda x: x[0])

        trades_by_bond = defaultdict(list)
        for row in trade_data:
            trades_by_bond[row['bond_code']].append(row['trade_date'])

        for bond_code in trades_by_bond:
            trades_by_bond[bond_code].sort()

        logger.info(f"  銘柄数: {len(trades_by_bond):,}")
        logger.info(f"  総取引日数: {sum(len(v) for v in trades_by_bond.values()):,}")

        logger.info("")
        logger.info("=" * 70)
        logger.info("Step 3: market_amount計算")
        logger.info("=" * 70)

        results = []
        total_bonds = len(trades_by_bond)
        processed = 0

        for bond_code, trade_dates in trades_by_bond.items():
            auctions = auction_by_bond.get(bond_code, [])
            boj_holdings = boj_by_bond.get(bond_code, [])

            cumulative_issuance = 0
            auction_idx = 0

            for trade_date in trade_dates:
                # その日までの累積発行額
                while auction_idx < len(auctions) and auctions[auction_idx][0] <= trade_date:
                    cumulative_issuance += int(auctions[auction_idx][1])
                    auction_idx += 1

                # その日時点の日銀保有額（二分探索）
                boj_holding = 0
                if boj_holdings:
                    dates = [h[0] for h in boj_holdings]
                    idx = bisect_right(dates, trade_date) - 1
                    if idx >= 0:
                        boj_holding = int(boj_holdings[idx][1])

                # 市中残存額
                market_amount = cumulative_issuance - boj_holding

                results.append({
                    'bond_code': bond_code,
                    'trade_date': trade_date,
                    'market_amount': market_amount
                })

            processed += 1
            if processed % 50 == 0:
                logger.info(f"  進捗: {processed}/{total_bonds} 銘柄 ({len(results):,}件計算済み)")

        logger.info(f"  ✅ 計算完了: {len(results):,}件")
        return results

    def bulk_update_via_rpc(self, results: List[Dict], batch_size: int = 1000) -> int:
        """
        RPC関数を使って一括UPDATE

        Args:
            results: [{'bond_code': str, 'trade_date': str, 'market_amount': int}, ...]
            batch_size: 1回のRPC呼び出しで処理する件数

        Returns:
            更新件数
        """
        logger.info("")
        logger.info("=" * 70)
        logger.info("Step 4: データベース更新（RPC関数使用）")
        logger.info("=" * 70)

        total_updated = 0
        total_batches = (len(results) + batch_size - 1) // batch_size

        for i in range(0, len(results), batch_size):
            batch = results[i:i + batch_size]
            batch_num = i // batch_size + 1

            try:
                # RPC関数呼び出し
                response = self.supabase.rpc('update_market_amounts', {
                    'updates': batch
                }).execute()

                updated_count = response.data if response.data else 0
                total_updated += updated_count

                logger.info(
                    f"  バッチ {batch_num}/{total_batches}: "
                    f"{updated_count:,}件更新 (累計: {total_updated:,}件)"
                )

            except Exception as e:
                logger.error(f"  ❌ バッチ {batch_num} 更新エラー: {e}")
                logger.error(f"     バッチ範囲: {i} - {i + len(batch)}")
                # エラーがあっても続行
                continue

        logger.info(f"  ✅ 全バッチ完了: {total_updated:,}件更新")
        return total_updated

    def run(self):
        """メイン処理"""
        start_time = datetime.now()

        logger.info("=" * 70)
        logger.info("市中残存額（market_amount）超高速計算 - RPC版")
        logger.info("=" * 70)
        logger.info("")

        try:
            # Step 1-3: データ取得 → 整理 → 計算
            results = self.calculate_all_market_amounts()

            # Step 4: RPC関数で一括UPDATE
            updated = self.bulk_update_via_rpc(results)

            # 完了
            elapsed = (datetime.now() - start_time).total_seconds()

            logger.info("")
            logger.info("=" * 70)
            logger.info("✅ 処理完了")
            logger.info("=" * 70)
            logger.info(f"計算レコード数: {len(results):,}件")
            logger.info(f"更新レコード数: {updated:,}件")
            logger.info(f"処理時間: {elapsed/60:.1f}分 ({elapsed:.0f}秒)")
            logger.info(f"処理速度: {updated/elapsed:.0f}件/秒")
            logger.info("=" * 70)

            return updated

        except Exception as e:
            logger.error(f"❌ エラー発生: {e}")
            import traceback
            traceback.print_exc()
            raise


def verify_rpc_function_exists(supabase):
    """RPC関数が存在するか確認"""
    try:
        # 空配列でテスト呼び出し
        response = supabase.rpc('update_market_amounts', {'updates': []}).execute()
        logger.info("✅ RPC関数 'update_market_amounts' が存在します")
        return True
    except Exception as e:
        logger.error("❌ RPC関数 'update_market_amounts' が見つかりません")
        logger.error(f"   エラー: {e}")
        logger.error("")
        logger.error("=" * 70)
        logger.error("Supabaseダッシュボードで以下のSQLを実行してください:")
        logger.error("=" * 70)
        logger.error("""
CREATE OR REPLACE FUNCTION update_market_amounts(
    updates jsonb
) RETURNS int AS $$
DECLARE
    update_count int := 0;
    rows_affected int;
    item jsonb;
BEGIN
    FOR item IN SELECT * FROM jsonb_array_elements(updates)
    LOOP
        UPDATE bond_data
        SET market_amount = (item->>'market_amount')::bigint
        WHERE bond_code = item->>'bond_code'
          AND trade_date = (item->>'trade_date')::date;

        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        update_count := update_count + rows_affected;
    END LOOP;

    RETURN update_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
        """)
        logger.error("=" * 70)
        return False


def main():
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')
    supabase = create_client(url, key)

    # RPC関数の存在確認
    if not verify_rpc_function_exists(supabase):
        logger.error("")
        logger.error("RPC関数を作成してから再実行してください。")
        sys.exit(1)

    # 計算実行
    calculator = RPCMarketAmountCalculator()
    calculator.run()


if __name__ == '__main__':
    main()
