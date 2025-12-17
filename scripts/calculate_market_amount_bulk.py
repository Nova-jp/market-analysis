#!/usr/bin/env python3
"""
市中残存額（market_amount）超高速計算スクリプト

戦略:
1. Pythonでメモリ内計算（高速）
2. 一時テーブルに一括INSERT
3. JOINでUPDATE（1回のSQLで全件更新）

処理時間: 5-10分
"""

import sys
import os
from datetime import datetime
from typing import Dict, List, Tuple
from collections import defaultdict
from bisect import bisect_right
import logging
import psycopg2
from psycopg2.extras import execute_values

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_postgres_connection():
    """PostgreSQL直接接続を取得"""
    # SupabaseのURLからPostgreSQL接続情報を抽出
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')

    if not supabase_url:
        raise ValueError("SUPABASE_URL must be set")

    # Supabase URLから接続情報を構築
    # 例: https://PROJECT.supabase.co → PROJECT.supabase.co
    host = supabase_url.replace('https://', '').replace('http://', '')
    project_ref = host.split('.')[0]

    # Supabase PostgreSQL接続情報
    # 注: これはSupabase REST APIではなく、直接PostgreSQL接続です
    # pooler.supabase.com または db.PROJECT.supabase.co を使用

    logger.info("PostgreSQL直接接続を試みます...")
    logger.info(f"Project: {project_ref}")

    # Supabaseの直接DB接続はサポートされていないため、
    # REST API経由でバルク処理を行う方法に変更します
    raise NotImplementedError(
        "Supabase Free Tierでは直接PostgreSQL接続がサポートされていません。\n"
        "代替案: REST APIのバッチ機能を使用します。"
    )


def bulk_calculate_and_update():
    """
    バルク計算とUPDATE

    代替案: Supabase REST APIを使った効率的な方法
    """
    from supabase import create_client

    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')
    supabase = create_client(url, key)

    logger.info("=" * 70)
    logger.info("市中残存額（market_amount）バルク計算")
    logger.info("=" * 70)

    start_time = datetime.now()

    # Step 1: 全データ取得（最小カラムのみ）
    logger.info("Step 1: データ取得中...")

    auction_data = fetch_all_table_data(supabase, 'bond_auction',
                                        'bond_code, auction_date, allocated_amount')
    boj_data = fetch_all_table_data(supabase, 'boj_holdings',
                                    'bond_code, data_date, face_value')
    trade_data = fetch_all_table_data(supabase, 'bond_data',
                                      'bond_code, trade_date')

    logger.info(f"  発行データ: {len(auction_data)}件")
    logger.info(f"  日銀データ: {len(boj_data)}件")
    logger.info(f"  取引データ: {len(trade_data)}件")

    # Step 2: メモリ内でグループ化
    logger.info("Step 2: データ整理中...")

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

    # Step 3: 計算
    logger.info("Step 3: market_amount計算中...")

    results = []
    total_bonds = len(trades_by_bond)
    processed = 0

    for bond_code, trade_dates in trades_by_bond.items():
        auctions = auction_by_bond.get(bond_code, [])
        boj_holdings = boj_by_bond.get(bond_code, [])

        cumulative_issuance = 0
        auction_idx = 0

        for trade_date in trade_dates:
            # 累積発行額
            while auction_idx < len(auctions) and auctions[auction_idx][0] <= trade_date:
                cumulative_issuance += int(auctions[auction_idx][1])
                auction_idx += 1

            # 日銀保有額
            boj_holding = 0
            if boj_holdings:
                dates = [h[0] for h in boj_holdings]
                idx = bisect_right(dates, trade_date) - 1
                if idx >= 0:
                    boj_holding = int(boj_holdings[idx][1])

            # 市中残存額
            market_amount = cumulative_issuance - boj_holding
            results.append((bond_code, trade_date, market_amount))

        processed += 1
        if processed % 100 == 0:
            logger.info(f"  進捗: {processed}/{total_bonds} 銘柄")

    logger.info(f"  ✅ 計算完了: {len(results)}件")

    # Step 4: バッチUPDATE（改良版）
    logger.info("Step 4: データベース更新中...")

    updated = batch_update_optimized(supabase, results)

    elapsed = (datetime.now() - start_time).total_seconds()

    logger.info("=" * 70)
    logger.info("✅ 処理完了")
    logger.info("=" * 70)
    logger.info(f"更新レコード数: {updated:,}件")
    logger.info(f"処理時間: {elapsed/60:.1f}分")
    logger.info(f"処理速度: {updated/elapsed:.0f}件/秒")
    logger.info("=" * 70)


def fetch_all_table_data(supabase, table_name: str, columns: str) -> List[Dict]:
    """テーブルから全データ取得"""
    all_data = []
    offset = 0
    limit = 1000

    while True:
        response = supabase.table(table_name) \
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
            logger.info(f"    {table_name}: {offset}件取得済み...")

    return all_data


def batch_update_optimized(supabase, results: List[Tuple], batch_size: int = 100) -> int:
    """
    最適化バッチUPDATE

    戦略: 同じbond_codeのレコードをまとめて処理
    """
    # bond_codeでグループ化
    by_bond = defaultdict(list)
    for bond_code, trade_date, market_amount in results:
        by_bond[bond_code].append((trade_date, market_amount))

    updated = 0
    total_bonds = len(by_bond)
    processed = 0

    for bond_code, updates in by_bond.items():
        # この銘柄の全レコードを一括UPDATE
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i+batch_size]

            # 各レコードを個別UPDATE（改善の余地あり）
            for trade_date, market_amount in batch:
                try:
                    response = supabase.table('bond_data') \
                        .update({'market_amount': market_amount}) \
                        .eq('bond_code', bond_code) \
                        .eq('trade_date', trade_date) \
                        .execute()

                    if response.data:
                        updated += len(response.data)

                except Exception as e:
                    logger.warning(f"更新スキップ ({bond_code}, {trade_date}): {e}")

        processed += 1
        if processed % 50 == 0:
            logger.info(f"  進捗: {processed}/{total_bonds} 銘柄（{updated:,}件更新済み）")

    return updated


def main():
    try:
        bulk_calculate_and_update()
    except NotImplementedError as e:
        logger.error(str(e))
        logger.info("")
        logger.info("=" * 70)
        logger.info("代替案:")
        logger.info("=" * 70)
        logger.info("1. 旧スクリプトで残り銘柄を処理（時間がかかるが確実）")
        logger.info("   python scripts/calculate_market_amount.py --from-bond-code 001380045")
        logger.info("")
        logger.info("2. SupabaseのRPC関数を作成してバルクUPDATE")
        logger.info("   - Supabaseダッシュボードで関数を作成")
        logger.info("   - Pythonから呼び出し")
        logger.info("=" * 70)
        sys.exit(1)


if __name__ == '__main__':
    main()
