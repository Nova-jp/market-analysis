#!/usr/bin/env python3
"""
market_amountをバッチでNULLにリセット

理由: 180万件を一度にUPDATEするとタイムアウトするため
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

def main():
    print("=" * 70)
    print("market_amount バッチリセット - 開始")
    print("=" * 70)
    print()

    supabase = create_client(
        os.getenv('SUPABASE_URL'),
        os.getenv('SUPABASE_KEY')
    )

    start_time = datetime.now()

    # Step 1: 全ユニーク日付を取得（DISTINCTでページネーション不要）
    print("Step 1: 全ユニーク日付を取得中...")

    # DatabaseManagerを使用
    from data.utils.database_manager import DatabaseManager
    db = DatabaseManager()
    unique_dates = sorted(db.get_all_existing_dates())

    print(f"  ✅ {len(unique_dates)}日分の日付取得完了")
    print()

    # Step 2: 日付ごとにリセット
    print("Step 2: 日付ごとにmarket_amountをNULLにリセット中...")
    print(f"  総日付数: {len(unique_dates)}")
    print()

    total_updated = 0

    for i, trade_date in enumerate(unique_dates):
        try:
            # 各日付のmarket_amountをNULLに
            supabase.table('bond_data') \
                .update({'market_amount': None}) \
                .eq('trade_date', trade_date) \
                .execute()

            total_updated += 1

            # 進捗表示
            if (i + 1) % 100 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                speed = total_updated / elapsed if elapsed > 0 else 0
                remaining = (len(unique_dates) - total_updated) / speed if speed > 0 else 0
                print(f"  進捗: {i + 1}/{len(unique_dates)} 日付処理済み "
                      f"({speed:.1f}日/秒, 残り約{remaining/60:.1f}分)")

        except Exception as e:
            print(f"  ❌ エラー: {trade_date} - {e}")

    elapsed = (datetime.now() - start_time).total_seconds()

    print()
    print("=" * 70)
    print("✅ リセット完了")
    print("=" * 70)
    print(f"処理日付数: {total_updated}日分")
    print(f"処理時間: {elapsed/60:.1f}分 ({elapsed:.0f}秒)")
    print("=" * 70)

if __name__ == '__main__':
    main()
