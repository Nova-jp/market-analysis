#!/usr/bin/env python3
"""
trade_date 日付マッピングSQL生成スクリプト

目的:
- 全ユニーク日付の「1営業日前」マッピングを生成
- PostgreSQLで直接実行可能なUPDATE文を生成

出力:
- SQL文ファイル（Supabaseダッシュボードで実行）
"""

import sys
import os
from datetime import date, timedelta
import jpholiday

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from data.utils.database_manager import DatabaseManager

load_dotenv()


def get_previous_business_day(d: date) -> date:
    """1営業日前を取得（土日・祝日スキップ）"""
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5 or jpholiday.is_holiday(prev):
        prev -= timedelta(days=1)
    return prev


def main():
    print("=" * 70)
    print("trade_date マッピングSQL生成")
    print("=" * 70)
    print()

    # Step 1: 全ユニーク日付取得
    print("Step 1: 全ユニーク日付取得中...")
    db = DatabaseManager()
    existing_dates = db.get_all_existing_dates()
    print(f"  ✅ {len(existing_dates)}件取得")
    print()

    # Step 2: マッピング作成
    print("Step 2: 日付マッピング作成中...")
    mapping = []
    for old_date_str in sorted(existing_dates):
        old_date = date.fromisoformat(old_date_str)
        new_date = get_previous_business_day(old_date)
        mapping.append((old_date_str, new_date.isoformat()))

    print(f"  ✅ {len(mapping)}件のマッピング作成完了")
    print()

    # Step 3: SQL生成
    print("Step 3: SQL文生成中...")

    # CASE文を使った一括UPDATE
    sql_lines = [
        "-- ======================================================================",
        "-- trade_date 一括UPDATE SQL",
        "-- ",
        f"-- 対象: {len(mapping)}日分",
        "-- 実行時間: 数秒〜数分",
        "-- ",
        "-- 実行方法:",
        "-- 1. Supabaseダッシュボード → SQL Editor",
        "-- 2. このSQLを貼り付けて実行",
        "-- ======================================================================",
        "",
        "UPDATE bond_data",
        "SET trade_date = CASE trade_date"
    ]

    # CASE文の各行を追加
    for old_date, new_date in mapping:
        sql_lines.append(f"    WHEN '{old_date}'::date THEN '{new_date}'::date")

    sql_lines.extend([
        "    ELSE trade_date",
        "END,",
        "updated_at = NOW();",
        "",
        "-- ======================================================================",
        "-- 検証SQL",
        "-- ======================================================================",
        "",
        "-- 土日データがないことを確認",
        "SELECT DATE_PART('dow', trade_date) as day_of_week, COUNT(*)",
        "FROM bond_data",
        "GROUP BY day_of_week",
        "ORDER BY day_of_week;",
        "-- 期待: 0(日), 6(土) が0件",
        "",
        "-- 最古・最新の日付確認",
        "SELECT MIN(trade_date), MAX(trade_date), COUNT(*)",
        "FROM bond_data;",
    ])

    sql_content = "\n".join(sql_lines)

    # ファイルに保存
    output_file = "scripts/update_trade_dates.sql"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(sql_content)

    print(f"  ✅ SQL文を生成: {output_file}")
    print()
    print("=" * 70)
    print("次のステップ:")
    print("=" * 70)
    print(f"1. {output_file} をSupabaseダッシュボードで実行")
    print("2. 実行後、検証SQLで土日データがないことを確認")
    print("3. 制約を再作成:")
    print("   ALTER TABLE bond_data")
    print("   ADD CONSTRAINT clean_bond_data_trade_date_bond_code_key")
    print("   UNIQUE (trade_date, bond_code);")
    print("=" * 70)

    return len(mapping)


if __name__ == '__main__':
    main()
