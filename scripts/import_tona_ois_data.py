#!/usr/bin/env python3
"""
TONA OISヒストリカルデータをSupabaseにインポート

Usage:
    source venv/bin/activate
    python scripts/import_tona_ois_data.py
"""

import pandas as pd
import re
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import logging

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data.utils.database_manager import DatabaseManager

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_tenor_from_column(column_name: str) -> str:
    """
    カラム名から期間（tenor）を抽出

    例:
        JPYTOCOIS1D=TRDJ (BID) -> 1D
        JPYTOCOISSW=TRDJ (BID) -> 1W
        JPYTOCOIS3W=TRDJ (BID) -> 3W
        JPYTOCOIS1M=TRDJ (BID) -> 1M
        JPYTOCOIS1Y=TRDJ (BID) -> 1Y
        JPYTOCOIS10Y=TRDJ (BID) -> 10Y
    """
    # 正規表現でtenor部分を抽出
    pattern = r'JPYTOCOIS(\d+[DWMY]|SW|[DWMY])'
    match = re.search(pattern, column_name)

    if match:
        tenor = match.group(1)
        # SW (1週間) を 1W に変換
        if tenor == 'SW':
            return '1W'
        # 数字なしの単位（M, Y など）を処理
        if len(tenor) == 1:
            return f'1{tenor}'
        return tenor

    return None


def parse_date(date_str) -> str:
    """
    日付文字列をYYYY-MM-DD形式に変換

    例:
        2025年12月26日 -> 2025-12-26
    """
    if pd.isna(date_str):
        return None

    # 文字列に変換
    date_str = str(date_str)

    # "年月日"形式をパース
    pattern = r'(\d{4})年(\d{1,2})月(\d{1,2})日'
    match = re.search(pattern, date_str)

    if match:
        year, month, day = match.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    # すでにYYYY-MM-DD形式の場合
    if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
        return date_str

    return None


def read_excel_to_long_format(file_path: str, rate_type: str = 'TONA') -> pd.DataFrame:
    """
    ExcelファイルをLong形式のDataFrameに変換

    Returns:
        pd.DataFrame: カラム [trade_date, rate_type, tenor, rate]
    """
    logger.info(f"読み込み中: {file_path}")

    # Excelファイルを読み込み
    df = pd.read_excel(file_path)

    # 1行目（ヘッダー）と2行目（"終値"というラベル）をスキップ
    # 実際のデータは3行目以降
    df = df.iloc[1:].copy()  # 2行目以降を取得（0-indexedなので1から）

    # 日付カラムをパース
    df['trade_date'] = df['日付'].apply(parse_date)

    # NaNの日付を除外
    df = df[df['trade_date'].notna()].copy()

    # Long形式に変換
    long_data = []

    for _, row in df.iterrows():
        trade_date = row['trade_date']

        # 各金利カラムを処理
        for col in df.columns:
            if col == '日付' or col == 'trade_date':
                continue

            # tenor を抽出
            tenor = extract_tenor_from_column(col)
            if not tenor:
                continue

            # レート値を取得
            rate_value = row[col]

            # NaNまたは"終値"などの文字列をスキップ
            if pd.isna(rate_value) or isinstance(rate_value, str):
                continue

            try:
                rate = float(rate_value)
                long_data.append({
                    'trade_date': trade_date,
                    'rate_type': rate_type,
                    'tenor': tenor,
                    'rate': rate
                })
            except (ValueError, TypeError):
                continue

    result_df = pd.DataFrame(long_data)
    logger.info(f"  -> {len(result_df)} レコード抽出")

    return result_df


def import_data_to_supabase(df: pd.DataFrame, table_name: str = 'irs_raw', batch_size: int = 500):
    """
    DataFrameをSupabaseにインポート
    """
    db = DatabaseManager()

    total_records = len(df)
    logger.info(f"\n{'='*60}")
    logger.info(f"テーブル: {table_name}")
    logger.info(f"総レコード数: {total_records:,}")
    logger.info(f"{'='*60}\n")

    # バッチ処理
    success_count = 0
    error_count = 0

    for i in range(0, total_records, batch_size):
        batch_df = df.iloc[i:i+batch_size]
        batch_records = batch_df.to_dict('records')

        try:
            # Supabase REST APIでバッチ挿入
            import requests
            response = requests.post(
                f'{db.supabase_url}/rest/v1/{table_name}',
                headers=db.headers,
                json=batch_records,
                timeout=60
            )

            if response.status_code in [201, 200]:
                success_count += len(batch_records)
                logger.info(f"✓ バッチ {i//batch_size + 1}: {len(batch_records)} レコード挿入成功")
            else:
                error_count += len(batch_records)
                logger.error(f"✗ バッチ {i//batch_size + 1}: エラー {response.status_code} - {response.text}")

        except Exception as e:
            error_count += len(batch_records)
            logger.error(f"✗ バッチ {i//batch_size + 1}: 例外 - {e}")

    logger.info(f"\n{'='*60}")
    logger.info(f"インポート完了")
    logger.info(f"成功: {success_count:,} / 失敗: {error_count:,}")
    logger.info(f"{'='*60}\n")


def main():
    """
    メイン処理
    """
    logger.info("TONA OISヒストリカルデータインポート開始\n")

    # Excelファイルのパス
    excel_files = [
        'external_data/tn-3w.xlsx',
        'external_data/1m-11m.xlsx',
        'external_data/1-9.xlsx',
        'external_data/10-40 (1).xlsx'
    ]

    # 全データを統合
    all_data = []

    for file_path in excel_files:
        if not os.path.exists(file_path):
            logger.warning(f"⚠️  ファイルが見つかりません: {file_path}")
            continue

        df = read_excel_to_long_format(file_path, rate_type='TONA')
        all_data.append(df)

    # 全データを結合
    combined_df = pd.concat(all_data, ignore_index=True)

    logger.info(f"\n統合後の総レコード数: {len(combined_df):,}")

    # 重複を削除（同一日付・tenor・rate_typeの組み合わせ）
    combined_df = combined_df.drop_duplicates(subset=['trade_date', 'rate_type', 'tenor'])

    logger.info(f"重複削除後: {len(combined_df):,}")

    # 日付でソート
    combined_df = combined_df.sort_values('trade_date')

    # Supabaseにインポート
    import_data_to_supabase(combined_df, table_name='irs_data', batch_size=500)

    logger.info("✓ すべての処理が完了しました")


if __name__ == '__main__':
    main()
