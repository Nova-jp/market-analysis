#!/usr/bin/env python3
"""
金利スワップPDFデータ構造分析スクリプト
全データを抽出してCSV形式で保存し、データ構造を分析
"""

import sys
from pathlib import Path
import csv
from typing import List, Dict, Any

try:
    import pdfplumber
except ImportError:
    print("pdfplumber is not installed. Please install it first.")
    sys.exit(1)


def parse_irs_data(pdf_path: str) -> List[Dict[str, Any]]:
    """PDFから金利スワップデータを抽出して構造化"""

    all_data = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()

            if not tables:
                continue

            table = tables[0]  # 最初のテーブル

            # 日付を抽出（Row 1の最後の列）
            trade_date = None
            if len(table) > 1 and table[1][-1]:
                trade_date = table[1][-1].strip()

            print(f"取引日: {trade_date}")
            print(f"\nデータ抽出中...")

            # データ行を処理（Row 3以降）
            for row_idx, row in enumerate(table[3:], start=3):
                # JPY OIS (列3-5)
                if row[3] and row[4]:
                    tenor = row[3].strip()
                    rate = row[4].strip()
                    unit = row[5].strip() if row[5] else '%'

                    all_data.append({
                        'trade_date': trade_date,
                        'product_type': 'OIS',
                        'tenor': tenor,
                        'rate': rate,
                        'unit': unit
                    })

                # JPY 3M TIBOR (列7-9)
                if row[7] and row[8]:
                    tenor = row[7].strip()
                    rate = row[8].strip()
                    unit = row[9].strip() if row[9] else '%'

                    all_data.append({
                        'trade_date': trade_date,
                        'product_type': '3M_TIBOR',
                        'tenor': tenor,
                        'rate': rate,
                        'unit': unit
                    })

                # JPY 6M TIBOR (列11-13)
                if row[11] and row[12]:
                    tenor = row[11].strip()
                    rate = row[12].strip()
                    unit = row[13].strip() if row[13] else '%'

                    all_data.append({
                        'trade_date': trade_date,
                        'product_type': '6M_TIBOR',
                        'tenor': tenor,
                        'rate': rate,
                        'unit': unit
                    })

                # JPY 1M TIBOR (列15-17)
                if row[15] and row[16]:
                    tenor = row[15].strip()
                    rate = row[16].strip()
                    unit = row[17].strip() if row[17] else '%'

                    all_data.append({
                        'trade_date': trade_date,
                        'product_type': '1M_TIBOR',
                        'tenor': tenor,
                        'rate': rate,
                        'unit': unit
                    })

    return all_data


def analyze_data_structure(data: List[Dict[str, Any]]):
    """データ構造を分析"""

    print(f"\n{'='*80}")
    print("データ構造分析")
    print(f"{'='*80}\n")

    print(f"総レコード数: {len(data)}")

    # プロダクトタイプごとのカウント
    product_counts = {}
    tenors_by_product = {}
    units_by_product = {}

    for record in data:
        product = record['product_type']
        tenor = record['tenor']
        unit = record['unit']

        product_counts[product] = product_counts.get(product, 0) + 1

        if product not in tenors_by_product:
            tenors_by_product[product] = set()
        tenors_by_product[product].add(tenor)

        if product not in units_by_product:
            units_by_product[product] = set()
        units_by_product[product].add(unit)

    print("\nプロダクトタイプ別統計:")
    for product in sorted(product_counts.keys()):
        print(f"\n  {product}:")
        print(f"    レコード数: {product_counts[product]}")
        print(f"    Tenor種類: {sorted(tenors_by_product[product])}")
        print(f"    単位: {sorted(units_by_product[product])}")

    # サンプルデータ表示
    print(f"\n{'='*80}")
    print("サンプルデータ（各プロダクトタイプから3件）")
    print(f"{'='*80}\n")

    for product in sorted(product_counts.keys()):
        print(f"\n{product}:")
        samples = [d for d in data if d['product_type'] == product][:3]
        for sample in samples:
            print(f"  {sample}")


def save_to_csv(data: List[Dict[str, Any]], output_path: str):
    """データをCSVファイルに保存"""

    if not data:
        print("データが空です")
        return

    fieldnames = ['trade_date', 'product_type', 'tenor', 'rate', 'unit']

    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"\nCSVファイルに保存しました: {output_path}")
    print(f"行数: {len(data) + 1} (ヘッダー含む)")


if __name__ == "__main__":
    pdf_path = "/tmp/irs_data.pdf"
    csv_path = "/tmp/irs_data_sample.csv"

    if not Path(pdf_path).exists():
        print(f"エラー: PDFファイルが見つかりません: {pdf_path}")
        sys.exit(1)

    # データ抽出
    data = parse_irs_data(pdf_path)

    # データ分析
    analyze_data_structure(data)

    # CSV保存
    save_to_csv(data, csv_path)
