#!/usr/bin/env python3
"""
金利スワップPDFデータ読取テストスクリプト
JPXのSettlement Rates PDFからデータを抽出するテスト
"""

import sys
from pathlib import Path

try:
    import pdfplumber
except ImportError:
    print("pdfplumber is not installed. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pdfplumber"])
    import pdfplumber


def test_pdf_reading(pdf_path: str):
    """PDFファイルを読み取ってテーブルデータを抽出"""

    print(f"PDFファイルを開いています: {pdf_path}")

    with pdfplumber.open(pdf_path) as pdf:
        print(f"\n総ページ数: {len(pdf.pages)}")

        # 各ページを処理
        for page_num, page in enumerate(pdf.pages, 1):
            print(f"\n{'='*80}")
            print(f"ページ {page_num}/{len(pdf.pages)}")
            print(f"{'='*80}")

            # テキスト抽出
            text = page.extract_text()
            if text:
                print("\n--- テキスト内容 ---")
                print(text[:500])  # 最初の500文字を表示

            # テーブル抽出
            tables = page.extract_tables()
            if tables:
                print(f"\n--- テーブル数: {len(tables)} ---")
                for table_num, table in enumerate(tables, 1):
                    print(f"\nテーブル {table_num}:")
                    print(f"行数: {len(table)}, 列数: {len(table[0]) if table else 0}")

                    # テーブルの最初の10行を表示
                    for i, row in enumerate(table[:10]):
                        print(f"  Row {i}: {row}")

                    if len(table) > 10:
                        print(f"  ... (残り {len(table) - 10} 行)")
            else:
                print("\nテーブルが見つかりませんでした")


if __name__ == "__main__":
    pdf_path = "/tmp/irs_data.pdf"

    if not Path(pdf_path).exists():
        print(f"エラー: PDFファイルが見つかりません: {pdf_path}")
        sys.exit(1)

    test_pdf_reading(pdf_path)
