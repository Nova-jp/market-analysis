#!/usr/bin/env python3
"""
金利スワップデータ収集スクリプト
JPXのPDFから金利スワップ清算値段を取得してSupabaseに保存
"""

import sys
from pathlib import Path
from datetime import datetime, date
import tempfile
import requests
from typing import List, Dict, Any, Optional
import logging

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    import pdfplumber
except ImportError:
    print("pdfplumber is not installed. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pdfplumber"])
    import pdfplumber

from data.utils.database_manager import DatabaseManager

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IRSDataCollector:
    """金利スワップデータ収集クラス"""

    # JPX IRS ページURL
    JPX_IRS_PAGE_URL = "https://www.jpx.co.jp/jscc/toukei_irs.html"
    JPX_BASE_URL = "https://www.jpx.co.jp"

    def __init__(self):
        self.db_manager = DatabaseManager()

    def get_latest_pdf_url(self, target_date: Optional[date] = None) -> str:
        """最新のPDF URLを取得"""

        if target_date is None:
            target_date = date.today()

        # PDF URLパターン: /jscc/cimhll0000000umu-att/SettlementRates_YYYYMMDD.pdf
        date_str = target_date.strftime("%Y%m%d")
        pdf_filename = f"SettlementRates_{date_str}.pdf"
        pdf_url = f"{self.JPX_BASE_URL}/jscc/cimhll0000000umu-att/{pdf_filename}"

        logger.info(f"PDF URL: {pdf_url}")
        return pdf_url

    def download_pdf(self, pdf_url: str) -> Path:
        """PDFをダウンロード"""

        logger.info(f"PDFダウンロード中: {pdf_url}")

        try:
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()

            # 一時ファイルに保存
            temp_file = tempfile.NamedTemporaryFile(
                delete=False, suffix='.pdf', prefix='irs_settlement_'
            )
            temp_file.write(response.content)
            temp_file.close()

            logger.info(f"PDFダウンロード完了: {temp_file.name}")
            return Path(temp_file.name)

        except requests.exceptions.RequestException as e:
            logger.error(f"PDFダウンロード失敗: {e}")
            raise

    def parse_pdf_data(self, pdf_path: Path, fallback_date: Optional[date] = None) -> tuple[List[Dict[str, Any]], date]:
        """
        PDFからデータを抽出

        Returns:
            tuple: (データリスト, 取引日)
        """

        logger.info(f"PDFデータ抽出中: {pdf_path}")

        all_data = []
        trade_date = fallback_date

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()

                if not tables:
                    logger.warning(f"ページにテーブルが見つかりません")
                    continue

                table = tables[0]  # 最初のテーブル

                # PDFから取引日を抽出（Row 1の最後の列）
                if not trade_date and len(table) > 1:
                    date_cell = table[1][-1]  # Row 1, 最後の列
                    if date_cell and date_cell.strip():
                        try:
                            # 日付形式: YYYY/MM/DD
                            trade_date = datetime.strptime(date_cell.strip(), '%Y/%m/%d').date()
                            logger.info(f"PDF内部から取引日を取得: {trade_date}")
                        except ValueError:
                            logger.warning(f"日付の解析に失敗: {date_cell}")

                if not trade_date:
                    if fallback_date:
                        trade_date = fallback_date
                        logger.warning(f"フォールバック日付を使用: {trade_date}")
                    else:
                        raise ValueError("取引日を取得できませんでした")

                # データ行を処理（Row 3以降）
                for row_idx, row in enumerate(table[3:], start=3):
                    # JPY OIS (列3-5)
                    if row[3] and row[4]:
                        tenor = row[3].strip()
                        rate = row[4].strip()
                        unit = row[5].strip() if row[5] else '%'

                        all_data.append({
                            'trade_date': trade_date.isoformat(),
                            'product_type': 'OIS',
                            'tenor': tenor,
                            'rate': float(rate),
                            'unit': unit
                        })

                    # JPY 3M TIBOR (列7-9)
                    if row[7] and row[8]:
                        tenor = row[7].strip()
                        rate = row[8].strip()
                        unit = row[9].strip() if row[9] else '%'

                        all_data.append({
                            'trade_date': trade_date.isoformat(),
                            'product_type': '3M_TIBOR',
                            'tenor': tenor,
                            'rate': float(rate),
                            'unit': unit
                        })

                    # JPY 6M TIBOR (列11-13)
                    if row[11] and row[12]:
                        tenor = row[11].strip()
                        rate = row[12].strip()
                        unit = row[13].strip() if row[13] else '%'

                        all_data.append({
                            'trade_date': trade_date.isoformat(),
                            'product_type': '6M_TIBOR',
                            'tenor': tenor,
                            'rate': float(rate),
                            'unit': unit
                        })

                    # JPY 1M TIBOR (列15-17)
                    if row[15] and row[16]:
                        tenor = row[15].strip()
                        rate = row[16].strip()
                        unit = row[17].strip() if row[17] else '%'

                        all_data.append({
                            'trade_date': trade_date.isoformat(),
                            'product_type': '1M_TIBOR',
                            'tenor': tenor,
                            'rate': float(rate),
                            'unit': unit
                        })

        logger.info(f"抽出完了: {len(all_data)}件のレコード (取引日: {trade_date})")
        return all_data, trade_date

    def save_to_database(self, data: List[Dict[str, Any]]) -> int:
        """データをデータベースに保存"""

        if not data:
            logger.warning("保存するデータがありません")
            return 0

        logger.info(f"データベースに保存中: {len(data)}件")

        try:
            # Supabase REST APIでデータを挿入（upsert）
            url = f"{self.db_manager.supabase_url}/rest/v1/irs_settlement_rates"
            headers = self.db_manager.headers.copy()
            headers['Prefer'] = 'resolution=merge-duplicates'

            response = requests.post(url, json=data, headers=headers)
            response.raise_for_status()

            logger.info(f"データベース保存完了: {len(data)}件")
            return len(data)

        except requests.exceptions.RequestException as e:
            logger.error(f"データベース保存失敗: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"レスポンス: {e.response.text}")
            raise

    def collect(self, target_date: Optional[date] = None) -> int:
        """データ収集メイン処理"""

        if target_date is None:
            target_date = date.today()

        logger.info(f"{'='*80}")
        logger.info(f"金利スワップデータ収集開始")
        logger.info(f"対象日: {target_date}")
        logger.info(f"{'='*80}")

        try:
            # 1. PDF URLを取得
            pdf_url = self.get_latest_pdf_url(target_date)

            # 2. PDFをダウンロード
            pdf_path = self.download_pdf(pdf_url)

            # 3. PDFからデータを抽出（PDF内部から取引日も取得）
            data, actual_trade_date = self.parse_pdf_data(pdf_path, target_date)

            logger.info(f"取引日: {actual_trade_date} (指定日: {target_date})")

            # 4. データベースに保存
            saved_count = self.save_to_database(data)

            # 5. 一時ファイルを削除
            pdf_path.unlink()
            logger.info(f"一時ファイル削除: {pdf_path}")

            logger.info(f"{'='*80}")
            logger.info(f"データ収集完了: {saved_count}件")
            logger.info(f"{'='*80}")

            return saved_count

        except Exception as e:
            logger.error(f"データ収集失敗: {e}")
            raise


def main():
    """メイン関数"""

    import argparse

    parser = argparse.ArgumentParser(description='金利スワップデータ収集')
    parser.add_argument(
        '--date',
        type=str,
        help='対象日 (YYYY-MM-DD形式)。指定しない場合は今日'
    )

    args = parser.parse_args()

    # 対象日を解析
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print(f"エラー: 日付形式が不正です: {args.date}")
            print("正しい形式: YYYY-MM-DD (例: 2025-12-22)")
            sys.exit(1)

    # データ収集実行
    collector = IRSDataCollector()
    try:
        collector.collect(target_date)
        sys.exit(0)
    except Exception as e:
        logger.error(f"データ収集エラー: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
