"""
IRS (Interest Rate Swap) Data Collector
JPX/JSCCから金利スワップ清算値段を収集するクラス
"""
import logging
import tempfile
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests
import pdfplumber

from data.utils.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

class IRSCollector:
    """金利スワップデータ収集クラス"""

    # JPX IRS ページURL
    JPX_BASE_URL = "https://www.jpx.co.jp"

    def __init__(self):
        self.db_manager = DatabaseManager()

    def get_pdf_url(self, target_date: date) -> str:
        """PDF URLを生成"""
        date_str = target_date.strftime("%Y%m%d")
        pdf_filename = f"SettlementRates_{date_str}.pdf"
        pdf_url = f"{self.JPX_BASE_URL}/jscc/cimhll0000000umu-att/{pdf_filename}"
        return pdf_url

    def download_pdf(self, pdf_url: str) -> Optional[Path]:
        """PDFをダウンロード"""
        logger.info(f"Downloading PDF: {pdf_url}")

        try:
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()

            # 一時ファイルに保存
            temp_file = tempfile.NamedTemporaryFile(
                delete=False, suffix='.pdf', prefix='irs_settlement_'
            )
            temp_file.write(response.content)
            temp_file.close()

            logger.info(f"PDF downloaded: {temp_file.name}")
            return Path(temp_file.name)

        except requests.exceptions.RequestException as e:
            logger.error(f"PDF download failed: {e}")
            return None

    def parse_pdf_data(self, pdf_path: Path, fallback_date: date) -> tuple[List[Dict[str, Any]], Optional[date]]:
        """
        PDFからデータを抽出

        Returns:
            tuple: (データリスト, 取引日)
        """
        logger.info(f"Parsing PDF data: {pdf_path}")

        all_data = []
        trade_date = None

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()

                    if not tables:
                        logger.warning("No tables found in page")
                        continue

                    table = tables[0]

                    # PDFから取引日を抽出（Row 1の最後の列）
                    if not trade_date and len(table) > 1:
                        date_cell = table[1][-1]
                        if date_cell and date_cell.strip():
                            try:
                                trade_date = datetime.strptime(date_cell.strip(), '%Y/%m/%d').date()
                                logger.info(f"Trade date from PDF: {trade_date}")
                            except ValueError:
                                logger.warning(f"Failed to parse date: {date_cell}")

                    if not trade_date:
                        trade_date = fallback_date
                        logger.warning(f"Using fallback date: {trade_date}")

                    # データ行を処理（Row 3以降）
                    for row_idx, row in enumerate(table[3:], start=3):
                        # JPY OIS (列3-5)
                        if row[3] and row[4]:
                            all_data.append({
                                'trade_date': trade_date.isoformat(),
                                'product_type': 'OIS',
                                'tenor': row[3].strip(),
                                'rate': float(row[4].strip()),
                                'unit': row[5].strip() if row[5] else '%'
                            })

                        # JPY 3M TIBOR (列7-9)
                        if row[7] and row[8]:
                            all_data.append({
                                'trade_date': trade_date.isoformat(),
                                'product_type': '3M_TIBOR',
                                'tenor': row[7].strip(),
                                'rate': float(row[8].strip()),
                                'unit': row[9].strip() if row[9] else '%'
                            })

                        # JPY 6M TIBOR (列11-13)
                        if row[11] and row[12]:
                            all_data.append({
                                'trade_date': trade_date.isoformat(),
                                'product_type': '6M_TIBOR',
                                'tenor': row[11].strip(),
                                'rate': float(row[12].strip()),
                                'unit': row[13].strip() if row[13] else '%'
                            })

                        # JPY 1M TIBOR (列15-17)
                        if row[15] and row[16]:
                            all_data.append({
                                'trade_date': trade_date.isoformat(),
                                'product_type': '1M_TIBOR',
                                'tenor': row[15].strip(),
                                'rate': float(row[16].strip()),
                                'unit': row[17].strip() if row[17] else '%'
                            })

            logger.info(f"Parsed {len(all_data)} records (trade_date: {trade_date})")
            return all_data, trade_date

        except Exception as e:
            logger.error(f"Error parsing PDF: {e}", exc_info=True)
            return [], None

    def save_to_database(self, data: List[Dict[str, Any]]) -> int:
        """データをデータベースに保存"""
        if not data:
            logger.warning("No data to save")
            return 0

        logger.info(f"Saving {len(data)} records to database")

        try:
            # DatabaseManagerを使用してデータを挿入
            saved_count = self.db_manager.batch_insert_data(
                data_list=data,
                table_name="irs_data"
            )

            logger.info(f"Successfully saved {saved_count} records")
            return saved_count

        except Exception as e:
            logger.error(f"Database save failed: {e}")
            return -1

    def collect_data(self, target_date: date) -> int:
        """
        指定日のデータを収集してDBに保存

        Args:
            target_date: 対象日

        Returns:
            保存件数（失敗時は-1）
        """
        try:
            # 1. PDF URLを取得
            pdf_url = self.get_pdf_url(target_date)

            # 2. PDFをダウンロード
            pdf_path = self.download_pdf(pdf_url)
            if not pdf_path:
                logger.error(f"Failed to download PDF for {target_date}")
                return -1

            # 3. PDFからデータを抽出
            data, actual_trade_date = self.parse_pdf_data(pdf_path, target_date)

            # 4. 一時ファイルを削除
            try:
                pdf_path.unlink()
                logger.info(f"Temporary file deleted: {pdf_path}")
            except Exception as e:
                logger.warning(f"Failed to delete temp file: {e}")

            if not data:
                logger.error(f"No data extracted from PDF for {target_date}")
                return 0

            # 5. データベースに保存
            saved_count = self.save_to_database(data)
            return saved_count

        except Exception as e:
            logger.error(f"Error during IRS data collection for {target_date}: {e}")
            return -1
