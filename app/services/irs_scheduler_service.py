"""
金利スワップ(IRS)データ収集スケジューラーサービス
JPXから毎日18:00に金利スワップデータを自動収集
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime, date, timedelta
from pathlib import Path
import tempfile
import requests

import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    import pdfplumber
except ImportError:
    pass

from data.utils.database_manager import DatabaseManager
import jpholiday

logger = logging.getLogger(__name__)


class IRSSchedulerService:
    """
    金利スワップデータ収集スケジューラーサービスクラス
    """

    # JPX IRS ページURL
    JPX_BASE_URL = "https://www.jpx.co.jp"

    def __init__(self):
        self.db_manager = DatabaseManager()

    def get_target_date(self) -> date:
        """
        取得対象日付を決定（祝日・土日をスキップ）

        Returns:
            対象日付（date型）
        """
        # 今日の日付を取得
        target_date = datetime.now().date()

        # 最大10日先まで確認
        for _ in range(10):
            if target_date.weekday() < 5 and not jpholiday.is_holiday(target_date):
                logger.info(f"Target date determined: {target_date}")
                return target_date

            if target_date.weekday() >= 5:
                logger.info(f"Skipping weekend: {target_date}")
            elif jpholiday.is_holiday(target_date):
                holiday_name = jpholiday.is_holiday_name(target_date)
                logger.info(f"Skipping holiday: {target_date} ({holiday_name})")

            target_date += timedelta(days=1)

        # フォールバック: 今日の日付を使用
        fallback = datetime.now().date()
        logger.warning(f"Could not find weekday, using fallback: {fallback}")
        return fallback

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

    def parse_pdf_data(self, pdf_path: Path, fallback_date: date) -> tuple[list, Optional[date]]:
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

    def save_to_database(self, data: list) -> int:
        """データをデータベースに保存"""
        if not data:
            logger.warning("No data to save")
            return 0

        logger.info(f"Saving {len(data)} records to database")

        try:
            url = f"{self.db_manager.supabase_url}/rest/v1/irs_settlement_rates"
            headers = self.db_manager.headers.copy()
            headers['Prefer'] = 'resolution=merge-duplicates,return=representation'

            response = requests.post(url, json=data, headers=headers)

            # 重複エラー（409 Conflict）は警告として扱う
            if response.status_code == 409:
                logger.warning("Data already exists (duplicate key). This is normal for re-runs on the same day.")
                return 0  # 重複の場合は0を返す（エラーではない）

            response.raise_for_status()

            logger.info(f"Successfully saved {len(data)} records")
            return len(data)

        except requests.exceptions.RequestException as e:
            # 409エラー以外のエラーはログに記録
            if hasattr(e, 'response') and e.response.status_code != 409:
                logger.error(f"Database save failed: {e}")
                if hasattr(e.response, 'text'):
                    logger.error(f"Response: {e.response.text}")
                return -1
            return 0

    def collect_data(self) -> Dict[str, Any]:
        """
        日次データ収集を実行

        Returns:
            実行結果の辞書
        """
        try:
            logger.info("=== Starting IRS data collection ===")

            # 1. 対象日付を決定
            target_date = self.get_target_date()

            # 2. PDF URLを取得
            pdf_url = self.get_pdf_url(target_date)

            # 3. PDFをダウンロード
            pdf_path = self.download_pdf(pdf_url)
            if not pdf_path:
                return {
                    "status": "error",
                    "message": f"Failed to download PDF for {target_date}",
                    "target_date": target_date.isoformat()
                }

            # 4. PDFからデータを抽出
            data, actual_trade_date = self.parse_pdf_data(pdf_path, target_date)

            # 5. 一時ファイルを削除
            try:
                pdf_path.unlink()
                logger.info(f"Temporary file deleted: {pdf_path}")
            except Exception as e:
                logger.warning(f"Failed to delete temp file: {e}")

            if not data:
                return {
                    "status": "error",
                    "message": f"No data extracted from PDF for {target_date}",
                    "target_date": target_date.isoformat()
                }

            # 6. データベースに保存
            saved_count = self.save_to_database(data)

            if saved_count > 0:
                logger.info(f"=== IRS data collection completed: {saved_count} records ===")
                return {
                    "status": "success",
                    "message": f"Successfully collected IRS data for {actual_trade_date}",
                    "target_date": target_date.isoformat(),
                    "actual_trade_date": actual_trade_date.isoformat() if actual_trade_date else None,
                    "records_saved": saved_count
                }
            elif saved_count == 0:
                # 重複の場合（既にデータが存在）
                logger.info(f"=== IRS data already exists for {actual_trade_date} ===")
                return {
                    "status": "success",
                    "message": f"IRS data already exists for {actual_trade_date} (duplicate)",
                    "target_date": target_date.isoformat(),
                    "actual_trade_date": actual_trade_date.isoformat() if actual_trade_date else None,
                    "records_saved": 0
                }
            else:
                return {
                    "status": "error",
                    "message": "Failed to save data to database",
                    "target_date": target_date.isoformat()
                }

        except Exception as e:
            error_msg = f"Unexpected error during IRS data collection: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                "status": "error",
                "message": error_msg
            }
