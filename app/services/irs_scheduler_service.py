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

    def __init__(self):
        # 共通コレクターを使用
        from data.collectors.jscc.irs_collector import IRSCollector
        self.collector = IRSCollector()

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

            # 2. 共通コレクターで収集実行
            saved_count = self.collector.collect_data(target_date)

            if saved_count > 0:
                logger.info(f"=== IRS data collection completed: {saved_count} records ===")
                return {
                    "status": "success",
                    "message": f"Successfully collected IRS data for {target_date}",
                    "target_date": target_date.isoformat(),
                    "records_saved": saved_count
                }
            elif saved_count == 0:
                # 0件の場合はデータなし、または重複の可能性
                # IRSCollectorの戻り値だけでは区別できないが、エラー(-1)でなければ成功扱いとする
                logger.info(f"=== IRS data collection finished with 0 records (possibly duplicate or empty) ===")
                return {
                    "status": "success",
                    "message": f"IRS data collection finished with 0 records for {target_date}",
                    "target_date": target_date.isoformat(),
                    "records_saved": 0
                }
            else:
                return {
                    "status": "error",
                    "message": "Failed to collect data (collector returned -1)",
                    "target_date": target_date.isoformat()
                }

        except Exception as e:
            error_msg = f"Unexpected error during IRS data collection: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                "status": "error",
                "message": error_msg
            }
