"""
スケジューラーサービス
データ収集ビジネスロジックを提供
"""
import logging
from typing import Dict, Any
from datetime import datetime, timedelta
import time

from data.processors.bond_data_processor import BondDataProcessor
from data.utils.database_manager import DatabaseManager
import jpholiday

logger = logging.getLogger(__name__)


class SchedulerService:
    """
    スケジューラーサービスクラス
    日次データ収集の実行を管理
    """

    def __init__(self):
        self.processor = BondDataProcessor()
        self.db_manager = DatabaseManager()

    def get_target_date(self) -> str:
        """
        取得対象日付を決定（祝日・土日をスキップ）

        Returns:
            YYYY-MM-DD形式の日付文字列
        """
        target_date = datetime.now() + timedelta(days=1)

        for _ in range(10):
            if target_date.weekday() < 5 and not jpholiday.is_holiday(target_date.date()):
                result = target_date.strftime('%Y-%m-%d')
                logger.info(f"Target date determined: {result}")
                return result

            if target_date.weekday() >= 5:
                logger.info(f"Skipping weekend: {target_date.strftime('%Y-%m-%d')}")
            elif jpholiday.is_holiday(target_date.date()):
                holiday_name = jpholiday.is_holiday_name(target_date.date())
                logger.info(f"Skipping holiday: {target_date.strftime('%Y-%m-%d')} ({holiday_name})")

            target_date += timedelta(days=1)

        fallback = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        logger.warning(f"Could not find weekday, using fallback: {fallback}")
        return fallback

    def collect_data(self) -> Dict[str, Any]:
        """
        日次データ収集を実行

        Returns:
            実行結果の辞書（status, message, records_saved等）
        """
        try:
            logger.info("=== Starting daily data collection ===")

            target_date_str = self.get_target_date()
            saved_count = self._collect_single_day(target_date_str)

            if saved_count > 0:
                logger.info(f"Successfully collected {saved_count} records")
                return {
                    "status": "success",
                    "message": f"Successfully collected data for {target_date_str}",
                    "target_date": target_date_str,
                    "records_saved": saved_count
                }
            elif saved_count == 0:
                logger.info("No data available (possibly a holiday)")
                return {
                    "status": "success",
                    "message": f"No data available for {target_date_str}",
                    "target_date": target_date_str,
                    "records_saved": 0
                }
            else:
                logger.error("Data collection failed")
                return {
                    "status": "error",
                    "message": "Data collection failed",
                    "target_date": target_date_str
                }

        except Exception as e:
            error_msg = f"Unexpected error during data collection: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                "status": "error",
                "message": error_msg
            }

    def _collect_single_day(self, target_date_str: str, max_retries: int = 1) -> int:
        """
        1日分のデータを収集

        Args:
            target_date_str: 対象日付（YYYY-MM-DD）
            max_retries: 最大リトライ回数

        Returns:
            保存したレコード数（失敗時は-1）
        """
        from datetime import date

        for attempt in range(max_retries + 1):
            try:
                logger.info(f"Collecting data for {target_date_str} (attempt {attempt + 1}/{max_retries + 1})")

                target_date = date.fromisoformat(target_date_str)

                # CSVデータ取得
                url, filename = self.processor.build_csv_url(target_date)
                raw_df = self.processor.download_csv_data(url)

                if raw_df is None:
                    logger.warning(f"Failed to download CSV for {target_date_str}")
                    if attempt < max_retries:
                        logger.info("Waiting 5 seconds before retry...")
                        time.sleep(5)
                    continue

                if raw_df.empty:
                    logger.info(f"No data available for {target_date_str}")
                    return 0

                # データ処理
                processed_df = self.processor.process_raw_data(raw_df)

                if processed_df.empty:
                    logger.info(f"Processed data is empty for {target_date_str}")
                    return 0

                # trade_date追加
                processed_df['trade_date'] = target_date_str

                # データベース保存
                batch_data = processed_df.to_dict('records')
                saved_count = self.db_manager.batch_insert_data(batch_data)

                if saved_count > 0:
                    logger.info(f"Successfully saved {saved_count} records")
                    return saved_count
                else:
                    logger.warning(f"Failed to save data for {target_date_str}")
                    return -1

            except Exception as e:
                error_msg = str(e)
                is_timeout = "timeout" in error_msg.lower() or "connection" in error_msg.lower()

                if attempt < max_retries:
                    wait_time = 300 if is_timeout else 180
                    logger.warning(f"Error (attempt {attempt + 1}): {error_msg}")
                    logger.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries reached: {error_msg}")
                    return -1

        return -1

    def health_check(self) -> Dict[str, Any]:
        """
        サービスのヘルスチェック

        Returns:
            ステータス情報の辞書
        """
        try:
            # DB接続確認
            record_count = self.db_manager.get_total_record_count()

            # 次回収集対象日付
            next_target = self.get_target_date()

            return {
                "status": "healthy",
                "database_connected": record_count >= 0,
                "total_records": record_count if record_count >= 0 else None,
                "next_collection_target": next_target,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
