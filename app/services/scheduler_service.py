"""
スケジューラーサービス
データ収集ビジネスロジックを提供
"""
import logging
from typing import Dict, Any
from datetime import datetime, timedelta, timezone, date
import time
import calendar

from data.processors.bond_data_processor import BondDataProcessor
from data.utils.database_manager import DatabaseManager
from data.collectors.boj.holdings_collector import BOJHoldingsCollector
from data.collectors.mof.bond_auction_web_collector import BondAuctionWebCollector
from app.services.jsda_volume_service import JSDAVolumeService
import jpholiday

logger = logging.getLogger(__name__)

# JST定義
JST = timezone(timedelta(hours=9))


class SchedulerService:
    """
    スケジューラーサービスクラス
    日次データ収集の実行を管理
    """

    def __init__(self):
        self.processor = BondDataProcessor()
        self.db_manager = DatabaseManager()
        self.boj_collector = BOJHoldingsCollector(delay_seconds=2.0)
        self.mof_collector = BondAuctionWebCollector()
        self.jsda_volume_service = JSDAVolumeService()
        self.date_validation_warning = None  # 日付検証の警告メッセージ

    def get_target_date(self) -> str:
        """
        取得対象日付を決定（祝日・土日をスキップ）

        Returns:
            YYYY-MM-DD形式の日付文字列
        """
        # JSTで現在時刻を取得
        now_jst = datetime.now(JST)
        target_date = now_jst + timedelta(days=1)

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

        fallback = (now_jst + timedelta(days=1)).strftime('%Y-%m-%d')
        logger.warning(f"Could not find weekday, using fallback: {fallback}")
        return fallback

    def collect_data(self) -> Dict[str, Any]:
        """
        日次データ収集を実行 (JSDA, MOF, BOJ)

        Returns:
            実行結果の辞書（status, message, records_saved等）
        """
        try:
            logger.info("=== Starting daily data collection ===")
            results = {
                "status": "success",
                "message": "Daily collection completed",
                "details": {}
            }

            # 1. JSDA Collection
            target_date_str = self.get_target_date()
            jsda_saved = self._collect_single_day(target_date_str)
            
            jsda_status = "success"
            jsda_msg = f"Saved {jsda_saved} records"
            if jsda_saved < 0:
                jsda_status = "error"
                jsda_msg = "Collection failed"
                results["status"] = "error" # Mark overall status as error if JSDA fails
            elif jsda_saved == 0:
                jsda_msg = "No data available (possibly holiday)"

            results["details"]["jsda"] = {
                "status": jsda_status,
                "message": jsda_msg,
                "count": jsda_saved,
                "target_date": target_date_str
            }

            # 2. MOF Collection
            # Refactored to use the collector's built-in sync method
            mof_success = self.mof_collector.sync_with_database()
            results["details"]["mof"] = {
                "status": "success" if mof_success else "warning", 
                "message": "Completed" if mof_success else "Failed with error"
            }

            # 3. BOJ Collection
            # Refactored to use the collector's built-in sync method
            boj_success = self.boj_collector.sync_with_database()
            results["details"]["boj"] = {
                "status": "success" if boj_success else "warning",
                "message": "Completed" if boj_success else "Failed with error"
            }

            # 4. JSDA Volume Collection (Monthly Statistics - Polling)
            # Only run check around the 20th-26th of each month to save resources, 
            # or just run it daily as it's lightweight.
            # Here we run it daily as the check is lightweight.
            jsda_volume_result = self.jsda_volume_service.sync_with_jsda()
            results["details"]["jsda_volume"] = jsda_volume_result

            logger.info(f"Collection finished. Results: {results}")
            return results

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
                # target_date: HTML/CSV公表日（翌営業日）
                # actual_trade_date: 実際の取引日（公表日の1営業日前）
                url, filename, actual_trade_date = self.processor.build_csv_url(target_date)

                # 日付検証: actual_trade_dateが今日（実行日）と一致するか確認
                today = date.today()
                if actual_trade_date != today:
                    warning_msg = (
                        f"⚠️ 日付不一致警告: "
                        f"実行日={today.isoformat()}, "
                        f"計算された取引日={actual_trade_date.isoformat()}, "
                        f"CSV公表日={target_date_str}"
                    )
                    logger.warning(warning_msg)
                    self.date_validation_warning = warning_msg
                else:
                    logger.info(f"✅ 日付検証OK: 実行日={today.isoformat()}, 取引日={actual_trade_date.isoformat()}")
                    self.date_validation_warning = None

                logger.info(f"CSV公表日: {target_date_str}, 実際の取引日: {actual_trade_date}")

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

                # trade_date追加（実際の取引日を使用）
                processed_df['trade_date'] = actual_trade_date.isoformat()

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

            result = {
                "status": "healthy",
                "database_connected": record_count >= 0,
                "total_records": record_count if record_count >= 0 else None,
                "next_collection_target": next_target,
                "timestamp": datetime.now().isoformat()
            }

            # 日付検証の警告があれば追加
            if self.date_validation_warning:
                result["date_validation_warning"] = self.date_validation_warning
                result["status"] = "warning"

            return result
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
