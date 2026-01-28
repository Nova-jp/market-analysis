import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
from data.collectors.jsda.bond_trade_volume_collector import BondTradeVolumeCollector
from data.utils.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

class JSDAVolumeService:
    """
    JSDA 公社債店頭売買高データの定期収集・管理サービス
    """

    def __init__(self):
        self.collector = BondTradeVolumeCollector()
        self.db = DatabaseManager()

    def sync_with_jsda(self) -> Dict[str, Any]:
        """
        JSDAから最新データを取得し、DBと同期する。
        ポーリング方式: 未登録の最新データがあるかチェックし、あれば取得する。
        """
        try:
            logger.info("Checking for JSDA Bond Trade Volume updates...")
            
            now = datetime.now(timezone(timedelta(hours=9)))
            current_fiscal_year = now.year if now.month >= 4 else now.year - 1
            
            # チェック対象の年度リスト
            # 基本は当年度だが、4月・5月は前年度の3月分データ等が更新される時期なので
            # 前年度もチェック対象に含める
            target_years = [current_fiscal_year]
            if now.month in [4, 5]:
                target_years.append(current_fiscal_year - 1)
                # リストをソート（古い年度から処理したほうが自然）
                target_years.sort()

            logger.info(f"Target fiscal years to check: {target_years}")

            # DBの最新データ日付を確認
            query = "SELECT MAX(reference_date) FROM bond_trade_volume_by_investor"
            result = self.db.execute_query(query)
            last_date = result[0][0] if result and result[0][0] else None
            
            logger.info(f"Last recorded date in DB: {last_date}")

            overall_success = True
            updated_any = False

            for year in target_years:
                # Collector内部で ON CONFLICT DO UPDATE を行っている
                # ファイルが存在しなくても(404)、新しい年度の初めならエラーログは出るが処理は続行させる
                success = self.collector.fetch_and_process_year(year)
                if not success:
                    # 4月時点で新年度ファイルがないのは正常なので、警告程度に留める判断も可だが
                    # ここでは一旦記録しておく
                    logger.warning(f"Fetch attempt for FY{year} returned False (might not exist yet or error)")
                    # ただし、前年度の取得に失敗した場合は問題
                    if year < current_fiscal_year:
                        overall_success = False
            
            # 更新後の最新日付を再確認
            result_new = self.db.execute_query(query)
            new_last_date = result_new[0][0] if result_new and result_new[0][0] else None
            
            if new_last_date != last_date:
                logger.info(f"New data found! Latest date: {new_last_date}")
                return {
                    "status": "success",
                    "message": f"Successfully updated. Newest data date: {new_last_date}",
                    "updated": True
                }
            else:
                logger.info("No new data found in Excel yet.")
                return {
                    "status": "success", # エラーではなく「データなし」として返す
                    "message": "Check completed. No new data found.",
                    "updated": False
                }

        except Exception as e:
            logger.error(f"Error in JSDAVolumeService.sync_with_jsda: {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e)
            }
