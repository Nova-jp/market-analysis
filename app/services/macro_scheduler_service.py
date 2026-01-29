import logging
import traceback
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, Any

from data.collectors.macro.yahoo_finance_collector import YahooFinanceCollector
from data.collectors.macro.fred_collector import FredCollector

class MacroSchedulerService:
    """
    マクロ経済データ収集用スケジューラーサービス
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def collect_data(self) -> Dict[str, Any]:
        """
        マクロ経済データ（株価、為替、米国債、CPI等）の収集を実行
        """
        try:
            self.logger.info("Starting macro data collection service...")
            
            results = {
                "yahoo_finance": "pending",
                "fred": "pending"
            }

            # 1. Yahoo Finance (直近5日分)
            try:
                self.logger.info("Executing YahooFinanceCollector (period='5d')...")
                y_collector = YahooFinanceCollector()
                y_collector.fetch_and_save_data(period="5d")
                results["yahoo_finance"] = "success"
            except Exception as e:
                self.logger.error(f"YahooFinanceCollector failed: {e}")
                results["yahoo_finance"] = f"failed: {str(e)}"
            
            # 2. FRED (直近2ヶ月分)
            try:
                # 月次データのラグを考慮して2ヶ月前からチェック
                start_date = (datetime.now() - relativedelta(months=2)).strftime("%Y-%m-%d")
                self.logger.info(f"Executing FredCollector (start_date='{start_date}')...")
                
                f_collector = FredCollector()
                f_collector.fetch_and_save_data(start_date=start_date)
                results["fred"] = "success"
            except Exception as e:
                self.logger.error(f"FredCollector failed: {e}")
                results["fred"] = f"failed: {str(e)}"

            # 総合判定
            if any(v.startswith("failed") for v in results.values()):
                return {
                    "status": "partial_success", # 一部失敗でも全体としては完了とする
                    "message": "Macro data collection completed with errors",
                    "details": results
                }
            
            return {
                "status": "success",
                "message": "Macro data collection completed successfully",
                "details": results
            }

        except Exception as e:
            error_msg = f"Critical error in macro collection service: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return {
                "status": "error",
                "message": error_msg,
                "traceback": traceback.format_exc()
            }
