import logging
from data.collectors.macro.yahoo_finance_collector import YahooFinanceCollector
from data.collectors.macro.fred_collector import FredCollector

def main():
    """
    全てのマクロ経済データ（Yahoo Finance + FRED）を取得・保存するスクリプト
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # 1. Yahoo Finance
    logger.info("--- Starting Yahoo Finance Collection ---")
    y_collector = YahooFinanceCollector()
    # period="max" で全期間取得。既存データはUPSERTで更新なしor最新化
    y_collector.fetch_and_save_data(period="max")
    
    # 2. FRED
    logger.info("--- Starting FRED Collection ---")
    f_collector = FredCollector()
    f_collector.fetch_and_save_data(start_date="1950-01-01")
    
    logger.info("--- All Macro Data Collection Completed ---")

if __name__ == "__main__":
    main()
