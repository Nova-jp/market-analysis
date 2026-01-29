import logging
import sys
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from data.collectors.macro.yahoo_finance_collector import YahooFinanceCollector
from data.collectors.macro.fred_collector import FredCollector

def main():
    """
    日次マクロ経済データ収集バッチ (Daily Macro Data Updater)
    
    実行頻度: 毎日 07:00 JST (推奨)
    対象データ:
      - Yahoo Finance: 直近5日分の株価・為替
      - FRED: 直近2ヶ月分の経済指標・金利 (CPIの遅れや訂正を考慮)
    """
    # ログ設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)
    
    logger.info("=== Starting Daily Macro Data Update ===")
    
    try:
        # 1. Yahoo Finance (直近5日分)
        # 週末や祝日のスキップを考慮して少し長めに取る
        logger.info("--- 1. Yahoo Finance Collection (Last 5 days) ---")
        y_collector = YahooFinanceCollector()
        y_collector.fetch_and_save_data(period="5d")
        
        # 2. FRED (直近2ヶ月分)
        # CPIなどの月次データは発表ラグがあるため、広めにカバーする
        start_date = (datetime.now() - relativedelta(months=2)).strftime("%Y-%m-%d")
        logger.info(f"--- 2. FRED Collection (From {start_date}) ---")
        f_collector = FredCollector()
        f_collector.fetch_and_save_data(start_date=start_date)
        
        logger.info("=== Daily Macro Data Update Completed Successfully ===")
        
    except Exception as e:
        logger.error(f"❌ Daily Update Failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
