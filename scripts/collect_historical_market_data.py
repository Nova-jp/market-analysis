import logging
from data.collectors.macro.yahoo_finance_collector import YahooFinanceCollector

def main():
    """
    市場データ（株価・為替・商品）の全期間データを取得・保存するスクリプト
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    logger.info("Starting historical market data collection (period='max')...")
    
    collector = YahooFinanceCollector()
    collector.fetch_and_save_data(period="max")
    
    logger.info("Historical market data collection completed.")

if __name__ == "__main__":
    main()
