import yfinance as yf
import pandas as pd
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date

from core.db.sync_client import DatabaseManager

class YahooFinanceCollector:
    """
    Yahoo Financeから市場データ（株価指数・為替）を取得し、データベースに保存するクラス。
    """
    
    # 収集対象の定義
    TARGETS = [
        # Stock Indices / ETFs
        {
            "type": "stock",
            "name": "Nikkei 225",
            "ticker": "^N225",
            "db_ticker": "^N225" # DBに保存する識別子
        },
        {
            "type": "stock",
            "name": "TOPIX ETF (Next Funds)",
            "ticker": "1306.T",
            "db_ticker": "1306.T"
        },
        {
            "type": "stock",
            "name": "S&P 500",
            "ticker": "^GSPC",
            "db_ticker": "^GSPC"
        },
        {
            "type": "stock",
            "name": "STOXX Europe 600",
            "ticker": "^STOXX",
            "db_ticker": "^STOXX"
        },
        # Forex
        {
            "type": "forex",
            "name": "USD/JPY",
            "ticker": "USDJPY=X",
            "currency_pair": "USDJPY"
        },
        {
            "type": "forex",
            "name": "EUR/JPY",
            "ticker": "EURJPY=X",
            "currency_pair": "EURJPY"
        },
        {
            "type": "forex",
            "name": "Dollar Index (DXY)",
            "ticker": "DX-Y.NYB",
            "currency_pair": "DXY"
        },
        # Commodities (Stored in stock_prices for now as it has similar structure)
        {
            "type": "stock",
            "name": "WTI Crude Oil",
            "ticker": "CL=F",
            "db_ticker": "CL=F"
        },
        # Yields (Germany Bund)
        {
            "type": "yield",
            "name": "Germany Bund 2Y",
            "ticker": "^GDBR2",
            "region": "DE",
            "tenor": "2Y"
        },
        {
            "type": "yield",
            "name": "Germany Bund 5Y",
            "ticker": "^GDBR5",
            "region": "DE",
            "tenor": "5Y"
        },
        {
            "type": "yield",
            "name": "Germany Bund 10Y",
            "ticker": "^GDBR10",
            "region": "DE",
            "tenor": "10Y"
        },
        {
            "type": "yield",
            "name": "Germany Bund 30Y",
            "ticker": "^GDBR30",
            "region": "DE",
            "tenor": "30Y"
        },
        # Yields (UK Gilt)
        {
            "type": "yield",
            "name": "UK Gilt 2Y",
            "ticker": "^GUKG2",
            "region": "UK",
            "tenor": "2Y"
        },
        {
            "type": "yield",
            "name": "UK Gilt 5Y",
            "ticker": "^GUKG5",
            "region": "UK",
            "tenor": "5Y"
        },
        {
            "type": "yield",
            "name": "UK Gilt 10Y",
            "ticker": "^GUKG10",
            "region": "UK",
            "tenor": "10Y"
        },
        {
            "type": "yield",
            "name": "UK Gilt 30Y",
            "ticker": "^GUKG30",
            "region": "UK",
            "tenor": "30Y"
        }
    ]

    def __init__(self):
        self.db = DatabaseManager()
        self.logger = logging.getLogger(__name__)

    def fetch_and_save_data(self, period: str = "1mo"):
        """
        指定期間のデータを取得して保存する。
        
        Args:
            period (str): 取得期間 (e.g., "1d", "5d", "1mo", "1y", "max")
        """
        self.logger.info(f"Starting market data collection (period={period})...")
        
        for target in self.TARGETS:
            try:
                ticker_symbol = target["ticker"]
                name = target["name"]
                data_type = target["type"]
                
                self.logger.info(f"Fetching {name} ({ticker_symbol})...")
                
                # yfinanceでデータ取得
                df = yf.Ticker(ticker_symbol).history(period=period)
                
                if df.empty:
                    self.logger.warning(f"No data found for {ticker_symbol}")
                    continue
                
                # インデックス（Date）をカラムに戻し、タイムゾーンを削除
                df.reset_index(inplace=True)
                df['Date'] = pd.to_datetime(df['Date']).dt.date
                
                if data_type == "stock":
                    self._save_stock_prices(df, target)
                elif data_type == "forex":
                    self._save_exchange_rates(df, target)
                elif data_type == "yield":
                    self._save_foreign_yields(df, target)
                    
            except Exception as e:
                self.logger.error(f"Error processing {target['name']}: {e}")

        self.logger.info("Market data collection completed.")

    def _save_stock_prices(self, df: pd.DataFrame, target_info: Dict[str, Any]):
        """株価データを保存"""
        data_list = []
        db_ticker = target_info["db_ticker"]
        name = target_info["name"]
        
        for _, row in df.iterrows():
            record = {
                "trade_date": row['Date'],
                "ticker": db_ticker,
                "name": name,
                "open_price": self._round(row.get('Open')),
                "high_price": self._round(row.get('High')),
                "low_price": self._round(row.get('Low')),
                "close_price": self._round(row.get('Close')),
                "volume": int(row.get('Volume', 0))
            }
            data_list.append(record)
            
        if data_list:
            count = self.db.batch_insert_data(
                data_list=data_list,
                table_name="stock_prices",
                batch_size=1000,
                conflict_target="trade_date, ticker",
                update_columns=["open_price", "high_price", "low_price", "close_price", "volume", "updated_at"]
            )
            self.logger.info(f"Saved {count} records for {name}")

    def _save_exchange_rates(self, df: pd.DataFrame, target_info: Dict[str, Any]):
        """為替データを保存"""
        data_list = []
        currency_pair = target_info["currency_pair"]
        
        for _, row in df.iterrows():
            record = {
                "trade_date": row['Date'],
                "currency_pair": currency_pair,
                "open_price": self._round(row.get('Open'), 4),
                "high_price": self._round(row.get('High'), 4),
                "low_price": self._round(row.get('Low'), 4),
                "close_price": self._round(row.get('Close'), 4)
            }
            data_list.append(record)
            
        if data_list:
            count = self.db.batch_insert_data(
                data_list=data_list,
                table_name="exchange_rates",
                batch_size=1000,
                conflict_target="trade_date, currency_pair",
                update_columns=["open_price", "high_price", "low_price", "close_price", "updated_at"]
            )
            self.logger.info(f"Saved {count} records for {currency_pair}")

    def _save_foreign_yields(self, df: pd.DataFrame, target_info: Dict[str, Any]):
        """海外金利データを保存"""
        data_list = []
        region = target_info["region"]
        tenor = target_info["tenor"]
        
        for _, row in df.iterrows():
            # Yahoo Financeの利回りシンボル(^GDBR10等)はCloseに利回り(%)が格納されている
            yield_val = row.get('Close')
            if pd.isna(yield_val):
                continue
                
            record = {
                "trade_date": row['Date'],
                "region": region,
                "instrument_type": "sovereign",
                "tenor": tenor,
                "yield_value": self._round(yield_val, 4),
                "source": "YahooFinance"
            }
            data_list.append(record)
            
        if data_list:
            count = self.db.batch_insert_data(
                data_list=data_list,
                table_name="foreign_yields",
                batch_size=1000,
                conflict_target="trade_date, region, instrument_type, tenor",
                update_columns=["yield_value", "updated_at"]
            )
            self.logger.info(f"Saved {count} records for {target_info['name']}")

    def _round(self, value, digits=2):
        """None安全な丸め処理"""
        if pd.isna(value) or value is None:
            return None
        return round(float(value), digits)

if __name__ == "__main__":
    # 直接実行時のテスト用
    logging.basicConfig(level=logging.INFO)
    collector = YahooFinanceCollector()
    collector.fetch_and_save_data(period="1mo")
