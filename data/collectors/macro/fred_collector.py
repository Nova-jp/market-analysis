import pandas_datareader.data as web
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from data.utils.database_manager import DatabaseManager

class FredCollector:
    """
    FRED (Federal Reserve Economic Data) から経済指標を取得し、データベースに保存するクラス。
    """
    
    # 収集対象の定義
    CPI_TARGETS = [
        {
            "name": "Japan CPI",
            "code": "JPNCPIALLMINMEI", # OECD: Consumer Price Index: All Items for Japan
            "country": "Japan",
            "frequency": "monthly"
        },
        {
            "name": "US CPI",
            "code": "CPIAUCSL", # Consumer Price Index for All Urban Consumers: All Items
            "country": "US",
            "frequency": "monthly"
        },
        {
            "name": "Euro Area CPI",
            "code": "CP0000EZ19M086NEST", # Harmonized Index of Consumer Prices: All Items for Euro Area 19
            "country": "Euro Area",
            "frequency": "monthly"
        }
    ]

    YIELD_TARGETS = [
        # US Treasuries (Constant Maturity)
        {"code": "DGS1MO", "region": "US", "type": "sovereign", "tenor": "1M"},
        {"code": "DGS3MO", "region": "US", "type": "sovereign", "tenor": "3M"},
        {"code": "DGS6MO", "region": "US", "type": "sovereign", "tenor": "6M"},
        {"code": "DGS1",    "region": "US", "type": "sovereign", "tenor": "1Y"},
        {"code": "DGS2",    "region": "US", "type": "sovereign", "tenor": "2Y"},
        {"code": "DGS3",    "region": "US", "type": "sovereign", "tenor": "3Y"},
        {"code": "DGS5",    "region": "US", "type": "sovereign", "tenor": "5Y"},
        {"code": "DGS7",    "region": "US", "type": "sovereign", "tenor": "7Y"},
        {"code": "DGS10",   "region": "US", "type": "sovereign", "tenor": "10Y"},
        {"code": "DGS20",   "region": "US", "type": "sovereign", "tenor": "20Y"},
        {"code": "DGS30",   "region": "US", "type": "sovereign", "tenor": "30Y"},
        # Policy Rates
        {"code": "DFF",     "region": "US", "type": "policy",    "tenor": "ON"},
        # SOFR
        {"code": "SOFR",    "region": "US", "type": "sofr",      "tenor": "ON"},
    ]

    def __init__(self):
        self.db = DatabaseManager()
        self.logger = logging.getLogger(__name__)

    def fetch_and_save_data(self, start_date: str = "1960-01-01"):
        """
        指定期間のデータを取得して保存する。
        """
        self.logger.info(f"Starting FRED data collection (start_date={start_date})...")
        
        # 1. CPI Data
        for target in self.CPI_TARGETS:
            self._process_target(target, "economic_indicators", start_date)
            
        # 2. Yield Data
        for target in self.YIELD_TARGETS:
            self._process_target(target, "foreign_yields", start_date)

        self.logger.info("FRED data collection completed.")

    def _process_target(self, target: Dict[str, Any], table_name: str, start_date: str):
        """個別の系列の取得と保存を処理"""
        try:
            code = target["code"]
            self.logger.info(f"Fetching {code} from FRED...")
            
            df = web.DataReader(code, 'fred', start_date)
            
            if df.empty:
                self.logger.warning(f"No data found for {code}")
                return

            if table_name == "economic_indicators":
                self._save_economic_indicators(df, target)
            elif table_name == "foreign_yields":
                self._save_foreign_yields(df, target)
                
        except Exception as e:
            self.logger.error(f"Error processing {target.get('code', 'unknown')}: {e}")

    def _save_economic_indicators(self, df: pd.DataFrame, target_info: Dict[str, Any]):
        """経済指標データを保存"""
        data_list = []
        indicator_code = target_info["code"]
        
        for date_idx, row in df.iterrows():
            value = row.get(indicator_code)
            if pd.isna(value): continue

            data_list.append({
                "release_date": date_idx.date(),
                "indicator_code": indicator_code,
                "country": target_info["country"],
                "name": target_info["name"],
                "value": float(value),
                "frequency": target_info["frequency"],
                "source": "FRED"
            })
            
        if data_list:
            count = self.db.batch_insert_data(
                data_list=data_list,
                table_name="economic_indicators",
                batch_size=2000,
                conflict_target="release_date, indicator_code",
                update_columns=["value", "updated_at"]
            )
            self.logger.info(f"Saved {count} records for {indicator_code}")

    def _save_foreign_yields(self, df: pd.DataFrame, target_info: Dict[str, Any]):
        """海外金利データを保存"""
        data_list = []
        code = target_info["code"]
        
        for date_idx, row in df.iterrows():
            value = row.get(code)
            if pd.isna(value): continue

            data_list.append({
                "trade_date": date_idx.date(),
                "region": target_info["region"],
                "instrument_type": target_info["type"],
                "tenor": target_info["tenor"],
                "yield_value": float(value),
                "source": "FRED"
            })
            
        if data_list:
            count = self.db.batch_insert_data(
                data_list=data_list,
                table_name="foreign_yields",
                batch_size=2000,
                conflict_target="trade_date, region, instrument_type, tenor",
                update_columns=["yield_value", "updated_at"]
            )
            self.logger.info(f"Saved {count} records for {code}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = FredCollector()
    collector.fetch_and_save_data()
