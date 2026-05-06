
import os
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from core.db.sync_client import DatabaseManager

def check_tables():
    db = DatabaseManager()
    tables = [
        "stock_prices",
        "exchange_rates",
        "economic_indicators",
        "foreign_yields",
        "irs_data"
    ]
    
    for table in tables:
        print(f"\n--- Checking table: {table} ---")
        try:
            count = db.get_total_record_count(table)
            print(f"Total records: {count}")
            
            if count > 0:
                # 最新の5件を取得
                if table == "economic_indicators":
                    date_col = "release_date"
                else:
                    date_col = "trade_date"
                
                query = f"SELECT * FROM {table} ORDER BY {date_col} DESC LIMIT 5"
                rows = db.select_as_dict(query)
                for row in rows:
                    print(row)
                    
                # DXY またはそれに類するデータがあるかチェック
                if table == "stock_prices":
                    dxy_query = "SELECT COUNT(*) as count FROM stock_prices WHERE ticker LIKE '%%DXY%%' OR ticker LIKE '%%DX-Y%%'"
                    res = db.select_as_dict(dxy_query)
                    print(f"Possible DXY records in stock_prices: {res[0]['count']}")
                elif table == "exchange_rates":
                    dxy_query = "SELECT COUNT(*) as count FROM exchange_rates WHERE currency_pair LIKE '%%DXY%%'"
                    res = db.select_as_dict(dxy_query)
                    print(f"Possible DXY records in exchange_rates: {res[0]['count']}")
                elif table == "economic_indicators":
                    dxy_query = "SELECT COUNT(*) as count FROM economic_indicators WHERE indicator_code LIKE '%%DXY%%'"
                    res = db.select_as_dict(dxy_query)
                    print(f"Possible DXY records in economic_indicators: {res[0]['count']}")
            
        except Exception as e:
            print(f"Error checking table {table}: {e}")

if __name__ == "__main__":
    check_tables()
