import sys
import os
from pathlib import Path
from sqlalchemy import create_engine, text

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from core.config import settings

def check_latest_dates():
    engine = create_engine(settings.database_url)
    with engine.connect() as conn:
        print("--- Latest Dates in DB ---")
        
        # Bond Data
        res = conn.execute(text("SELECT MAX(trade_date) FROM bond_data"))
        latest_bond = res.scalar()
        print(f"Bond Data: {latest_bond}")

        # OIS Data
        res = conn.execute(text("SELECT MAX(trade_date) FROM irs_data WHERE product_type = 'OIS'"))
        latest_ois = res.scalar()
        print(f"OIS Data:  {latest_ois}")

        # ASW Data
        res = conn.execute(text("SELECT MAX(trade_date) FROM \"ASW_data\""))
        latest_asw = res.scalar()
        print(f"ASW Data:  {latest_asw}")

if __name__ == "__main__":
    check_latest_dates()
