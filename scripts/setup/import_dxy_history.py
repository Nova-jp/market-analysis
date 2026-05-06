"""
DXY (Dollar Index) 過去データの一括インポート
exchange_rates テーブルへ 2014-01-01 以降のデータを挿入
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import yfinance as yf
import pandas as pd
import logging
from core.db.sync_client import DatabaseManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def fetch_dxy(start='2014-01-01', end='2026-03-22'):
    logger.info(f"Fetching DXY from Yahoo Finance: {start} -> {end}")
    dxy = yf.Ticker('DX-Y.NYB')
    hist = dxy.history(start=start, end=end)
    hist.index = hist.index.tz_localize(None).normalize()
    hist = hist[['Open', 'High', 'Low', 'Close']].dropna()
    logger.info(f"Fetched {len(hist)} rows: {hist.index.min().date()} to {hist.index.max().date()}")
    return hist


def import_to_db(hist: pd.DataFrame):
    db = DatabaseManager()
    insert_sql = """
        INSERT INTO exchange_rates (trade_date, currency_pair, open_price, high_price, low_price, close_price)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_date, currency_pair) DO UPDATE
            SET open_price  = EXCLUDED.open_price,
                high_price  = EXCLUDED.high_price,
                low_price   = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price
    """
    records = [
        (
            idx.date(), 'DXY',
            round(float(row['Open']), 4),
            round(float(row['High']), 4),
            round(float(row['Low']),  4),
            round(float(row['Close']),4),
        )
        for idx, row in hist.iterrows()
    ]

    # バッチ分割で挿入
    batch_size = 500
    total = 0
    with db._get_connection() as conn:
        with conn.cursor() as cur:
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                cur.executemany(insert_sql, batch)
                total += len(batch)
                logger.info(f"  Inserted batch {i//batch_size + 1}: {total}/{len(records)}")
        conn.commit()
    logger.info(f"Done. Total {total} records inserted/updated.")

    # 確認
    res = db.select_as_dict(
        "SELECT MIN(trade_date) as from_d, MAX(trade_date) as to_d, COUNT(*) as cnt "
        "FROM exchange_rates WHERE currency_pair='DXY'"
    )
    logger.info(f"DB check: {res[0]}")


if __name__ == '__main__':
    hist = fetch_dxy()
    import_to_db(hist)
