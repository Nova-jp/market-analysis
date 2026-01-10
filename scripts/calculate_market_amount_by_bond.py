import sys
import os
import logging
from bisect import bisect_right
from collections import defaultdict
from tqdm import tqdm
from psycopg2.extras import execute_batch

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.utils.database_manager import DatabaseManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MarketAmountRefresher:
    def __init__(self):
        self.db = DatabaseManager()
        self.auctions = defaultdict(list)
        self.boj_holdings = defaultdict(list)

    def load_base_data(self):
        """入札データと日銀保有データをメモリにロード"""
        logger.info("入札データをメモリにロード中...")
        # 銘柄ごと、日付順に取得することで累積計算を正しく行う
        rows = self.db.execute_query("SELECT bond_code, auction_date, total_amount FROM bond_auction ORDER BY bond_code, auction_date")
        for code, date, amount in rows:
            # 累積額として保持するために加工
            prev_total = self.auctions[code][-1][1] if self.auctions[code] else 0
            self.auctions[code].append((date, prev_total + float(amount or 0)))

        logger.info("日銀保有データをメモリにロード中...")
        rows = self.db.execute_query("SELECT bond_code, data_date, face_value FROM boj_holdings ORDER BY bond_code, data_date")
        for code, date, value in rows:
            self.boj_holdings[code].append((date, float(value or 0)))

    def _save_to_db(self, data):
        """確実に上書き保存する"""
        query = """
            INSERT INTO bond_market_amount (trade_date, bond_code, market_amount)
            VALUES (%s, %s, %s)
            ON CONFLICT (trade_date, bond_code) 
            DO UPDATE SET market_amount = EXCLUDED.market_amount, updated_at = CURRENT_TIMESTAMP
        """
        with self.db._get_connection() as conn:
            with conn.cursor() as cur:
                values = [(d['trade_date'], d['bond_code'], d['market_amount']) for d in data]
                execute_batch(cur, query, values)
                conn.commit()

    def process_stream(self):
        """bond_dataを一括取得し、順次計算して保存"""
        logger.info("bond_dataから全取引日を一括取得中...")
        
        # 挿入用バッファ
        buffer = []
        BATCH_SIZE = 10000
        total_processed = 0

        # bond_data 全件取得 (ORDER BY bond_code が重要)
        all_rows = self.db.execute_query("SELECT bond_code, trade_date FROM bond_data ORDER BY bond_code, trade_date")
        
        logger.info(f"取得完了: {len(all_rows)} 件。計算を開始します。")

        # 高速化のため、ループ外で変数を用意
        current_bond_code = None
        current_auction_dates = []
        current_auction_data = []
        current_boj_dates = []
        current_boj_data = []

        for bond_code, trade_date in tqdm(all_rows, desc="計算中"):
            # 銘柄が変わったタイミングで、参照データを切り替え
            if bond_code != current_bond_code:
                current_bond_code = bond_code
                
                # 入札データ
                auc_list = self.auctions.get(bond_code, [])
                current_auction_dates = [x[0] for x in auc_list]
                current_auction_data = auc_list

                # 日銀保有データ
                boj_list = self.boj_holdings.get(bond_code, [])
                current_boj_dates = [x[0] for x in boj_list]
                current_boj_data = boj_list

            # 1. 累積発行額 (trade_date以前の最新)
            idx_a = bisect_right(current_auction_dates, trade_date)
            cum_issuance = current_auction_data[idx_a-1][1] if idx_a > 0 else 0

            # 2. 日銀保有額 (trade_date以前の最新)
            idx_b = bisect_right(current_boj_dates, trade_date)
            boj_amount = current_boj_data[idx_b-1][1] if idx_b > 0 else 0

            # 計算結果
            market_amount = round(cum_issuance - boj_amount, 2)

            buffer.append({
                'trade_date': trade_date,
                'bond_code': bond_code,
                'market_amount': market_amount
            })

            # バッファが溢れたら書き込み
            if len(buffer) >= BATCH_SIZE:
                self._save_to_db(buffer)
                total_processed += len(buffer)
                buffer = []

        # 残りのバッファを処理
        if buffer:
            self._save_to_db(buffer)
            total_processed += len(buffer)

        logger.info(f"全処理完了: {total_processed} 件")

    def run(self):
        self.load_base_data()
        self.process_stream()

if __name__ == "__main__":
    refresher = MarketAmountRefresher()
    refresher.run()
