"""
市中残存額（market_amount）計算クラス

市中残存額 = 累積発行額 - 日銀保有額
"""

import logging
from typing import Dict, List, Optional
from data.utils.database_manager import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MarketAmountCalculator:
    """市中残存額計算クラス"""

    def __init__(self):
        self.db = DatabaseManager()

    def get_all_bond_codes(self) -> List[str]:
        """bond_dataテーブルから全銘柄コードを取得"""
        query = "SELECT DISTINCT bond_code FROM bond_data ORDER BY bond_code"
        rows = self.db.execute_query(query)
        codes = [r[0] for r in rows]
        logger.info(f"Found {len(codes)} unique bond codes in bond_data")
        return codes

    def get_auction_history(self, bond_code: str) -> List[Dict]:
        """
        銘柄の入札履歴を取得（発行額情報）

        Returns:
            [{'auction_date': '2024-01-01', 'allocated_amount': 1000}, ...]
        """
        query = """
            SELECT auction_date, allocated_amount 
            FROM bond_auction 
            WHERE bond_code = %s 
            ORDER BY auction_date
        """
        rows = self.db.execute_query(query, (bond_code,))
        
        # 辞書リストに変換 (datetime.dateは文字列比較可能なためそのままでも良いが、
        # 既存ロジックが文字列比較前提ならisoformatにする必要があるかも。
        # ここではDatabaseManagerが返すdate型を想定。)
        result = []
        for r in rows:
            auction_date = r[0]
            # date型なら文字列に変換（既存ロジックとの互換性のため）
            if hasattr(auction_date, 'isoformat'):
                auction_date_str = auction_date.isoformat()
            else:
                auction_date_str = str(auction_date)
                
            result.append({
                'auction_date': auction_date_str,
                'allocated_amount': r[1]
            })
        
        return result

    def get_boj_holdings_history(self, bond_code: str) -> List[Dict]:
        """
        銘柄の日銀保有履歴を取得

        Returns:
            [{'data_date': '2024-01-10', 'face_value': 500}, ...]
        """
        query = """
            SELECT data_date, face_value 
            FROM boj_holdings 
            WHERE bond_code = %s 
            ORDER BY data_date
        """
        rows = self.db.execute_query(query, (bond_code,))
        
        result = []
        for r in rows:
            data_date = r[0]
            if hasattr(data_date, 'isoformat'):
                data_date_str = data_date.isoformat()
            else:
                data_date_str = str(data_date)
                
            result.append({
                'data_date': data_date_str,
                'face_value': r[1]
            })
        
        return result


    def calculate_cumulative_issuance(
        self,
        auction_history: List[Dict],
        target_date: str
    ) -> Optional[int]:
        """
        指定日までの累積発行額を計算

        Args:
            auction_history: 入札履歴（日付昇順）
            target_date: 対象日（YYYY-MM-DD）

        Returns:
            累積発行額（億円）、発行前はNone
        """
        total = 0
        found = False

        for auction in auction_history:
            if auction['auction_date'] <= target_date:
                amount = auction.get('allocated_amount')
                if amount is not None:
                    total += int(amount)
                    found = True

        return total if found else None

    def get_latest_boj_holding(
        self,
        boj_history: List[Dict],
        target_date: str
    ) -> Optional[int]:
        """
        指定日以前の最新の日銀保有額を取得

        Args:
            boj_history: 日銀保有履歴（日付昇順）
            target_date: 対象日（YYYY-MM-DD）

        Returns:
            日銀保有額（億円）、データなしはNone
        """
        latest = None

        for holding in boj_history:
            if holding['data_date'] <= target_date:
                face_value = holding.get('face_value')
                if face_value is not None:
                    latest = int(face_value)
            else:
                break

        return latest

