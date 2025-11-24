#!/usr/bin/env python3
"""
市中残存額（market_amount）計算スクリプト

市中残存額 = 累積発行額 - 日銀保有額

計算ロジック:
1. 各銘柄について、発行日以降のすべてのtrade_dateに対して計算
2. 累積発行額: bond_auctionのallocated_amountを発行日から累積
3. 日銀保有額: boj_holdingsから対象日以前の最新face_valueを使用
4. 古い日付から順に処理

使用方法:
    # 全銘柄の市中残存額を計算
    python scripts/calculate_market_amount.py

    # 特定の銘柄のみ
    python scripts/calculate_market_amount.py --bond-code 004640067

    # テストモード（DB更新なし）
    python scripts/calculate_market_amount.py --test

    # 特定日付以降のみ再計算
    python scripts/calculate_market_amount.py --from-date 2024-01-01
"""

import sys
import os
import argparse
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import logging

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MarketAmountCalculator:
    """市中残存額計算クラス"""

    def __init__(self):
        url = os.getenv('SUPABASE_URL')
        key = os.getenv('SUPABASE_KEY')
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        self.supabase = create_client(url, key)

    def get_all_bond_codes(self) -> List[str]:
        """bond_dataテーブルから全銘柄コードを取得"""
        result = self.supabase.table('bond_data') \
            .select('bond_code') \
            .execute()

        codes = set(r['bond_code'] for r in result.data)
        logger.info(f"Found {len(codes)} unique bond codes in bond_data")
        return sorted(codes)

    def get_auction_history(self, bond_code: str) -> List[Dict]:
        """
        銘柄の入札履歴を取得（発行額情報）

        Returns:
            [{'auction_date': '2024-01-01', 'allocated_amount': 1000}, ...]
        """
        result = self.supabase.table('bond_auction') \
            .select('auction_date, allocated_amount') \
            .eq('bond_code', bond_code) \
            .order('auction_date') \
            .execute()

        return result.data

    def get_boj_holdings_history(self, bond_code: str) -> List[Dict]:
        """
        銘柄の日銀保有履歴を取得

        Returns:
            [{'data_date': '2024-01-10', 'face_value': 500}, ...]
        """
        result = self.supabase.table('boj_holdings') \
            .select('data_date, face_value') \
            .eq('bond_code', bond_code) \
            .order('data_date') \
            .execute()

        return result.data

    def get_trade_dates_for_bond(
        self,
        bond_code: str,
        from_date: Optional[str] = None
    ) -> List[str]:
        """銘柄のtrade_date一覧を取得（昇順）"""
        query = self.supabase.table('bond_data') \
            .select('trade_date') \
            .eq('bond_code', bond_code) \
            .order('trade_date')

        if from_date:
            query = query.gte('trade_date', from_date)

        result = query.execute()
        return [r['trade_date'] for r in result.data]

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

    def calculate_for_bond(
        self,
        bond_code: str,
        from_date: Optional[str] = None,
        test_mode: bool = False
    ) -> Dict:
        """
        1銘柄の市中残存額を計算

        Returns:
            {'bond_code': '...', 'updated': 100, 'skipped': 50}
        """
        logger.info(f"Processing bond: {bond_code}")

        # データ取得
        auction_history = self.get_auction_history(bond_code)
        boj_history = self.get_boj_holdings_history(bond_code)
        trade_dates = self.get_trade_dates_for_bond(bond_code, from_date)

        if not auction_history:
            logger.warning(f"  No auction data for {bond_code}")
            return {'bond_code': bond_code, 'updated': 0, 'skipped': len(trade_dates)}

        result = {
            'bond_code': bond_code,
            'updated': 0,
            'skipped': 0,
            'updates': []
        }

        for trade_date in trade_dates:
            # 累積発行額
            cumulative_issuance = self.calculate_cumulative_issuance(
                auction_history, trade_date
            )

            if cumulative_issuance is None:
                # 発行前
                result['skipped'] += 1
                continue

            # 日銀保有額（直近値）
            boj_holding = self.get_latest_boj_holding(boj_history, trade_date)

            # 市中残存額計算
            if boj_holding is not None:
                market_amount = cumulative_issuance - boj_holding
            else:
                # 日銀データなし → 全額が市中に存在
                market_amount = cumulative_issuance

            result['updates'].append({
                'trade_date': trade_date,
                'market_amount': market_amount,
                'cumulative_issuance': cumulative_issuance,
                'boj_holding': boj_holding
            })

        # DB更新
        if not test_mode and result['updates']:
            self._batch_update(bond_code, result['updates'])
            result['updated'] = len(result['updates'])
        elif test_mode:
            result['updated'] = len(result['updates'])
            logger.info(f"  [TEST] Would update {len(result['updates'])} records")

        logger.info(f"  Updated: {result['updated']}, Skipped: {result['skipped']}")
        return result

    def _batch_update(self, bond_code: str, updates: List[Dict], batch_size: int = 100):
        """バッチ更新"""
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]

            for update in batch:
                try:
                    self.supabase.table('bond_data') \
                        .update({'market_amount': update['market_amount']}) \
                        .eq('bond_code', bond_code) \
                        .eq('trade_date', update['trade_date']) \
                        .execute()
                except Exception as e:
                    logger.error(f"Update error for {bond_code}/{update['trade_date']}: {e}")

    def calculate_all(
        self,
        bond_codes: Optional[List[str]] = None,
        from_date: Optional[str] = None,
        test_mode: bool = False
    ) -> Dict:
        """
        全銘柄または指定銘柄の市中残存額を計算

        Returns:
            {'total_bonds': 100, 'total_updated': 5000, 'errors': [...]}
        """
        if bond_codes is None:
            bond_codes = self.get_all_bond_codes()

        total_result = {
            'total_bonds': len(bond_codes),
            'total_updated': 0,
            'total_skipped': 0,
            'errors': []
        }

        for i, bond_code in enumerate(bond_codes):
            logger.info(f"[{i+1}/{len(bond_codes)}] Processing {bond_code}")

            try:
                result = self.calculate_for_bond(
                    bond_code,
                    from_date=from_date,
                    test_mode=test_mode
                )
                total_result['total_updated'] += result['updated']
                total_result['total_skipped'] += result['skipped']
            except Exception as e:
                logger.error(f"Error processing {bond_code}: {e}")
                total_result['errors'].append({
                    'bond_code': bond_code,
                    'error': str(e)
                })

        return total_result


def main():
    parser = argparse.ArgumentParser(
        description='市中残存額（market_amount）計算',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--bond-code', type=str, help='特定の銘柄コードのみ処理')
    parser.add_argument('--from-date', type=str, help='指定日以降のみ再計算 (YYYY-MM-DD)')
    parser.add_argument('--test', action='store_true', help='テストモード（DB更新なし）')
    parser.add_argument('--verbose', '-v', action='store_true', help='詳細ログ出力')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    print("=" * 60)
    print("市中残存額（market_amount）計算")
    print("=" * 60)
    print(f"対象銘柄: {args.bond_code or '全銘柄'}")
    print(f"開始日付: {args.from_date or '全期間'}")
    print(f"モード: {'テスト（DB更新なし）' if args.test else '本番'}")
    print("=" * 60)
    print()

    calculator = MarketAmountCalculator()

    bond_codes = [args.bond_code] if args.bond_code else None

    result = calculator.calculate_all(
        bond_codes=bond_codes,
        from_date=args.from_date,
        test_mode=args.test
    )

    print()
    print("=" * 60)
    print("計算結果")
    print("=" * 60)
    print(f"処理銘柄数: {result['total_bonds']}")
    print(f"更新レコード数: {result['total_updated']}")
    print(f"スキップレコード数: {result['total_skipped']}")

    if result['errors']:
        print(f"\nエラー数: {len(result['errors'])}")
        for err in result['errors'][:5]:
            print(f"  - {err['bond_code']}: {err['error']}")
        if len(result['errors']) > 5:
            print(f"  ... 他 {len(result['errors']) - 5} 件")


if __name__ == '__main__':
    main()
