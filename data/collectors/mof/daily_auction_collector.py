#!/usr/bin/env python3
"""
MOF (財務省) 日次入札結果収集サービス

カレンダーに基づいて、その日の入札結果を自動収集する。
Cloud Schedulerから毎日12:36に呼び出される想定。

機能:
- カレンダーから今日の入札予定を確認
- 入札種類に応じて適切なコレクターを呼び出し
- 結果をDBに保存
"""

import sys
from pathlib import Path
from datetime import date, datetime
from typing import List, Dict, Optional
import logging

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from data.collectors.mof.calendar_collector import AuctionCalendarCollector
from data.collectors.mof.liquidity_supply_collector import LiquiditySupplyCollector
from data.collectors.mof.tdb_collector import TDBCollector
from data.collectors.mof.bond_auction_web_collector import BondAuctionWebCollector
from data.utils.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class DailyAuctionCollector:
    """
    日次入札結果収集サービス

    カレンダーを確認し、今日の入札があれば結果を収集してDBに保存する。
    """

    def __init__(self):
        self.calendar = AuctionCalendarCollector()
        self.liquidity_collector = LiquiditySupplyCollector()
        self.tdb_collector = TDBCollector()
        self.normal_collector = BondAuctionWebCollector()
        self.db = DatabaseManager()

    def collect_for_date(self, target_date: date = None) -> Dict:
        """
        指定日の入札結果を収集

        Args:
            target_date: 対象日（デフォルト: 今日）

        Returns:
            {
                'date': '2024-11-05',
                'auctions_scheduled': 2,
                'auctions_collected': 2,
                'results': [
                    {'type': 'normal', 'status': 'success', 'records': 1},
                    {'type': 'tap', 'status': 'success', 'records': 5},
                ],
                'errors': []
            }
        """
        if target_date is None:
            target_date = date.today()

        logger.info(f"=== 日次入札結果収集開始: {target_date} ===")

        result = {
            'date': str(target_date),
            'auctions_scheduled': 0,
            'auctions_collected': 0,
            'results': [],
            'errors': []
        }

        # カレンダーから今日の入札を取得
        auctions = self.calendar.get_auctions_for_date(target_date)

        if not auctions:
            logger.info(f"本日({target_date})の入札予定はありません")
            return result

        result['auctions_scheduled'] = len(auctions)
        logger.info(f"本日の入札予定: {len(auctions)}件")

        for auction in auctions:
            logger.info(f"  - {auction['auction_type']}: {auction['description']}")

        # 入札種類ごとに収集
        auction_types = set(a['auction_type'] for a in auctions)

        for auction_type in auction_types:
            try:
                collect_result = self._collect_by_type(auction_type, target_date)
                result['results'].append(collect_result)
                if collect_result['status'] == 'success':
                    result['auctions_collected'] += 1
            except Exception as e:
                error_msg = f"{auction_type}: {str(e)}"
                logger.error(f"収集エラー - {error_msg}")
                result['errors'].append(error_msg)
                result['results'].append({
                    'type': auction_type,
                    'status': 'error',
                    'message': str(e)
                })

        logger.info(f"=== 収集完了: {result['auctions_collected']}/{result['auctions_scheduled']} ===")
        return result

    def _collect_by_type(self, auction_type: str, target_date: date) -> Dict:
        """入札種類に応じたコレクターを呼び出し"""

        if auction_type == 'tap':
            return self._collect_liquidity_supply(target_date)
        elif auction_type == 'tdb':
            return self._collect_tdb(target_date)
        elif auction_type == 'normal':
            return self._collect_normal_auction(target_date)
        elif auction_type == 'gx':
            # GX経済移行債は通常入札と同じコレクターで収集
            return self._collect_normal_auction(target_date)
        else:
            return {
                'type': auction_type,
                'status': 'skipped',
                'message': f'未対応の入札種類: {auction_type}'
            }

    def _collect_liquidity_supply(self, target_date: date) -> Dict:
        """流動性供給入札の収集"""
        logger.info("流動性供給入札の結果を収集中...")

        try:
            # 既存のコレクターを使用（collect_all_dataで全データ取得）
            # 注: 最新データのみ取得するメソッドがないため、全データから該当日をフィルタ
            records = self.liquidity_collector.collect_all_data()

            if records:
                # 対象日のデータのみフィルタ
                target_records = [
                    r for r in records
                    if r.get('auction_date') == str(target_date)
                ]

                if not target_records:
                    return {
                        'type': 'tap',
                        'status': 'no_data',
                        'message': f'{target_date}のデータなし'
                    }

                # DBに保存
                saved = 0
                for record in target_records:
                    if self._save_auction_record(record, 'tap'):
                        saved += 1

                return {
                    'type': 'tap',
                    'status': 'success',
                    'records': saved
                }
            else:
                return {
                    'type': 'tap',
                    'status': 'no_data',
                    'message': '結果データなし'
                }
        except Exception as e:
            logger.error(f"流動性供給入札収集エラー: {e}")
            raise

    def _collect_tdb(self, target_date: date) -> Dict:
        """国庫短期証券の収集"""
        logger.info("国庫短期証券の結果を収集中...")

        try:
            # 既存のコレクターを使用（collect_all_dataで全データ取得）
            records = self.tdb_collector.collect_all_data()

            if records:
                # 対象日のデータのみフィルタ
                target_records = [
                    r for r in records
                    if r.get('auction_date') == str(target_date)
                ]

                if not target_records:
                    return {
                        'type': 'tdb',
                        'status': 'no_data',
                        'message': f'{target_date}のデータなし'
                    }

                saved = 0
                for record in target_records:
                    if self._save_auction_record(record, 'TDB'):
                        saved += 1

                return {
                    'type': 'tdb',
                    'status': 'success',
                    'records': saved
                }
            else:
                return {
                    'type': 'tdb',
                    'status': 'no_data',
                    'message': '結果データなし'
                }
        except Exception as e:
            logger.error(f"国庫短期証券収集エラー: {e}")
            raise

    def _collect_normal_auction(self, target_date: date) -> Dict:
        """通常国債入札の収集"""
        logger.info("通常国債入札の結果を収集中...")

        try:
            # 既存のコレクターを使用（collect_auction_dataで指定日のデータ取得）
            records = self.normal_collector.collect_auction_data(target_date)

            if records:
                saved = 0
                for record in records:
                    if self._save_auction_record(record, 'normal'):
                        saved += 1

                return {
                    'type': 'normal',
                    'status': 'success',
                    'records': saved
                }
            else:
                return {
                    'type': 'normal',
                    'status': 'no_data',
                    'message': '結果データなし'
                }
        except Exception as e:
            logger.error(f"通常国債入札収集エラー: {e}")
            raise

    def _save_auction_record(self, record: Dict, auction_type: str) -> bool:
        """レコードをDBに保存"""
        try:
            record['auction_type'] = auction_type

            # 必須フィールドの確認
            required = ['bond_code', 'auction_date']
            for field in required:
                if field not in record:
                    logger.warning(f"必須フィールド欠落: {field}")
                    return False

            # bond_codeを9桁に正規化
            if 'bond_code' in record:
                record['bond_code'] = str(record['bond_code']).zfill(9)

            # batch_insert_dataでUPSERT（on_conflict='bond_code,auction_date'）
            result = self.db.batch_insert_data(
                [record],
                table_name='bond_auction',
                on_conflict='bond_code,auction_date'
            )
            return result.get('inserted', 0) > 0 or result.get('updated', 0) > 0
        except Exception as e:
            logger.error(f"DB保存エラー: {e}")
            return False


def main():
    """メイン実行"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    collector = DailyAuctionCollector()

    # 今日の入札を収集
    result = collector.collect_for_date()

    print("\n" + "=" * 60)
    print("日次入札結果収集レポート")
    print("=" * 60)
    print(f"対象日: {result['date']}")
    print(f"予定入札数: {result['auctions_scheduled']}")
    print(f"収集成功数: {result['auctions_collected']}")

    if result['results']:
        print("\n詳細:")
        for r in result['results']:
            status = r.get('status', 'unknown')
            records = r.get('records', 0)
            print(f"  - {r['type']}: {status} ({records}件)")

    if result['errors']:
        print("\nエラー:")
        for e in result['errors']:
            print(f"  - {e}")

    print("=" * 60)


if __name__ == "__main__":
    main()
