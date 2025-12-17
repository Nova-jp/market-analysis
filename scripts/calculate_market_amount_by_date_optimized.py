#!/usr/bin/env python3
"""
market_amount 日付ごと再計算スクリプト (Supabase PRO版最適化)

アルゴリズム:
1. 各日付ごとに処理
2. その日付の全銘柄について:
   - 累積発行額 = SUM(bond_auction.total_amount WHERE auction_date <= trade_date)
   - 日銀保有額 = LATEST(boj_holdings.face_value WHERE data_date <= trade_date)
   - market_amount = 累積発行額 - 日銀保有額

特徴:
- 既に計算済み（Phase 2-2で完了分）はスキップ
- 日付ごとに処理するため、途中で止まっても再開可能
- PRO版の性能を活用（バッチサイズ大きめ）

使用方法:
    python scripts/calculate_market_amount_by_date_optimized.py
"""

import os
import sys
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
import logging
from datetime import datetime
from typing import List, Dict

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/market_amount_by_date.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 環境変数読み込み
load_dotenv()


class DateBasedCalculator:
    """日付ごとにmarket_amountを計算"""

    def __init__(self):
        url = os.getenv('SUPABASE_URL')
        key = os.getenv('SUPABASE_KEY')

        if not url or not key:
            raise ValueError("環境変数 SUPABASE_URL, SUPABASE_KEY が設定されていません")

        self.supabase: Client = create_client(url, key)
        logger.info("✅ Supabase接続成功")

    def get_uncalculated_dates(self) -> List[str]:
        """
        market_amountが未計算（NULL）の日付一覧を取得

        Returns:
            List[str]: 日付リスト（昇順）
        """
        logger.info("📅 未計算日付の取得中...")

        try:
            # market_amount が NULL のレコードから、ユニークな日付を取得
            response = self.supabase.rpc(
                'get_uncalculated_dates'
            ).execute()

            if response.data:
                dates = [row['trade_date'] for row in response.data]
                logger.info(f"  ✅ 未計算日付数: {len(dates)}日")
                return dates
            else:
                # RPC関数がない場合の代替処理
                logger.warning("  ⚠️  RPC関数が見つかりません。代替方法で取得します...")
                return self._get_uncalculated_dates_fallback()

        except Exception as e:
            logger.warning(f"  ⚠️  RPC取得エラー: {e}")
            logger.info("  代替方法で取得します...")
            return self._get_uncalculated_dates_fallback()

    def _get_uncalculated_dates_fallback(self) -> List[str]:
        """代替方法: Pythonで日付一覧を取得"""
        logger.info("  📥 代替方法: bond_dataから未計算日付を抽出中...")

        all_dates = set()
        offset = 0
        limit = 10000

        while True:
            response = self.supabase.table('bond_data') \
                .select('trade_date') \
                .is_('market_amount', 'null') \
                .range(offset, offset + limit - 1) \
                .execute()

            if not response.data:
                break

            for row in response.data:
                all_dates.add(row['trade_date'])

            logger.info(f"    進捗: offset={offset:,}, ユニーク日付数={len(all_dates)}")

            if len(response.data) < limit:
                break

            offset += limit

        dates = sorted(list(all_dates))
        logger.info(f"  ✅ 未計算日付数: {len(dates)}日")
        return dates

    def get_bonds_on_date(self, trade_date: str) -> List[str]:
        """
        指定日付の銘柄一覧を取得（market_amount が NULL のもののみ）

        Args:
            trade_date: 取引日

        Returns:
            List[str]: 銘柄コードリスト
        """
        response = self.supabase.table('bond_data') \
            .select('bond_code') \
            .eq('trade_date', trade_date) \
            .is_('market_amount', 'null') \
            .execute()

        return [row['bond_code'] for row in response.data]

    def calculate_cumulative_issuance(self, bond_code: str, trade_date: str) -> int:
        """
        累積発行額を計算

        Args:
            bond_code: 銘柄コード
            trade_date: 取引日

        Returns:
            int: 累積発行額
        """
        response = self.supabase.table('bond_auction') \
            .select('total_amount') \
            .eq('bond_code', bond_code) \
            .lte('auction_date', trade_date) \
            .execute()

        if response.data:
            return int(sum(row['total_amount'] for row in response.data))
        return 0

    def get_latest_boj_holding(self, bond_code: str, trade_date: str) -> int:
        """
        最新の日銀保有額を取得

        Args:
            bond_code: 銘柄コード
            trade_date: 取引日

        Returns:
            int: 日銀保有額
        """
        response = self.supabase.table('boj_holdings') \
            .select('face_value') \
            .eq('bond_code', bond_code) \
            .lte('data_date', trade_date) \
            .order('data_date', desc=True) \
            .limit(1) \
            .execute()

        if response.data and len(response.data) > 0:
            return response.data[0]['face_value']
        return 0

    def update_market_amount(self, bond_code: str, trade_date: str, market_amount: int):
        """
        market_amountを更新

        Args:
            bond_code: 銘柄コード
            trade_date: 取引日
            market_amount: 市中残存額
        """
        self.supabase.table('bond_data') \
            .update({'market_amount': market_amount}) \
            .eq('bond_code', bond_code) \
            .eq('trade_date', trade_date) \
            .execute()

    def process_date(self, trade_date: str, date_index: int, total_dates: int) -> int:
        """
        1日分の処理

        Args:
            trade_date: 取引日
            date_index: 日付のインデックス（進捗表示用）
            total_dates: 総日付数

        Returns:
            int: 更新件数
        """
        logger.info(f"\n📦 日付 {date_index}/{total_dates}: {trade_date}")
        logger.info("-" * 70)

        # その日付の銘柄一覧を取得
        bond_codes = self.get_bonds_on_date(trade_date)

        if not bond_codes:
            logger.info(f"  ⚠️  未計算の銘柄なし（スキップ）")
            return 0

        logger.info(f"  📊 銘柄数: {len(bond_codes)}銘柄")

        updated_count = 0

        # 各銘柄について計算
        for i, bond_code in enumerate(bond_codes, 1):
            try:
                # 累積発行額を計算
                cumulative = self.calculate_cumulative_issuance(bond_code, trade_date)

                # 日銀保有額を取得
                boj_holding = self.get_latest_boj_holding(bond_code, trade_date)

                # market_amount を計算（int型に明示的に変換）
                market_amount = int(cumulative - boj_holding)

                # 更新
                self.update_market_amount(bond_code, trade_date, market_amount)

                updated_count += 1

                # 進捗表示（100件ごと）
                if i % 100 == 0:
                    logger.info(f"    進捗: {i}/{len(bond_codes)}銘柄処理完了")

            except Exception as e:
                logger.error(f"    ❌ エラー: {bond_code} - {e}")
                continue

        logger.info(f"  ✅ {trade_date} 完了: {updated_count}件更新")
        return updated_count

    def run(self):
        """メイン処理"""
        logger.info("======================================================================")
        logger.info("market_amount 日付ごと再計算 (Supabase PRO版)")
        logger.info("======================================================================")
        logger.info("  方式: 日付ごとに処理（既計算分はスキップ）")
        logger.info("  対象: market_amount が NULL のレコードのみ")
        logger.info("======================================================================")
        logger.info("")

        start_time = datetime.now()

        # 未計算の日付一覧を取得
        dates = self.get_uncalculated_dates()

        if not dates:
            logger.info("✅ すべての日付で計算済みです！")
            return

        logger.info(f"\n📅 処理対象: {len(dates)}日分")
        logger.info(f"  開始日: {dates[0]}")
        logger.info(f"  終了日: {dates[-1]}")
        logger.info("")

        total_updated = 0

        # 各日付を処理
        for i, trade_date in enumerate(dates, 1):
            updated = self.process_date(trade_date, i, len(dates))
            total_updated += updated

        # 完了
        end_time = datetime.now()
        elapsed = (end_time - start_time).total_seconds()

        logger.info("")
        logger.info("=" * 70)
        logger.info("🎉 処理完了！")
        logger.info("=" * 70)
        logger.info(f"  更新レコード数: {total_updated:,}件")
        logger.info(f"  処理日数: {len(dates)}日")
        logger.info(f"  実行時間: {elapsed:.1f}秒 ({elapsed/60:.1f}分)")
        logger.info(f"  平均速度: {total_updated/elapsed:.1f}件/秒")
        logger.info("=" * 70)


def main():
    """エントリーポイント"""
    try:
        calculator = DateBasedCalculator()
        calculator.run()
    except Exception as e:
        logger.error(f"❌ 致命的エラー: {e}")
        raise


if __name__ == '__main__':
    main()
