#!/usr/bin/env python3
"""
毎日18時自動データ収集スクリプト
JSDAから最新の国債金利データを取得してSupabaseに格納
"""
import os
import sys
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from data.processors.bond_data_processor import BondDataProcessor
from data.utils.database_manager import DatabaseManager
from data.collectors.boj.holdings_collector import BOJHoldingsCollector
from data.collectors.mof.bond_auction_web_collector import BondAuctionWebCollector

# JST定義
JST = timezone(timedelta(hours=9))

# ログ設定（Cloud Run対応：ファイル出力なし、標準出力のみ）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DailyDataCollector:
    """毎日のデータ収集管理クラス"""

    def __init__(self):
        self.processor = BondDataProcessor()
        self.db_manager = DatabaseManager()
        self.boj_collector = BOJHoldingsCollector(delay_seconds=2.0)
        self.mof_collector = BondAuctionWebCollector()

    def get_target_date(self):
        """
        取得対象日付を決定
        祝日・土日をスキップして次の平日を取得
        """
        import jpholiday

        # 翌日から開始 (JST基準)
        target_date = datetime.now(JST) + timedelta(days=1)

        # 最大10日先まで確認（無限ループ防止）
        for _ in range(10):
            # 平日かつ祝日でない場合
            if target_date.weekday() < 5 and not jpholiday.is_holiday(target_date.date()):
                result = target_date.strftime('%Y-%m-%d')
                logger.info(f"Target date determined: {result} (平日)")
                return result

            # 祝日・土日の場合はスキップ
            if target_date.weekday() >= 5:
                logger.info(f"Skipping weekend: {target_date.strftime('%Y-%m-%d')} ({target_date.strftime('%A')})")
            elif jpholiday.is_holiday(target_date.date()):
                holiday_name = jpholiday.is_holiday_name(target_date.date())
                logger.info(f"Skipping holiday: {target_date.strftime('%Y-%m-%d')} ({holiday_name})")

            # 翌日に進む
            target_date += timedelta(days=1)

        # 10日先まで平日が見つからない場合（通常ありえない）
        fallback = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        logger.warning(f"Could not find weekday within 10 days, using fallback: {fallback}")
        return fallback

    def collect_daily_data(self):
        """日次データ収集のメイン処理"""
        try:
            logger.info("=== 日次データ収集開始 ===")
            
            overall_success = True

            # 1. JSDAデータ収集
            target_date_str = self.get_target_date()
            jsda_saved = self.collect_single_day_data(target_date_str)
            
            if jsda_saved < 0:
                logger.error("JSDAデータ収集失敗")
                overall_success = False
            elif jsda_saved == 0:
                logger.info("JSDAデータなし（休日の可能性）")
            else:
                logger.info(f"JSDAデータ収集成功: {jsda_saved}件")

            # 2. MOF入札データ収集 (独立して実行)
            if not self.mof_collector.sync_with_database():
                logger.warning("MOFデータ収集でエラーが発生しましたが、処理を継続します")

            # 3. BOJ保有データ収集 (独立して実行)
            if not self.boj_collector.sync_with_database():
                logger.warning("BOJデータ収集でエラーが発生しましたが、処理を継続します")

            logger.info(f"=== 日次データ収集完了 (JSDA成功: {overall_success}) ===")
            return overall_success

        except Exception as e:
            logger.error(f"日次データ収集中に致命的エラー発生: {e}")
            return False

    def _calculate_market_amount_for_record(self, bond_code: str, trade_date: str):
        """
        1レコード分のmarket_amountを計算

        Args:
            bond_code: 銘柄コード
            trade_date: 取引日 (YYYY-MM-DD)

        Returns:
            market_amount (億円), 計算不可時はNone
        """
        try:
            # MarketAmountCalculatorクラスのロジックを使用
            from data.utils.market_amount_calculator import MarketAmountCalculator

            calculator = MarketAmountCalculator()
            auction_history = calculator.get_auction_history(bond_code)
            boj_history = calculator.get_boj_holdings_history(bond_code)

            # 累積発行額
            cumulative = calculator.calculate_cumulative_issuance(
                auction_history, trade_date
            )
            if cumulative is None:
                return None  # 発行前

            # 日銀保有額
            boj_holding = calculator.get_latest_boj_holding(boj_history, trade_date)

            # 市中残存額
            if boj_holding is not None:
                return cumulative - boj_holding
            else:
                return cumulative

        except Exception as e:
            logger.error(f"market_amount計算エラー ({bond_code}, {trade_date}): {e}")
            return None

    def collect_single_day_data(self, target_date_str, retry_count=1):
        """
        1日分のデータ収集（simple_multi_day_collector.pyと同じロジック）
        """
        from datetime import date

        max_retries = retry_count

        for attempt in range(max_retries + 1):
            try:
                logger.info(f"📅 収集中: {target_date_str} (試行 {attempt + 1}/{max_retries + 1})")

                # 日付オブジェクトに変換
                target_date = date.fromisoformat(target_date_str)

                # 1. CSVデータ取得
                # download_data_for_dateを使用（年またぎ対応）
                raw_df = self.processor.download_data_for_date(target_date)

                if raw_df is None:
                    logger.warning(f"  ❌ CSVデータ取得失敗: {target_date_str}")
                    if attempt < max_retries:
                        logger.info("  ⏱️  5秒待機してリトライ")
                        time.sleep(5)
                    continue

                if raw_df.empty:
                    logger.info(f"  📭 データが空（休日の可能性）: {target_date_str}")
                    return 0

                # 2. データ処理
                processed_df = self.processor.process_raw_data(raw_df)

                if processed_df.empty:
                    logger.info(f"  📭 処理後データが空: {target_date_str}")
                    return 0

                # 3. trade_dateの検証と補完
                
                # HTML情報に基づく正確な取引日の特定（2026/1/5問題対応）
                actual_trade_date = None
                try:
                    actual_trade_date = self.processor.determine_trade_date_from_html(target_date)
                except Exception as e:
                    logger.warning(f"  ⚠️  HTML日付特定失敗: {e}")

                if actual_trade_date:
                    logger.info(f"  📅 HTML情報により取引日を修正: {target_date_str} -> {actual_trade_date}")
                    processed_df['trade_date'] = actual_trade_date.isoformat()
                elif 'trade_date' in processed_df.columns:
                    # CSV内の日付とターゲット日付が一致するか確認（ログ出力のみ）
                    csv_dates = processed_df['trade_date'].unique()
                    if len(csv_dates) > 0 and csv_dates[0] != target_date_str:
                        logger.warning(f"  ⚠️  日付不一致: CSV内={csv_dates[0]}, 指定={target_date_str}")
                        # JSDAの仕様上、ファイル名の日付(公表日)と中身の日付(公表日)は一致するはずだが、
                        # 万が一不一致でも、管理上の日付(target_date_str)を優先する場合は上書きする。
                        # ここではプロジェクトの慣習に従い、引数で指定された日付を優先して上書きする。
                        processed_df['trade_date'] = target_date_str
                else:
                    # trade_dateがない場合は付与
                    processed_df['trade_date'] = target_date_str

                # 4. データベース保存前にmarket_amount計算
                batch_data = processed_df.to_dict('records')

                # 各レコードにmarket_amountを追加
                logger.info(f"  🔢 市中残存額計算中: {len(batch_data)}件")
                for record in batch_data:
                    bond_code = record.get('bond_code')
                    if bond_code:
                        market_amount = self._calculate_market_amount_for_record(
                            bond_code, target_date_str
                        )
                        record['market_amount'] = market_amount

                # 5. データベース保存
                saved_count = self.db_manager.batch_insert_data(batch_data)

                if saved_count > 0:
                    logger.info(f"  ✅ 成功: {saved_count}件保存")
                    return saved_count
                else:
                    logger.warning(f"  ⚠️  保存失敗: {target_date_str}")
                    return -1

            except Exception as e:
                error_msg = str(e)
                is_timeout = "timeout" in error_msg.lower() or "connection" in error_msg.lower()

                if attempt < max_retries:
                    if is_timeout:
                        logger.warning(f"  ⏱️  タイムアウトエラー (試行 {attempt + 1}): {error_msg}")
                        logger.info("  ⏱️  5分待機してリトライ")
                        time.sleep(300)  # 5分待機
                    else:
                        logger.warning(f"  ⚠️  一般エラー (試行 {attempt + 1}): {error_msg}")
                        logger.info("  ⏱️  3分待機してリトライ")
                        time.sleep(180)  # 3分待機
                else:
                    logger.error(f"  ❌ 最大リトライ回数に達しました: {error_msg}")
                    return -1

        return -1

    def run_safety_checks(self):
        """実行前の安全確認"""
        try:
            # 他のPythonプロセスチェック
            import subprocess
            try:
                result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
                processes = result.stdout
                jsda_processes = [line for line in processes.split('\n')
                                if 'python' in line and ('collector' in line or 'jsda' in line.lower())]

                if len(jsda_processes) > 1:  # 現在のプロセス以外にもある
                    logger.warning("他のJSDA収集プロセスが実行中の可能性があります")
                    for proc in jsda_processes:
                        logger.info(f"  プロセス: {proc.strip()}")

            except Exception as e:
                logger.warning(f"プロセスチェック中にエラー: {e}")

            # DB接続確認
            try:
                # データベースマネージャーのテスト（レコード数取得で接続確認）
                record_count = self.db_manager.get_total_record_count()
                if record_count >= 0:
                    logger.info(f"データベース接続確認: {record_count}件のレコードが存在")
                else:
                    logger.warning("データベース接続は成功しましたが、レコード数取得に失敗")

            except Exception as e:
                logger.error(f"データベース接続テスト失敗: {e}")
                return False

            logger.info("安全確認チェック完了")
            return True

        except Exception as e:
            logger.error(f"安全確認チェック中にエラー: {e}")
            return False


def main():
    """メイン実行関数"""
    logger.info("Daily Data Collector Started")

    try:
        collector = DailyDataCollector()

        # 実行前安全確認
        if not collector.run_safety_checks():
            logger.error("安全確認チェックに失敗しました。実行を中止します。")
            sys.exit(1)

        # データ収集実行
        success = collector.collect_daily_data()

        if success:
            logger.info("日次データ収集が正常に完了しました")
            sys.exit(0)
        else:
            logger.error("日次データ収集に失敗しました")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("ユーザーによって処理が中断されました")
        sys.exit(1)
    except Exception as e:
        logger.error(f"予期しないエラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()