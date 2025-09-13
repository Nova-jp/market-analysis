#!/usr/bin/env python3
"""
Historical Bond Data Collector
JSDAからヒストリカルデータを取得するクラス
"""

import pandas as pd
import requests
import io
import time
import logging
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Tuple
import os
from dotenv import load_dotenv
import calendar

load_dotenv()

class HistoricalBondCollector:
    """JSDAヒストリカルデータ収集クラス"""
    
    def __init__(self, delay_seconds: float = 1.0):
        self.base_url = "https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/files"
        self.delay_seconds = delay_seconds  # リクエスト間隔
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')
        
        # ログ設定
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # 日本の祝日（簡易版）
        self.holidays = self._get_japanese_holidays()
        
    def _get_japanese_holidays(self) -> List[date]:
        """日本の祝日リストを取得（簡易版）"""
        holidays = []
        
        # 主要な祝日のみ（元日、GW、お盆、年末年始など）
        for year in range(2002, 2026):
            holidays.extend([
                date(year, 1, 1),   # 元日
                date(year, 1, 2),   # 正月休み
                date(year, 1, 3),   # 正月休み
                date(year, 5, 3),   # 憲法記念日
                date(year, 5, 4),   # みどりの日
                date(year, 5, 5),   # こどもの日
                date(year, 12, 29), # 年末休み
                date(year, 12, 30), # 年末休み
                date(year, 12, 31), # 大晦日
            ])
        
        return holidays
    
    def is_business_day(self, check_date: date) -> bool:
        """営業日かどうかを判定"""
        # 土日をチェック
        if check_date.weekday() >= 5:  # 土曜日(5)、日曜日(6)
            return False
        
        # 祝日をチェック
        if check_date in self.holidays:
            return False
            
        return True
    
    def get_business_days_in_range(self, start_date: date, end_date: date) -> List[date]:
        """指定期間の営業日リストを取得"""
        business_days = []
        current_date = start_date
        
        while current_date <= end_date:
            if self.is_business_day(current_date):
                business_days.append(current_date)
            current_date += timedelta(days=1)
        
        return business_days
    
    def build_csv_url(self, target_date: date) -> Tuple[str, str]:
        """指定日のCSVファイルURLを構築"""
        year = target_date.year
        date_str = target_date.strftime("%y%m%d")
        filename = f"S{date_str}.csv"
        url = f"{self.base_url}/{year}/{filename}"
        return url, filename
    
    def download_csv_data(self, url: str, target_date: date) -> Optional[pd.DataFrame]:
        """CSVデータをダウンロード"""
        try:
            self.logger.info(f"データ取得中: {target_date.strftime('%Y-%m-%d')} - {url}")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # 文字コード判定・変換
            try:
                content = response.content.decode('shift_jis')
            except UnicodeDecodeError:
                try:
                    content = response.content.decode('utf-8')
                except UnicodeDecodeError:
                    content = response.content.decode('cp932')
            
            # CSVとしてパース
            df = pd.read_csv(io.StringIO(content), header=None)
            
            if len(df) > 0:
                self.logger.info(f"取得成功: {len(df)}行")
                return df
            else:
                self.logger.warning(f"空のファイル: {target_date}")
                return None
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.logger.info(f"データなし（404）: {target_date}")
            else:
                self.logger.error(f"HTTPエラー {e.response.status_code}: {target_date}")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"接続エラー: {target_date} - {e}")
        except Exception as e:
            self.logger.error(f"予期しないエラー: {target_date} - {e}")
        
        return None
    
    def get_existing_dates(self) -> List[date]:
        """データベース内の既存日付を取得"""
        if not self.supabase_url or not self.supabase_key:
            self.logger.warning("Supabase設定がありません")
            return []
        
        headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}'
        }
        
        try:
            response = requests.get(
                f"{self.supabase_url}/rest/v1/clean_bond_data?select=trade_date&order=trade_date",
                headers=headers
            )
            
            if response.status_code == 200:
                data = response.json()
                existing_dates = [datetime.strptime(item['trade_date'], '%Y-%m-%d').date() 
                                for item in data]
                unique_dates = list(set(existing_dates))
                self.logger.info(f"既存データ: {len(unique_dates)}日分")
                return unique_dates
            else:
                self.logger.error(f"既存データ確認エラー: {response.status_code}")
                
        except Exception as e:
            self.logger.error(f"既存データ確認失敗: {e}")
        
        return []
    
    def collect_historical_data(self, start_date: date, end_date: date, 
                              skip_existing: bool = True) -> Dict[str, int]:
        """ヒストリカルデータの一括取得"""
        
        self.logger.info(f"ヒストリカルデータ取得開始: {start_date} to {end_date}")
        
        # 営業日リスト作成
        business_days = self.get_business_days_in_range(start_date, end_date)
        total_days = len(business_days)
        
        self.logger.info(f"対象営業日数: {total_days}日")
        
        # 既存データ確認
        existing_dates = set()
        if skip_existing:
            existing_dates = set(self.get_existing_dates())
            if existing_dates:
                business_days = [d for d in business_days if d not in existing_dates]
                self.logger.info(f"未取得営業日数: {len(business_days)}日")
        
        # 統計カウンタ
        stats = {
            'attempted': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'total_records': 0
        }
        
        # データ処理用（既存のprocessorを使用）
        from ..processors.bond_data_processor import BondDataProcessor
        processor = BondDataProcessor()
        
        # 一日ずつ処理
        for i, target_date in enumerate(business_days):
            stats['attempted'] += 1
            
            # プログレス表示
            progress = (i + 1) / len(business_days) * 100
            self.logger.info(f"進行状況: {progress:.1f}% ({i+1}/{len(business_days)})")
            
            # データ取得
            url, filename = self.build_csv_url(target_date)
            raw_df = self.download_csv_data(url, target_date)
            
            if raw_df is not None:
                # データ処理
                processed_df = processor.process_raw_data(raw_df)
                
                if not processed_df.empty:
                    # 日付を正しく設定
                    processed_df['trade_date'] = target_date
                    
                    # データベース保存
                    if processor.save_to_supabase(processed_df):
                        stats['successful'] += 1
                        stats['total_records'] += len(processed_df)
                        self.logger.info(f"保存成功: {target_date} ({len(processed_df)}件)")
                    else:
                        stats['failed'] += 1
                        self.logger.error(f"保存失敗: {target_date}")
                else:
                    stats['skipped'] += 1
                    self.logger.warning(f"処理データなし: {target_date}")
            else:
                stats['failed'] += 1
            
            # レート制限（サーバー負荷軽減）
            if i < len(business_days) - 1:  # 最後以外
                time.sleep(self.delay_seconds)
        
        # 結果レポート
        self.logger.info("=" * 50)
        self.logger.info("ヒストリカルデータ取得完了")
        self.logger.info(f"試行: {stats['attempted']}日")
        self.logger.info(f"成功: {stats['successful']}日")
        self.logger.info(f"失敗: {stats['failed']}日")
        self.logger.info(f"スキップ: {stats['skipped']}日")
        self.logger.info(f"総レコード数: {stats['total_records']}件")
        self.logger.info("=" * 50)
        
        return stats
    
    def collect_historical_data_with_adaptive_delay(self, start_date: date, end_date: date, 
                                                  batch_size: int = 10, initial_delay: float = 30,
                                                  skip_existing: bool = True) -> Dict[str, int]:
        """適応的遅延機能付きヒストリカルデータ収集"""
        
        self.logger.info(f"適応的遅延収集開始: {start_date} to {end_date}, 初期間隔: {initial_delay}秒")
        
        # 営業日リスト作成
        business_days = self.get_business_days_in_range(start_date, end_date)
        
        # 既存データ確認
        existing_dates = set()
        if skip_existing:
            existing_dates = set(self.get_existing_dates())
            if existing_dates:
                business_days = [d for d in business_days if d not in existing_dates]
                self.logger.info(f"未取得営業日数: {len(business_days)}日")
        
        # 統計カウンタ
        stats = {
            'attempted': 0,
            'successful': 0,
            'failed': 0,
            'timeout_errors': 0,
            'connection_errors': 0,
            'total_records': 0,
            'delay_adjustments': 0
        }
        
        # 適応的遅延パラメータ
        current_delay = initial_delay
        consecutive_failures = 0
        consecutive_successes = 0
        
        # 遅延調整ルール
        delay_levels = [15, 20, 30, 45, 60, 90, 120, 180, 300]  # 15秒から5分まで
        success_threshold = 3  # 連続成功でレベル下げ
        failure_threshold = 2  # 連続失敗でレベル上げ
        
        # データ処理用
        from ..processors.bond_data_processor import BondDataProcessor
        processor = BondDataProcessor()
        
        self.logger.info(f"📊 対象日数: {len(business_days)}日")
        self.logger.info(f"⚡ 適応的遅延: {delay_levels}秒のレベル調整")
        
        # 日付を順番に処理
        for i, target_date in enumerate(business_days):
            stats['attempted'] += 1
            
            # プログレス表示
            progress = (i + 1) / len(business_days) * 100
            self.logger.info(f"📊 進行状況: {progress:.1f}% ({i+1}/{len(business_days)}) | 現在間隔: {current_delay}秒")
            
            # データ取得試行
            url, filename = self.build_csv_url(target_date)
            raw_df = self.download_csv_data(url, target_date)
            
            success = False
            
            if raw_df is not None:
                # データ処理
                processed_df = processor.process_raw_data(raw_df)
                
                if not processed_df.empty:
                    # 日付を正しく設定
                    processed_df['trade_date'] = target_date
                    
                    # データベース保存
                    if processor.save_to_supabase(processed_df):
                        stats['successful'] += 1
                        stats['total_records'] += len(processed_df)
                        success = True
                        self.logger.info(f"✅ 保存成功: {target_date} ({len(processed_df)}件)")
                    else:
                        stats['failed'] += 1
                        self.logger.error(f"❌ 保存失敗: {target_date}")
                else:
                    success = True  # データなしも成功扱い
                    self.logger.warning(f"⏭️  処理データなし: {target_date}")
            else:
                stats['failed'] += 1
                self.logger.error(f"❌ データ取得失敗: {target_date}")
            
            # 適応的遅延調整
            if success:
                consecutive_successes += 1
                consecutive_failures = 0
                
                # 連続成功で遅延短縮
                if consecutive_successes >= success_threshold:
                    current_level = delay_levels.index(current_delay) if current_delay in delay_levels else len(delay_levels) - 1
                    if current_level > 0:
                        current_delay = delay_levels[current_level - 1]
                        stats['delay_adjustments'] += 1
                        self.logger.info(f"🚀 遅延短縮: {current_delay}秒 (連続成功: {consecutive_successes})")
                        consecutive_successes = 0
            else:
                consecutive_failures += 1
                consecutive_successes = 0
                
                # 連続失敗による遅延調整
                if consecutive_failures >= failure_threshold:
                    # タイムアウトが発生した場合は即座に2分休憩
                    if consecutive_failures >= 3:  # 3回連続失敗で2分休憩
                        self.logger.warning("💤 タイムアウト多発検知: 2分間休憩してリセットします...")
                        time.sleep(120)  # 2分休憩
                        current_delay = 15  # 15秒でリセット
                        consecutive_failures = 0
                        consecutive_successes = 0
                        stats['delay_adjustments'] += 1
                        self.logger.info("🔄 休憩完了: 15秒間隔でリセット再開")
                    else:
                        # 通常の連続失敗処理
                        current_level = delay_levels.index(current_delay) if current_delay in delay_levels else 0
                        if current_level < len(delay_levels) - 1:
                            current_delay = delay_levels[current_level + 1]
                            stats['delay_adjustments'] += 1
                            self.logger.warning(f"⏰ 遅延延長: {current_delay}秒 (連続失敗: {consecutive_failures})")
                            consecutive_failures = 0
            
            # 次のリクエストまで待機
            if i < len(business_days) - 1:
                self.logger.info(f"⏱️  {current_delay}秒待機中...")
                time.sleep(current_delay)
        
        # 結果レポート
        self.logger.info("=" * 60)
        self.logger.info("🎉 適応的遅延収集完了")
        self.logger.info(f"🎯 試行: {stats['attempted']}日")
        self.logger.info(f"✅ 成功: {stats['successful']}日")
        self.logger.info(f"❌ 失敗: {stats['failed']}日")
        self.logger.info(f"⏰ タイムアウト: {stats['timeout_errors']}件")
        self.logger.info(f"🔌 接続エラー: {stats['connection_errors']}件") 
        self.logger.info(f"💾 総レコード数: {stats['total_records']}件")
        self.logger.info(f"⚡ 遅延調整回数: {stats['delay_adjustments']}回")
        self.logger.info(f"🎯 最終間隔: {current_delay}秒")
        self.logger.info("=" * 60)
        
        return stats

    def collect_historical_data_in_batches(self, start_date: date, end_date: date, 
                                         batch_size: int = 10, delay_seconds: float = 300,
                                         skip_existing: bool = True) -> Dict[str, int]:
        """バッチ処理でのヒストリカルデータ収集（中断・再開対応）"""
        
        self.logger.info(f"バッチ処理開始: {start_date} to {end_date}, バッチサイズ: {batch_size}")
        
        # 営業日リスト作成
        business_days = self.get_business_days_in_range(start_date, end_date)
        total_days = len(business_days)
        
        # 既存データ確認
        existing_dates = set()
        if skip_existing:
            existing_dates = set(self.get_existing_dates())
            if existing_dates:
                business_days = [d for d in business_days if d not in existing_dates]
                self.logger.info(f"未取得営業日数: {len(business_days)}日")
        
        # 統計カウンタ
        stats = {
            'attempted': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'total_records': 0,
            'batches_completed': 0
        }
        
        # データ処理用
        from ..processors.bond_data_processor import BondDataProcessor
        processor = BondDataProcessor()
        
        # バッチ単位で処理
        for batch_start in range(0, len(business_days), batch_size):
            batch_end = min(batch_start + batch_size, len(business_days))
            batch_dates = business_days[batch_start:batch_end]
            current_batch = (batch_start // batch_size) + 1
            total_batches = (len(business_days) + batch_size - 1) // batch_size
            
            self.logger.info(f"=" * 60)
            self.logger.info(f"📦 バッチ {current_batch}/{total_batches} 開始")
            self.logger.info(f"📅 期間: {batch_dates[0]} ～ {batch_dates[-1]} ({len(batch_dates)}日)")
            self.logger.info(f"=" * 60)
            
            batch_success = 0
            
            # バッチ内の各日付を処理
            for i, target_date in enumerate(batch_dates):
                stats['attempted'] += 1
                
                # プログレス表示
                batch_progress = (i + 1) / len(batch_dates) * 100
                overall_progress = (batch_start + i + 1) / len(business_days) * 100
                
                self.logger.info(f"📊 バッチ進行: {batch_progress:.1f}% | 全体: {overall_progress:.1f}% ({batch_start + i + 1}/{len(business_days)})")
                
                # データ取得
                url, filename = self.build_csv_url(target_date)
                raw_df = self.download_csv_data(url, target_date)
                
                if raw_df is not None:
                    # データ処理
                    processed_df = processor.process_raw_data(raw_df)
                    
                    if not processed_df.empty:
                        # 日付を正しく設定
                        processed_df['trade_date'] = target_date
                        
                        # データベース保存
                        if processor.save_to_supabase(processed_df):
                            stats['successful'] += 1
                            stats['total_records'] += len(processed_df)
                            batch_success += 1
                            self.logger.info(f"✅ 保存成功: {target_date} ({len(processed_df)}件)")
                        else:
                            stats['failed'] += 1
                            self.logger.error(f"❌ 保存失敗: {target_date}")
                    else:
                        stats['skipped'] += 1
                        self.logger.warning(f"⏭️  処理データなし: {target_date}")
                else:
                    stats['failed'] += 1
                
                # レート制限（最後の日付以外）
                if i < len(batch_dates) - 1:
                    self.logger.info(f"⏱️  {delay_seconds}秒待機中...")
                    time.sleep(delay_seconds)
            
            # バッチ完了
            stats['batches_completed'] += 1
            self.logger.info(f"🎯 バッチ {current_batch} 完了: {batch_success}/{len(batch_dates)}日成功")
            
            # 最後のバッチでない場合、次のバッチまで少し待機
            if batch_end < len(business_days):
                extra_wait = 60  # 1分追加待機
                self.logger.info(f"💤 次のバッチまで{extra_wait}秒待機...")
                time.sleep(extra_wait)
        
        # 結果レポート
        self.logger.info("=" * 60)
        self.logger.info("🎉 バッチ処理完了")
        self.logger.info(f"📦 完了バッチ: {stats['batches_completed']}")
        self.logger.info(f"🎯 試行: {stats['attempted']}日")
        self.logger.info(f"✅ 成功: {stats['successful']}日")
        self.logger.info(f"❌ 失敗: {stats['failed']}日")
        self.logger.info(f"⏭️  スキップ: {stats['skipped']}日")
        self.logger.info(f"💾 総レコード数: {stats['total_records']}件")
        self.logger.info("=" * 60)
        
        return stats

def main():
    """メイン実行"""
    collector = HistoricalBondCollector(delay_seconds=1.5)
    
    # デフォルト: 2020年からの5年分のデータ
    start_date = date(2020, 1, 1)
    end_date = date.today()
    
    print(f"JSDAヒストリカルデータ取得")
    print(f"期間: {start_date} ～ {end_date}")
    
    # ユーザー確認
    response = input("この期間でデータ取得を開始しますか？ (y/n): ")
    if response.lower() != 'y':
        print("取得をキャンセルしました")
        return
    
    # データ取得実行
    stats = collector.collect_historical_data(start_date, end_date)
    
    print("\n🎉 取得完了！")
    print(f"成功: {stats['successful']}日")
    print(f"総レコード: {stats['total_records']}件")

if __name__ == "__main__":
    main()