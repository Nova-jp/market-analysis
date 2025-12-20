#!/usr/bin/env python3
"""
シンプル複数日データ収集スクリプト
collect_single_day.pyをベースに作成
"""

import sys
import os
import json
import time
from datetime import datetime
import logging

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.processors.bond_data_processor import BondDataProcessor
from data.utils.database_manager import DatabaseManager

def setup_logging():
    """ログ設定"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'data_collection_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )
    return logging.getLogger(__name__)

def load_target_dates(json_file_path):
    """JSONファイルから対象日付を読み込み"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        target_dates = data.get('target_dates', [])
        logger.info(f"📂 ターゲット日付読み込み: {len(target_dates)}日分")
        return target_dates
    
    except Exception as e:
        logger.error(f"❌ JSONファイル読み込みエラー: {e}")
        return []

def collect_single_day_data(processor, db_manager, target_date_str, retry_count=1):
    """
    1日分のデータ収集（collect_single_day.pyと同じロジック）
    """
    from datetime import date
    
    max_retries = retry_count
    
    for attempt in range(max_retries + 1):
        try:
            logger.info(f"📅 収集中: {target_date_str} (試行 {attempt + 1}/{max_retries + 1})")
            
            # 日付オブジェクトに変換
            target_date = date.fromisoformat(target_date_str)
            
            # 1. CSVデータ取得
            url, filename = processor.build_csv_url(target_date)
            raw_df = processor.download_csv_data(url)
            
            if raw_df is None:
                logger.warning(f"  ❌ CSVデータ取得失敗: {target_date_str}")
                continue
            
            if raw_df.empty:
                logger.info(f"  📭 データが空（休日の可能性）: {target_date_str}")
                return 0
            
            # 2. データ処理
            processed_df = processor.process_raw_data(raw_df)
            
            if processed_df.empty:
                logger.info(f"  📭 処理後データが空: {target_date_str}")
                return 0
            
            # 3. trade_date追加
            processed_df['trade_date'] = target_date_str
            
            # 4. データベース保存
            batch_data = processed_df.to_dict('records')
            saved_count = db_manager.batch_insert_data(batch_data)
            
            if saved_count > 0:
                logger.info(f"  ✅ 成功: {saved_count}件保存")
                return saved_count
            else:
                logger.warning(f"  ⚠️  保存失敗: {target_date_str}")
                return 0
                
        except Exception as e:
            error_msg = str(e)
            is_timeout = "timeout" in error_msg.lower() or "connection" in error_msg.lower()
            
            if attempt < max_retries:
                if is_timeout:
                    logger.warning(f"  ⏱️  タイムアウトエラー (試行 {attempt + 1}): {error_msg}")
                    logger.info(f"  ⏱️  5分待機してリトライ...")
                    time.sleep(300)  # 5分待機（タイムアウト時）
                else:
                    logger.warning(f"  ❌ エラー (試行 {attempt + 1}): {error_msg}")
                    logger.info(f"  ⏱️  3分待機してリトライ...")
                    time.sleep(180)  # 3分待機（通常エラー）
            else:
                logger.error(f"  ❌ 最終的に失敗: {target_date_str} - {error_msg}")
                if is_timeout:
                    logger.info(f"  ⏱️  タイムアウトのため5分待機後、次の日付へ")
                    time.sleep(300)  # 5分待機してスキップ
                else:
                    logger.info(f"  ⏭️  30秒待機後、次の日付へスキップ")
                    time.sleep(30)  # 30秒待機してスキップ
                return None
    
    return None

def main():
    """メイン実行"""
    if len(sys.argv) != 2:
        print("使用方法:")
        print("  python simple_multi_day_collector.py <target_dates_json_file>")
        print("\n例:")
        print("  python simple_multi_day_collector.py data_files/target_dates_latest.json")
        return 1
    
    json_file_path = sys.argv[1]
    global logger
    logger = setup_logging()
    
    logger.info("🎯 シンプル複数日データ収集（JSDAサーバー保護版）")
    logger.info("=" * 60)
    logger.info("📊 J列: interest_payment_date (MM/DD文字列形式)")
    logger.info("🔄 処理: 1日ごとに保存")
    logger.info("⏱️  日付間隔: 30秒待機")
    logger.info("⏱️  タイムアウト時: 5分待機→リトライ→5分待機→スキップ")
    logger.info("⏱️  通常エラー時: 3分待機→リトライ→30秒待機→スキップ")
    logger.info("-" * 60)
    
    # 対象日付読み込み
    target_dates = load_target_dates(json_file_path)
    if not target_dates:
        logger.error("❌ 対象日付が見つかりません")
        return 1
    
    # データベース管理とプロセッサ初期化
    db_manager = DatabaseManager()
    processor = BondDataProcessor()
    
    # 既存データ確認
    logger.info("📊 既存データ確認中...")
    existing_dates = db_manager.get_all_existing_dates()
    logger.info(f"📊 DB既存データ: {len(existing_dates)}日分")
    
    # 未収集日付の特定
    new_dates = [date for date in target_dates if date not in existing_dates]
    logger.info(f"🎯 新規収集対象: {len(new_dates)}日分")
    
    if not new_dates:
        logger.info("✅ 全ての対象日付は既に収集済みです")
        return 0
    
    # データ収集開始
    logger.info("🚀 データ収集開始...")
    logger.info("-" * 60)
    
    successful_count = 0
    failed_count = 0
    skipped_count = 0
    
    for i, target_date in enumerate(new_dates, 1):
        logger.info(f"[{i:3d}/{len(new_dates)}] 処理中: {target_date}")
        
        # 1日分のデータ収集
        result = collect_single_day_data(processor, db_manager, target_date)
        
        if result is not None:
            if result > 0:
                successful_count += 1
                logger.info(f"  📈 累計成功: {successful_count}日")
            else:
                skipped_count += 1
                logger.info(f"  📊 データなし (累計: {skipped_count}日)")
        else:
            failed_count += 1
            logger.info(f"  📉 累計失敗: {failed_count}日")
        
        # 次の日付への間隔（JSDAサーバー保護のため30秒）
        if i < len(new_dates):
            logger.info(f"  ⏱️  30秒待機してから次の日付へ...")
            time.sleep(30)  # 30秒間隔で次へ
    
    # 結果サマリー
    logger.info("=" * 60)
    logger.info("🎉 データ収集完了")
    logger.info(f"✅ 成功: {successful_count}日")
    logger.info(f"⚠️  データなし: {skipped_count}日")
    logger.info(f"❌ 失敗: {failed_count}日")
    logger.info(f"📊 処理対象: {len(new_dates)}日")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())