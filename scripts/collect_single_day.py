#!/usr/bin/env python3
"""
1日分データ収集スクリプト
指定した日付の債券データをJSDAから取得してデータベースに保存
"""

import sys
import os
from datetime import date

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.processors.bond_data_processor import BondDataProcessor
from data.utils.database_manager import DatabaseManager

def collect_single_day_data(target_date_str):
    """
    指定日付のデータを収集してデータベースに保存
    
    Args:
        target_date_str (str): 対象日付 (YYYY-MM-DD形式)
        
    Returns:
        bool: 成功時True、失敗時False
    """
    try:
        target_date = date.fromisoformat(target_date_str)
    except ValueError:
        print(f"❌ 無効な日付形式: {target_date_str}")
        print("   正しい形式: YYYY-MM-DD (例: 2025-04-21)")
        return False
    
    print(f"🎯 1日分データ収集")
    print("=" * 40)
    print(f"📅 収集対象日付: {target_date}")
    print(f"🔗 データソース: JSDA")
    print("-" * 40)
    
    # 初期化
    processor = BondDataProcessor()
    db_manager = DatabaseManager()
    
    try:
        # 1. CSVデータ取得
        print("📡 JSDAからCSVデータを取得中...")
        url, filename = processor.build_csv_url(target_date)
        print(f"   URL: {url}")
        
        raw_df = processor.download_csv_data(url)
        
        if raw_df is None:
            print("❌ CSVデータの取得に失敗しました")
            return False
        
        if raw_df.empty:
            print("📭 データが空でした（休日の可能性があります）")
            return True
        
        print(f"✅ CSV取得成功: {len(raw_df):,}行")
        
        # 2. データ処理
        print("🔄 データ処理中...")
        processed_df = processor.process_raw_data(raw_df)
        
        if processed_df.empty:
            print("📭 処理後のデータが空でした")
            return True
        
        print(f"✅ データ処理成功: {len(processed_df):,}行")
        
        # 3. trade_date追加
        processed_df['trade_date'] = target_date.strftime('%Y-%m-%d')
        
        # 4. データベース保存
        print("💾 データベースに保存中...")
        batch_data = processed_df.to_dict('records')
        saved_count = db_manager.batch_insert_data(batch_data)
        
        if saved_count > 0:
            print(f"🎉 保存成功: {saved_count:,}件")
            
            # 保存結果の詳細表示
            print("\n📊 保存データ詳細:")
            # 銘柄種別の内訳
            issue_type_1 = sum(1 for record in batch_data if record.get('issue_type') == 1)
            issue_type_2 = sum(1 for record in batch_data if record.get('issue_type') == 2)
            print(f"   国庫短期証券 (issue_type=1): {issue_type_1:,}件")
            print(f"   中期国債 (issue_type=2): {issue_type_2:,}件")
            
            return True
        else:
            print("❌ データベース保存に失敗しました")
            return False
            
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        return False

def main():
    """メイン実行"""
    if len(sys.argv) != 2:
        print("使用方法:")
        print("  python collect_single_day.py YYYY-MM-DD")
        print()
        print("例:")
        print("  python collect_single_day.py 2025-04-21")
        return 1
    
    target_date_str = sys.argv[1]
    
    success = collect_single_day_data(target_date_str)
    
    if success:
        print("\n✅ 処理完了")
        return 0
    else:
        print("\n❌ 処理失敗")
        return 1

if __name__ == "__main__":
    sys.exit(main())