#!/usr/bin/env python3
"""
JSDA利用可能日付収集スクリプト
HTMLアーカイブページから利用可能な全日付を取得し保存
"""

import sys
import os
import json
import time
from datetime import date, datetime
import logging

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.utils.jsda_parser import JSDAParser
from data.utils.database_manager import DatabaseManager

class AvailableDateCollector:
    """利用可能日付収集クラス"""
    
    def __init__(self):
        self.jsda_parser = JSDAParser()
        self.db_manager = DatabaseManager()
        logging.basicConfig(level=logging.INFO, format='%(message)s')
        self.logger = logging.getLogger(__name__)
    
    def collect_and_save_dates(self):
        """
        JSDAから利用可能日付を取得し、ファイルに保存
        """
        print("📅 JSDA利用可能日付収集開始")
        print("=" * 50)
        
        # 1. 既存データベースの日付を取得
        print("📊 既存データベース日付確認中...")
        existing_dates = set(self.db_manager.get_all_existing_dates())
        print(f"✅ DB既存日数: {len(existing_dates)}日分")
        
        # 2. JSDAサイトから利用可能日付を取得
        print("\n🌐 JSDAサイトから利用可能日付取得中...")
        print("⏱️ HTMLアーカイブページ解析（30秒間隔）")
        
        available_dates = self.jsda_parser.get_available_dates_from_html()
        
        if not available_dates:
            print("❌ 利用可能日付の取得に失敗しました")
            return False
        
        print(f"✅ JSDA取得成功: {len(available_dates)}日分")
        print(f"📅 日付範囲: {available_dates[-1]} ～ {available_dates[0]}")
        
        # 3. 新規収集対象日付を特定
        available_dates_str = [d.isoformat() for d in available_dates]
        new_dates = [d for d in available_dates_str if d not in existing_dates]
        new_dates.sort(reverse=True)  # 今日から昔へ
        
        print(f"\n📋 収集対象分析:")
        print(f"  - JSDA利用可能: {len(available_dates_str)}日分")
        print(f"  - DB既存データ: {len(existing_dates)}日分")
        print(f"  - 新規収集対象: {len(new_dates)}日分")
        
        # 4. 結果をファイルに保存
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 全利用可能日付を保存
        available_file = f"data_files/available_dates_{timestamp}.json"
        with open(available_file, 'w', encoding='utf-8') as f:
            json.dump({
                "collection_timestamp": timestamp,
                "total_available_dates": len(available_dates_str),
                "date_range": {
                    "earliest": available_dates_str[-1] if available_dates_str else None,
                    "latest": available_dates_str[0] if available_dates_str else None
                },
                "available_dates": available_dates_str
            }, f, ensure_ascii=False, indent=2)
        
        # 新規収集対象日付を保存
        target_file = f"data_files/target_dates_{timestamp}.json"
        with open(target_file, 'w', encoding='utf-8') as f:
            json.dump({
                "collection_timestamp": timestamp,
                "total_target_dates": len(new_dates),
                "estimated_time_hours": round(len(new_dates) * 20 / 3600, 1),
                "target_dates": new_dates
            }, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 ファイル保存完了:")
        print(f"  - 全利用可能日付: {available_file}")
        print(f"  - 新規収集対象: {target_file}")
        print(f"\n⏱️ 推定データ収集時間: {round(len(new_dates) * 20 / 3600, 1)}時間")
        
        return True

def main():
    """メイン実行"""
    try:
        collector = AvailableDateCollector()
        success = collector.collect_and_save_dates()
        
        if success:
            print("\n🎉 利用可能日付収集完了")
            print("次のステップ: データ収集スクリプトを実行してください")
            return 0
        else:
            print("\n❌ 利用可能日付収集失敗")
            return 1
        
    except KeyboardInterrupt:
        print("\n\n⚠️ ユーザーによって中断されました")
        return 1
    except Exception as e:
        print(f"\n💥 予期しないエラー: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())