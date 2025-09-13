#!/usr/bin/env python3
"""
2019年以前の月別ディレクトリ構造対応のテストスクリプト
"""

import sys
import os
import json
from datetime import datetime

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.utils.jsda_parser import JSDAParser

def test_monthly_structure():
    """月別ディレクトリ構造のテスト"""
    
    print("🧪 月別ディレクトリ構造テスト開始")
    print("=" * 50)
    
    parser = JSDAParser()
    
    # 2018年をテスト（月別構造）
    test_year = 2018
    
    print(f"📅 {test_year}年のデータ取得テスト...")
    
    try:
        available_dates = parser._parse_archive_page(test_year)
        
        if available_dates:
            dates_list = sorted(list(available_dates))
            
            print(f"✅ {test_year}年: {len(dates_list)}日分取得成功")
            print(f"📊 期間: {dates_list[0]} ～ {dates_list[-1]}")
            
            # 月別集計
            monthly_counts = {}
            for date_obj in dates_list:
                month_key = f"{date_obj.year}-{date_obj.month:02d}"
                monthly_counts[month_key] = monthly_counts.get(month_key, 0) + 1
            
            print(f"📈 月別集計:")
            for month, count in sorted(monthly_counts.items()):
                print(f"  {month}: {count}日")
            
            # サンプル保存
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            test_result_file = f"data_files/test_monthly_{test_year}_{timestamp}.json"
            
            result_data = {
                "test_timestamp": timestamp,
                "year_tested": test_year,
                "dates_found": len(dates_list),
                "date_range": {
                    "start": str(dates_list[0]) if dates_list else None,
                    "end": str(dates_list[-1]) if dates_list else None
                },
                "monthly_counts": monthly_counts,
                "sample_dates": [str(d) for d in dates_list[:10]]  # 最初の10日
            }
            
            with open(test_result_file, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=2)
            
            print(f"💾 テスト結果保存: {test_result_file}")
            
            return True, len(dates_list)
        else:
            print(f"❌ {test_year}年: データ取得失敗")
            return False, 0
            
    except Exception as e:
        print(f"💥 テストエラー: {e}")
        return False, 0

def test_single_month():
    """単一月のテスト（デバッグ用）"""
    print("\n🔍 単一月テスト (2018年12月)")
    print("-" * 30)
    
    parser = JSDAParser()
    
    try:
        # 2018年12月のみテスト
        month_dates = parser._scan_monthly_directory(2018, 12)
        
        if month_dates:
            dates_list = sorted(list(month_dates))
            print(f"✅ 2018年12月: {len(dates_list)}日分")
            print(f"📋 日付サンプル: {dates_list[:5]}")
        else:
            print("❌ 2018年12月: データなし")
            
    except Exception as e:
        print(f"💥 単一月テストエラー: {e}")

def main():
    """メイン実行"""
    print("🚨 JSDA保護ルール:")
    print("  - 1秒間隔でファイル存在チェック")
    print("  - 5秒間隔で月切り替え")
    print("  - HEADリクエストで軽量チェック")
    print()
    
    try:
        # 単一月テスト（軽量）
        test_single_month()
        
        # フル年テスト
        success, count = test_monthly_structure()
        
        if success:
            print(f"\n🎉 月別構造テスト成功: {count}日分")
            print("✅ 次のステップ: 他の年も同様にテスト可能")
        else:
            print(f"\n❌ 月別構造テスト失敗")
            print("🔧 ディレクトリ構造やURL形式を再確認してください")
            
    except KeyboardInterrupt:
        print("\n\n⚠️ ユーザーによって中断されました")
    except Exception as e:
        print(f"\n💥 予期しないエラー: {e}")

if __name__ == "__main__":
    main()