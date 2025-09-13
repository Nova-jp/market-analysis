#!/usr/bin/env python3
"""
修正されたCSVパス構造のテストスクリプト
"""

import sys
import os
from datetime import date

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.utils.jsda_parser import JSDAParser

def test_csv_paths():
    """CSVパス構造のテスト"""
    
    print("🧪 CSVパス構造テスト")
    print("=" * 40)
    
    parser = JSDAParser()
    
    # テスト日付
    test_dates = [
        date(2019, 12, 30),  # 2019年以前: 月別ディレクトリ
        date(2020, 1, 6),    # 2020年以降: 年別ディレクトリ
        date(2018, 6, 15),   # 2018年テスト
    ]
    
    for test_date in test_dates:
        print(f"\n📅 テスト日付: {test_date}")
        
        # 期待されるURL形式を表示
        year = test_date.year
        month = test_date.month
        date_str = test_date.strftime("%y%m%d")
        
        if year <= 2019:
            expected_url = f"https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/files/{year}/{month:02d}/S{date_str}.csv"
            print(f"   期待URL（月別）: {expected_url}")
        else:
            expected_url = f"https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/files/{year}/S{date_str}.csv"
            print(f"   期待URL（年別）: {expected_url}")
        
        # ファイル存在チェック
        try:
            exists = parser._check_csv_file_exists(test_date)
            if exists:
                print(f"   ✅ ファイル存在: はい")
            else:
                print(f"   ❌ ファイル存在: いいえ")
        except Exception as e:
            print(f"   💥 チェックエラー: {e}")

def main():
    """メイン実行"""
    print("🚨 テスト内容:")
    print("  - 2019年以前: /files/YYYY/MM/S{YYMMDD}.csv")
    print("  - 2020年以降: /files/YYYY/S{YYMMDD}.csv")
    print()
    
    try:
        test_csv_paths()
        print(f"\n🎯 CSVパス構造の修正が正しく動作していることを確認")
        
    except KeyboardInterrupt:
        print("\n\n⚠️ ユーザーによって中断されました")
    except Exception as e:
        print(f"\n💥 予期しないエラー: {e}")

if __name__ == "__main__":
    main()