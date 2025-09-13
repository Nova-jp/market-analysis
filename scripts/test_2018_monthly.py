#!/usr/bin/env python3
"""
2018年1月のみのテストスクリプト（高速テスト用）
"""

import sys
import os
import json
from datetime import datetime

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.utils.jsda_parser import JSDAParser

def test_2018_january():
    """2018年1月のみテスト"""
    
    print("🧪 2018年1月 月別ディレクトリテスト")
    print("=" * 40)
    
    parser = JSDAParser()
    
    try:
        # 2018年1月のみテスト
        january_dates = parser._scan_monthly_directory(2018, 1)
        
        if january_dates:
            dates_list = sorted(list(january_dates))
            
            print(f"✅ 2018年1月: {len(dates_list)}日分取得成功")
            print(f"📋 日付リスト: {[str(d) for d in dates_list]}")
            
            # 結果保存
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            test_result_file = f"data_files/test_2018_jan_{timestamp}.json"
            
            result_data = {
                "test_timestamp": timestamp,
                "year_month": "2018-01",
                "dates_found": len(dates_list),
                "dates": [str(d) for d in dates_list]
            }
            
            with open(test_result_file, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=2)
            
            print(f"💾 テスト結果保存: {test_result_file}")
            
            if len(dates_list) > 0:
                print(f"\n🎉 月別構造テスト成功！")
                print(f"💡 確認されたURL形式:")
                print(f"   https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/files/2018/01/S{dates_list[0].strftime('%y%m%d')}.csv")
                return True
            else:
                return False
        else:
            print(f"❌ 2018年1月: データなし")
            return False
            
    except Exception as e:
        print(f"💥 テストエラー: {e}")
        return False

def main():
    """メイン実行"""
    print("🚨 JSDA保護設定:")
    print("  - 2秒間隔でファイル存在チェック")
    print("  - 営業日のみチェック（土日スキップ）")
    print("  - HEADリクエストで軽量チェック")
    print()
    
    try:
        success = test_2018_january()
        
        if success:
            print(f"\n✅ 月別ディレクトリ構造が確認されました")
            print(f"🚀 次のステップ: 他の年月でも同様の構造でデータ収集可能")
        else:
            print(f"\n❌ 月別ディレクトリ構造テスト失敗")
            print(f"🔧 URL形式やディレクトリ構造を再確認してください")
            
    except KeyboardInterrupt:
        print("\n\n⚠️ ユーザーによって中断されました")
    except Exception as e:
        print(f"\n💥 予期しないエラー: {e}")

if __name__ == "__main__":
    main()