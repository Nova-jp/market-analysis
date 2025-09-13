#!/usr/bin/env python3
"""
JSDAの2008-2019年データ再取得スクリプト
30秒間隔でリトライ実行
"""

import sys
import os
import json
import time
import requests
from datetime import datetime
from bs4 import BeautifulSoup

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def retry_missing_years():
    """欠損年データの再取得"""
    base_url = "https://market.jsda.or.jp"
    missing_years = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]
    
    print("🔄 JSDA欠損年データ再取得（30秒間隔）")
    print("=" * 50)
    
    recovered_dates = {}
    
    for year in missing_years:
        print(f"\n📅 {year}年データ取得開始...")
        archive_url = f"{base_url}/shijyo/saiken/baibai/baisanchi/archive{year}.html"
        
        try:
            print(f"  🌐 URL: {archive_url}")
            print("  ⏱️  45秒タイムアウト設定...")
            
            # 45秒タイムアウトで取得
            response = requests.get(archive_url, timeout=45)
            response.raise_for_status()
            
            if response.encoding is None or response.encoding == 'ISO-8859-1':
                response.encoding = 'utf-8'
            
            # HTMLパース
            soup = BeautifulSoup(response.text, 'html.parser')
            year_dates = []
            
            # CSVリンクを検索
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href')
                if href and ('S' in href and '.csv' in href):
                    import re
                    match = re.search(r'S(\d{6})\.csv', href)
                    if match:
                        date_str = match.group(1)
                        try:
                            if len(date_str) == 6:
                                parsed_year = int('20' + date_str[:2])
                                month = int(date_str[2:4])
                                day = int(date_str[4:6])
                                
                                # 指定年と一致する場合のみ追加
                                if parsed_year == year:
                                    date_obj = f"{parsed_year}-{month:02d}-{day:02d}"
                                    year_dates.append(date_obj)
                        except ValueError:
                            continue
            
            if year_dates:
                recovered_dates[year] = sorted(year_dates)
                print(f"  ✅ {year}年: {len(year_dates)}日分取得成功")
                print(f"     サンプル: {year_dates[:3]}...")
            else:
                print(f"  ❌ {year}年: データなし")
                
        except requests.exceptions.Timeout:
            print(f"  ⚠️  {year}年: タイムアウト（45秒経過）")
        except requests.exceptions.ConnectionError:
            print(f"  ⚠️  {year}年: 接続エラー")
        except Exception as e:
            print(f"  ❌ {year}年: 予期しないエラー - {e}")
        
        # 30秒待機（最後の年以外）
        if year < missing_years[-1]:
            print("  ⏳ 30秒待機中...")
            time.sleep(30)
    
    # 結果保存
    if recovered_dates:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_file = f"data_files/recovered_dates_{timestamp}.json"
        
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump({
                "recovery_timestamp": timestamp,
                "total_recovered_years": len(recovered_dates),
                "recovered_data": recovered_dates,
                "summary": {
                    year: len(dates) for year, dates in recovered_dates.items()
                }
            }, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 回復データ保存: {result_file}")
        print(f"🎉 回復完了: {len(recovered_dates)}年分")
        
        # 統計表示
        total_dates = sum(len(dates) for dates in recovered_dates.values())
        print(f"📊 総データ数: {total_dates}日分")
        
        return result_file, recovered_dates
    else:
        print("\n❌ 回復データなし")
        return None, {}

def main():
    """メイン実行"""
    print("🚨 JSDA保護ルール:")
    print("  - 30秒間隔でアクセス")
    print("  - 45秒タイムアウト")
    print("  - フォアグラウンド実行")
    print()
    
    try:
        result_file, recovered_data = retry_missing_years()
        
        if recovered_data:
            print(f"\n✅ 次のステップ:")
            print(f"  1. {result_file} を確認")
            print(f"  2. target_dates_latest.json に統合")
            return 0
        else:
            print(f"\n❌ データ回復に失敗")
            return 1
            
    except KeyboardInterrupt:
        print("\n\n⚠️ ユーザーによって中断されました")
        return 1
    except Exception as e:
        print(f"\n💥 予期しないエラー: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())