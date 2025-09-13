#!/usr/bin/env python3
"""
2019年のみの個別リトライスクリプト
より長いタイムアウト（60秒）と複数回リトライで対応
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

def retry_2019_only():
    """2019年データの専用取得"""
    base_url = "https://market.jsda.or.jp"
    year = 2019
    
    print("🔄 2019年専用データ取得（60秒タイムアウト、5回リトライ）")
    print("=" * 60)
    
    archive_url = f"{base_url}/shijyo/saiken/baibai/baisanchi/archive{year}.html"
    max_retries = 5
    timeout_seconds = 60  # 60秒に延長
    
    for attempt in range(max_retries):
        try:
            print(f"\n📅 2019年データ取得 - 試行 {attempt + 1}/{max_retries}")
            print(f"  🌐 URL: {archive_url}")
            print(f"  ⏱️  {timeout_seconds}秒タイムアウト設定...")
            
            # 60秒タイムアウトで取得
            response = requests.get(archive_url, timeout=timeout_seconds)
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
                                
                                # 2019年と一致する場合のみ追加
                                if parsed_year == year:
                                    date_obj = f"{parsed_year}-{month:02d}-{day:02d}"
                                    year_dates.append(date_obj)
                        except ValueError:
                            continue
            
            if year_dates:
                sorted_dates = sorted(year_dates)
                print(f"  ✅ 2019年: {len(year_dates)}日分取得成功")
                print(f"     サンプル: {sorted_dates[:3]}...{sorted_dates[-3:]}")
                
                # 結果保存
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                result_file = f"data_files/recovered_2019_{timestamp}.json"
                
                result_data = {
                    "recovery_timestamp": timestamp,
                    "year": 2019,
                    "attempt_number": attempt + 1,
                    "timeout_used": timeout_seconds,
                    "dates_count": len(year_dates),
                    "dates": sorted_dates
                }
                
                with open(result_file, 'w', encoding='utf-8') as f:
                    json.dump(result_data, f, ensure_ascii=False, indent=2)
                
                print(f"\n💾 2019年データ保存: {result_file}")
                print(f"🎉 取得成功: {len(year_dates)}日分")
                print(f"📅 期間: {sorted_dates[0]} ～ {sorted_dates[-1]}")
                
                return result_file, result_data
            else:
                print(f"  ❌ 2019年: データなし（試行 {attempt + 1}）")
                
        except requests.exceptions.Timeout:
            print(f"  ⚠️  2019年: タイムアウト（{timeout_seconds}秒経過、試行 {attempt + 1}）")
        except requests.exceptions.ConnectionError:
            print(f"  ⚠️  2019年: 接続エラー（試行 {attempt + 1}）")
        except Exception as e:
            print(f"  ❌ 2019年: 予期しないエラー（試行 {attempt + 1}） - {e}")
        
        # 次の試行前に待機（最後の試行以外）
        if attempt < max_retries - 1:
            wait_time = 45  # 45秒待機
            print(f"  ⏳ {wait_time}秒待機してリトライ...")
            time.sleep(wait_time)
    
    print(f"\n❌ 2019年: 最大リトライ数（{max_retries}回）到達")
    return None, {}

def main():
    """メイン実行"""
    print("🚨 JSDA保護ルール:")
    print("  - 45秒間隔でリトライ")
    print("  - 60秒タイムアウト")
    print("  - 最大5回リトライ")
    print()
    
    try:
        result_file, result_data = retry_2019_only()
        
        if result_data:
            print(f"\n✅ 2019年データ取得完了:")
            print(f"  - ファイル: {result_file}")
            print(f"  - 日数: {result_data['dates_count']}日")
            print(f"  - 次のステップ: target_dates_latest.jsonに統合")
            return 0
        else:
            print(f"\n❌ 2019年データ取得に失敗")
            print("  - JSDAサーバーの負荷が高い可能性")
            print("  - 後日再試行を推奨")
            return 1
            
    except KeyboardInterrupt:
        print("\n\n⚠️ ユーザーによって中断されました")
        return 1
    except Exception as e:
        print(f"\n💥 予期しないエラー: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())