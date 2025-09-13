#!/usr/bin/env python3
"""
2019年データをtarget_dates_latest.jsonに統合するスクリプト
"""

import json
from datetime import datetime
from typing import Set, List

def merge_2019_data():
    """2019年データを既存のtarget_datesに安全に統合"""
    
    # ファイル読み込み
    print("📖 データファイル読み込み中...")
    
    # 既存のtarget_dates_latest.json
    with open("data_files/target_dates_latest.json", 'r', encoding='utf-8') as f:
        existing_data = json.load(f)
    
    # 2019年データ
    with open("data_files/recovered_2019_20250913_040248.json", 'r', encoding='utf-8') as f:
        data_2019 = json.load(f)
    
    print(f"✅ 既存データ: {existing_data['total_target_dates']}日分")
    print(f"✅ 2019年データ: {data_2019['dates_count']}日分")
    
    # 既存日付をセットに変換
    existing_dates: Set[str] = set(existing_data['target_dates'])
    print(f"📊 既存日付数: {len(existing_dates)}")
    
    # 2019年データから日付を抽出
    dates_2019: Set[str] = set(data_2019['dates'])
    print(f"📊 2019年日付数: {len(dates_2019)}")
    
    # 重複チェック
    duplicates = existing_dates & dates_2019
    print(f"🔄 重複日付数: {len(duplicates)}")
    
    # 新規追加分
    new_dates = dates_2019 - existing_dates
    print(f"🆕 新規追加分: {len(new_dates)}")
    
    if len(new_dates) == 0:
        print("ℹ️  追加する新しい日付がありません")
        return
    
    # サンプル表示
    if len(new_dates) > 0:
        sorted_new = sorted(list(new_dates))
        print(f"📅 新規日付サンプル: {sorted_new[:5]}...{sorted_new[-5:]}")
    
    # 統合
    print("🔄 データ統合中...")
    all_dates = existing_dates | dates_2019
    final_dates = sorted(list(all_dates), reverse=True)  # 降順ソート
    
    # 統合データ作成
    merged_data = {
        "collection_timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "total_target_dates": len(final_dates),
        "estimated_time_hours": round(len(final_dates) * 5.5 / 3600, 1),
        "merge_info": {
            "existing_dates_count": len(existing_dates),
            "dates_2019_count": len(dates_2019),
            "new_dates_count": len(new_dates),
            "duplicates_count": len(duplicates),
            "merged_dates_count": len(final_dates),
            "dates_preserved": len(existing_dates),
            "dates_added": len(new_dates)
        },
        "recovery_info": {
            "previous_recoveries": existing_data.get("recovery_info", {}),
            "latest_recovery": {
                "source_file": "recovered_2019_20250913_040248.json",
                "year": 2019,
                "dates_added": len(new_dates),
                "merge_timestamp": datetime.now().strftime("%Y%m%d_%H%M%S")
            }
        },
        "target_dates": final_dates
    }
    
    # バックアップ作成
    backup_filename = f"data_files/target_dates_backup_before_2019_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(backup_filename, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=2)
    print(f"💾 既存データバックアップ: {backup_filename}")
    
    # 統合データ保存
    with open("data_files/target_dates_latest.json", 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 2019年データ統合完了!")
    print(f"📊 最終統計:")
    print(f"   - 元データ: {len(existing_dates)}日")
    print(f"   - 2019年データ: {len(dates_2019)}日")
    print(f"   - 重複除外後: {len(final_dates)}日")
    print(f"   - 新規追加: {len(new_dates)}日")
    print(f"   - 最古日付: {final_dates[-1]}")
    print(f"   - 最新日付: {final_dates[0]}")
    
    return merged_data

if __name__ == "__main__":
    merge_2019_data()