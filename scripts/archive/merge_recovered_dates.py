#!/usr/bin/env python3
"""
回復データをtarget_dates_latest.jsonに統合するスクリプト
"""

import json
from datetime import datetime
from typing import Set, List

def merge_recovered_dates():
    """回復したデータを既存のtarget_datesに安全に統合"""
    
    # ファイル読み込み
    print("📖 データファイル読み込み中...")
    
    # 既存のtarget_dates_latest.json
    with open("data_files/target_dates_latest.json", 'r', encoding='utf-8') as f:
        existing_data = json.load(f)
    
    # 回復データ
    with open("data_files/recovered_dates_20250913_035123.json", 'r', encoding='utf-8') as f:
        recovered_data = json.load(f)
    
    print(f"✅ 既存データ: {existing_data['total_target_dates']}日分")
    print(f"✅ 回復データ: {recovered_data['summary']} = {sum(recovered_data['summary'].values())}日分")
    
    # 既存日付をセットに変換
    existing_dates: Set[str] = set(existing_data['target_dates'])
    print(f"📊 既存日付数: {len(existing_dates)}")
    
    # 回復データから日付を抽出
    recovered_dates: Set[str] = set()
    for year_str, dates_list in recovered_data['recovered_data'].items():
        recovered_dates.update(dates_list)
    
    print(f"📊 回復日付数: {len(recovered_dates)}")
    
    # 重複チェック
    duplicates = existing_dates & recovered_dates
    print(f"🔄 重複日付数: {len(duplicates)}")
    
    # 新規追加分
    new_dates = recovered_dates - existing_dates
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
    all_dates = existing_dates | recovered_dates
    final_dates = sorted(list(all_dates), reverse=True)  # 降順ソート
    
    # 統合データ作成
    merged_data = {
        "collection_timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "total_target_dates": len(final_dates),
        "estimated_time_hours": round(len(final_dates) * 5.5 / 3600, 1),
        "merge_info": {
            "existing_dates_count": len(existing_dates),
            "recovered_dates_count": len(recovered_dates),
            "new_dates_count": len(new_dates),
            "duplicates_count": len(duplicates),
            "merged_dates_count": len(final_dates),
            "dates_preserved": len(existing_dates),
            "dates_added": len(new_dates)
        },
        "recovery_info": {
            "source_file": "recovered_dates_20250913_035123.json",
            "recovered_years": list(recovered_data['recovered_data'].keys()),
            "years_recovered": recovered_data['total_recovered_years']
        },
        "target_dates": final_dates
    }
    
    # バックアップ作成
    backup_filename = f"data_files/target_dates_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(backup_filename, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=2)
    print(f"💾 既存データバックアップ: {backup_filename}")
    
    # 統合データ保存
    with open("data_files/target_dates_latest.json", 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 統合完了!")
    print(f"📊 最終統計:")
    print(f"   - 元データ: {len(existing_dates)}日")
    print(f"   - 回復データ: {len(recovered_dates)}日")
    print(f"   - 重複除外後: {len(final_dates)}日")
    print(f"   - 新規追加: {len(new_dates)}日")
    print(f"   - 最古日付: {final_dates[-1]}")
    print(f"   - 最新日付: {final_dates[0]}")
    
    return merged_data

if __name__ == "__main__":
    merge_recovered_dates()