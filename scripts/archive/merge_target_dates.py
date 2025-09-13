#!/usr/bin/env python3
"""
ターゲット日付ファイル安全マージスクリプト
既存データを保護しつつ新規データを統合
"""

import json
import sys
from datetime import datetime

def safe_merge_target_dates():
    """安全にターゲット日付をマージ"""
    
    # ファイルパス
    existing_file = "data_files/target_dates_latest.json"
    new_file = "data_files/target_dates_20250913_011222.json"
    backup_file = f"data_files/target_dates_latest_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    try:
        print("📊 ターゲット日付ファイル安全マージ")
        print("=" * 50)
        
        # 1. 既存ファイル読み込み
        print("📂 既存ファイル読み込み...")
        with open(existing_file, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
        existing_dates = set(existing_data.get('target_dates', []))
        print(f"   既存データ: {len(existing_dates)}日分")
        
        # 2. 新規ファイル読み込み
        print("📂 新規ファイル読み込み...")
        with open(new_file, 'r', encoding='utf-8') as f:
            new_data = json.load(f)
        new_dates = set(new_data.get('target_dates', []))
        print(f"   新規データ: {len(new_dates)}日分")
        
        # 3. データ分析
        print("\n📈 データ分析:")
        common_dates = existing_dates & new_dates
        only_existing = existing_dates - new_dates
        only_new = new_dates - existing_dates
        
        print(f"   共通日付: {len(common_dates)}日分")
        print(f"   既存のみ: {len(only_existing)}日分") 
        print(f"   新規のみ: {len(only_new)}日分")
        
        if only_existing:
            print(f"   ⚠️  既存専用日付サンプル: {sorted(list(only_existing))[:5]}")
        if only_new:
            print(f"   ✅ 新規追加日付サンプル: {sorted(list(only_new), reverse=True)[:5]}")
        
        # 4. バックアップ作成
        print(f"\n💾 既存ファイルバックアップ: {backup_file}")
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)
        
        # 5. マージ実行
        merged_dates = sorted(list(existing_dates | new_dates), reverse=True)
        
        # 新規データ構造をベースに作成（より新しいtimestamp等）
        merged_data = {
            "collection_timestamp": new_data.get('collection_timestamp'),
            "total_target_dates": len(merged_dates),
            "estimated_time_hours": round(len(merged_dates) * 20 / 3600, 1),
            "merge_info": {
                "existing_dates_count": len(existing_dates),
                "new_dates_count": len(new_dates),
                "merged_dates_count": len(merged_dates),
                "dates_preserved": len(only_existing),
                "dates_added": len(only_new)
            },
            "target_dates": merged_dates
        }
        
        # 6. 結果保存
        print(f"\n💾 マージ結果保存: {existing_file}")
        with open(existing_file, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=2)
        
        print("\n🎉 マージ完了:")
        print(f"   最終データ: {len(merged_dates)}日分")
        print(f"   データ保護: {len(only_existing)}日分保持")
        print(f"   新規追加: {len(only_new)}日分")
        print(f"   推定時間: {merged_data['estimated_time_hours']}時間")
        
        return True
        
    except Exception as e:
        print(f"❌ マージエラー: {e}")
        return False

if __name__ == "__main__":
    success = safe_merge_target_dates()
    sys.exit(0 if success else 1)