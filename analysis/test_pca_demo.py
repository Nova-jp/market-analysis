#!/usr/bin/env python3
"""
主成分分析デモ実行スクリプト
テスト用パラメータで自動実行
"""

import sys
import os
from datetime import date

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.principal_component_analysis import YieldCurvePCA

def demo_pca():
    """デモ実行"""
    print("🎯 主成分分析デモ実行")
    print("=" * 60)
    
    try:
        # 分析クラス初期化
        pca_analyzer = YieldCurvePCA()
        
        # まず利用可能な日付を確認
        print("📋 利用可能日付を確認中...")
        available_dates = pca_analyzer.db_manager.get_all_existing_dates()
        if available_dates:
            latest_date_str = max(available_dates)
            latest_date = date.fromisoformat(latest_date_str)
            print(f"✅ 最新日付: {latest_date}")
        else:
            print("❌ 利用可能な日付が見つかりません")
            return 1
        
        # デモパラメータ
        analysis_date = latest_date  # 実際に存在する最新日付を使用
        analysis_days = 60  # 過去60日分に拡大
        n_components_restore = 3  # 第3主成分まで使用して復元
        
        print(f"📅 分析対象日付: {analysis_date}")
        print(f"📊 分析期間: 過去{analysis_days}日分")
        print(f"🔧 復元主成分数: 第{n_components_restore}主成分")
        print()
        
        # データ取得
        df = pca_analyzer.get_historical_data(analysis_date, analysis_days)
        
        # スプライン補間
        interpolated_data = pca_analyzer.interpolate_yield_curves(df)
        
        # 主成分分析実行
        pca_result = pca_analyzer.perform_pca(interpolated_data)
        
        # 主成分ベクトル描画
        pca_analyzer.plot_principal_components(pca_result)
        
        # 指定日のイールドカーブ復元
        reconstruction_result = pca_analyzer.reconstruct_yield_curve(
            pca_result, analysis_date, n_components_restore)
        
        # 復元結果と差分の描画
        pca_analyzer.plot_reconstruction_comparison(
            reconstruction_result, analysis_date, pca_result, n_components_restore)
        
        print("\n🎉 主成分分析デモ完了！")
        
    except Exception as e:
        print(f"\n❌ エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(demo_pca())