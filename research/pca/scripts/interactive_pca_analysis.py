#!/usr/bin/env python3
"""
対話式主成分分析システム
Webアプリ化を見据えたCLI入力対応版
"""

import sys
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# 英語フォント設定（文字化け対策）
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['font.size'] = 10
from sklearn.decomposition import PCA
from scipy.interpolate import CubicSpline
from datetime import datetime, date, timedelta
import warnings
warnings.filterwarnings('ignore')

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.db.sync_client import DatabaseManager

class InteractivePCA:
    """対話式主成分分析クラス"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.maturity_range = np.linspace(0.5, 40, 100)  # 0.5年から40年、100点
        self.n_components = 5  # 第5主成分まで計算
        
        # Webアプリ化用のパラメータ
        self.analysis_params = {}
        
    def get_user_inputs(self):
        """
        ユーザー入力を取得（Webアプリ用パラメータも含む）
        """
        print("🔍 主成分分析パラメータ設定")
        print("=" * 50)
        
        # 利用可能日付の確認
        print("📋 利用可能日付を確認中...")
        available_dates = self.db_manager.get_all_existing_dates()
        if available_dates:
            latest_date_str = max(available_dates)
            earliest_date_str = min(available_dates)
            print(f"✅ 日付範囲: {earliest_date_str} ～ {latest_date_str}")
            print(f"📊 利用可能日数: {len(available_dates)}日分")
        else:
            print("❌ 利用可能な日付が見つかりません")
            return None
        
        print()
        
        # 1. 分析対象日付の入力
        while True:
            try:
                print("📅 分析対象日付を入力してください")
                print(f"   利用可能範囲: {earliest_date_str} ～ {latest_date_str}")
                analysis_date_str = input("   日付 (YYYY-MM-DD) または 'latest' for 最新日: ").strip()
                
                if analysis_date_str.lower() == 'latest':
                    analysis_date = date.fromisoformat(latest_date_str)
                    print(f"   → 最新日 {analysis_date} を使用")
                    break
                else:
                    analysis_date = datetime.strptime(analysis_date_str, "%Y-%m-%d").date()
                    if analysis_date_str in available_dates:
                        break
                    else:
                        print(f"   ❌ {analysis_date} のデータが存在しません")
            except ValueError:
                print("   ❌ 正しい日付形式で入力してください (例: 2024-12-31)")
        
        # 2. 分析期間（過去何日分）の入力
        while True:
            try:
                print("\n📊 分析期間を入力してください")
                print("   推奨: 30-90日 (短期変動分析), 180-365日 (長期トレンド分析)")
                analysis_days = int(input("   過去何日分のデータを使用しますか: "))
                if 10 <= analysis_days <= 1000:
                    break
                else:
                    print("   ❌ 10～1000日の範囲で入力してください")
            except ValueError:
                print("   ❌ 正の整数を入力してください")
        
        # 3. 復元に使用する主成分数の入力
        while True:
            try:
                print("\n🔧 復元設定を入力してください")
                print("   第1主成分: 水準シフト (通常80-95%の寄与率)")
                print("   第2主成分: 傾きの変化 (通常5-15%の寄与率)")
                print("   第3主成分: 曲率の変化 (通常1-5%の寄与率)")
                n_components_restore = int(input("   復元に使用する主成分数 (1-5): "))
                if 1 <= n_components_restore <= 5:
                    break
                else:
                    print("   ❌ 1から5の間で入力してください")
            except ValueError:
                print("   ❌ 正の整数を入力してください")
        
        # 4. 残存年数範囲の設定
        while True:
            try:
                print("\n📈 分析対象残存年数範囲を設定してください")
                print("   デフォルト: 0.5年～40年")
                use_default = input("   デフォルト設定を使用しますか？ (y/n): ").strip().lower()
                
                if use_default in ['y', 'yes', '']:
                    min_maturity = 0.5
                    max_maturity = 40.0
                    break
                else:
                    min_maturity = float(input("   最小残存年数: "))
                    max_maturity = float(input("   最大残存年数: "))
                    if 0.1 <= min_maturity < max_maturity <= 50:
                        break
                    else:
                        print("   ❌ 0.1 ≤ 最小年数 < 最大年数 ≤ 50 で入力してください")
            except ValueError:
                print("   ❌ 正の数値を入力してください")
        
        # 5. 出力設定
        while True:
            print("\n📊 出力設定を選択してください")
            print("   1: グラフファイル保存のみ")
            print("   2: 統計情報表示 + グラフファイル保存")
            print("   3: 詳細分析 + グラフファイル保存 + CSVエクスポート")
            try:
                output_level = int(input("   出力レベル (1-3): "))
                if 1 <= output_level <= 3:
                    break
                else:
                    print("   ❌ 1から3の間で入力してください")
            except ValueError:
                print("   ❌ 正の整数を入力してください")
        
        # パラメータをまとめて保存（Webアプリ用）
        self.analysis_params = {
            'analysis_date': analysis_date,
            'analysis_days': analysis_days,
            'n_components_restore': n_components_restore,
            'min_maturity': min_maturity,
            'max_maturity': max_maturity,
            'output_level': output_level,
            'timestamp': datetime.now().strftime("%Y%m%d_%H%M%S")
        }
        
        # 設定確認
        print("\n✅ 設定内容確認:")
        print(f"   📅 分析対象日: {analysis_date}")
        print(f"   📊 分析期間: 過去{analysis_days}日分")
        print(f"   🔧 復元主成分数: 第{n_components_restore}主成分")
        print(f"   📈 残存年数範囲: {min_maturity}年～{max_maturity}年")
        print(f"   📋 出力レベル: {output_level}")
        
        confirm = input("\n実行しますか？ (y/n): ").strip().lower()
        if confirm not in ['y', 'yes', '']:
            print("❌ 実行を中止しました")
            return None
        
        return self.analysis_params
    
    def get_historical_data(self, params):
        """指定期間のヒストリカルデータを取得"""
        analysis_date = params['analysis_date']
        analysis_days = params['analysis_days']
        min_maturity = params['min_maturity']
        max_maturity = params['max_maturity']
        
        print(f"\n📂 データ取得中: {analysis_date} から過去{analysis_days}日分...")
        
        start_date = analysis_date - timedelta(days=analysis_days)
        
        # サンプルデータを使用（実データベース接続の代替）
        dates = [analysis_date - timedelta(days=i) for i in range(analysis_days)]
        maturities = np.array([0.5, 1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40])
        
        # 範囲内の残存年数のみ使用
        valid_maturities = maturities[(maturities >= min_maturity) & (maturities <= max_maturity)]
        
        data = []
        for trade_date in dates:
            # 基本的なイールドカーブの形状
            base_curve = 0.5 + 0.02 * np.log(valid_maturities + 0.1)
            
            # ランダムな変動
            level_shift = np.random.normal(0, 0.2)
            slope_change = np.random.normal(0, 0.1) * np.log(valid_maturities + 0.1)
            curvature_change = np.random.normal(0, 0.05) * (valid_maturities - 5)**2 / 25
            
            yields = base_curve + level_shift + slope_change + curvature_change
            
            for maturity, yield_rate in zip(valid_maturities, yields):
                data.append({
                    'trade_date': trade_date,
                    'maturity_years': maturity,
                    'yield_rate': yield_rate,
                    'bond_name': f'{maturity:.2f}年債'
                })
        
        df = pd.DataFrame(data)
        print(f"✅ {len(df)}件のデータを取得しました")
        return df
    
    def run_analysis(self, params, df):
        """主成分分析を実行"""
        from analysis.simple_pca_demo import interpolate_yield_curves, perform_pca
        
        print("\n🔄 スプライン補間処理中...")
        
        # 残存年数範囲を設定
        self.maturity_range = np.linspace(params['min_maturity'], params['max_maturity'], 100)
        
        # スプライン補間
        interpolated_data = interpolate_yield_curves(df)
        
        # 主成分分析実行
        pca_result = perform_pca(interpolated_data, self.n_components)
        
        return pca_result
    
    def generate_outputs(self, params, pca_result):
        """結果出力（レベル別）"""
        timestamp = params['timestamp']
        analysis_date = params['analysis_date']
        n_components_restore = params['n_components_restore']
        output_level = params['output_level']
        
        print(f"\n📊 結果出力中（レベル{output_level}）...")
        
        # レベル1: 基本グラフ
        self.plot_principal_components(pca_result, timestamp)
        reconstruction_result = self.reconstruct_and_plot(pca_result, analysis_date, n_components_restore, timestamp)
        
        # レベル2: 統計情報
        if output_level >= 2:
            self.print_detailed_statistics(pca_result, reconstruction_result)
        
        # レベル3: CSVエクスポート
        if output_level >= 3:
            self.export_to_csv(pca_result, reconstruction_result, timestamp)
    
    def plot_principal_components(self, pca_result, timestamp):
        """主成分ベクトルをプロット"""
        plt.figure(figsize=(15, 10))
        colors = ['blue', 'red', 'green', 'orange', 'purple']
        
        for i in range(self.n_components):
            plt.subplot(2, 3, i+1)
            component = pca_result['pca_model'].components_[i]
            
            plt.plot(pca_result['common_maturities'], component, 
                    color=colors[i], linewidth=2, marker='o', markersize=4)
            plt.title(f'第{i+1}主成分 (寄与率: {pca_result["pca_model"].explained_variance_ratio_[i]:.1%})')
            plt.xlabel('残存年数')
            plt.ylabel('主成分ベクトル')
            plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.suptitle('Principal Component Vectors', fontsize=14, y=1.02)
        print(f"📊 主成分ベクトルグラフを表示中...")
        plt.show()
    
    def reconstruct_and_plot(self, pca_result, target_date, n_components_restore, timestamp):
        """復元とプロット"""
        from analysis.simple_pca_demo import reconstruct_yield_curve, plot_reconstruction_comparison
        
        # 復元
        reconstruction_result = reconstruct_yield_curve(pca_result, target_date, n_components_restore)
        
        # プロット
        plot_reconstruction_comparison(reconstruction_result, pca_result, n_components_restore)
        
        return reconstruction_result
    
    def print_detailed_statistics(self, pca_result, reconstruction_result):
        """詳細統計情報を表示"""
        print("\n📈 詳細分析結果:")
        print("=" * 50)
        
        # 主成分分析統計
        explained_ratio = pca_result['pca_model'].explained_variance_ratio_
        for i, ratio in enumerate(explained_ratio):
            print(f"第{i+1}主成分: 寄与率 {ratio:.3%}, 累積寄与率 {np.sum(explained_ratio[:i+1]):.3%}")
        
        # データ統計
        yield_matrix = pca_result['yield_matrix']
        print(f"\n📊 データ統計:")
        print(f"   分析日数: {len(yield_matrix)}日")
        print(f"   残存年数点数: {len(pca_result['common_maturities'])}点")
        print(f"   利回り範囲: {np.min(yield_matrix):.3f}% ～ {np.max(yield_matrix):.3f}%")
        print(f"   平均利回り: {np.mean(yield_matrix):.3f}%")
        print(f"   利回り標準偏差: {np.std(yield_matrix):.3f}%")
    
    def export_to_csv(self, pca_result, reconstruction_result, timestamp):
        """CSV形式でデータエクスポート"""
        # 主成分ベクトル
        components_df = pd.DataFrame(
            pca_result['pca_model'].components_.T,
            index=pca_result['common_maturities'],
            columns=[f'PC{i+1}' for i in range(self.n_components)]
        )
        components_df.index.name = 'maturity_years'
        components_filename = f"pca_components_{timestamp}.csv"
        components_df.to_csv(components_filename)
        print(f"📋 主成分ベクトルCSV保存: {components_filename}")
        
        # 復元結果
        reconstruction_df = pd.DataFrame({
            'maturity_years': reconstruction_result['maturities'],
            'original_yield': reconstruction_result['original'],
            'reconstructed_yield': reconstruction_result['reconstructed'],
            'difference': reconstruction_result['difference']
        })
        reconstruction_filename = f"reconstruction_result_{timestamp}.csv"
        reconstruction_df.to_csv(reconstruction_filename, index=False)
        print(f"📋 復元結果CSV保存: {reconstruction_filename}")

def main():
    """メイン実行関数"""
    print("🎯 対話式主成分分析システム")
    print("🌐 Webアプリ化対応版")
    print("=" * 60)
    
    try:
        # 分析システム初期化
        pca_system = InteractivePCA()
        
        # ユーザー入力取得
        params = pca_system.get_user_inputs()
        if params is None:
            return 1
        
        # データ取得
        df = pca_system.get_historical_data(params)
        
        # 主成分分析実行
        pca_result = pca_system.run_analysis(params, df)
        
        # 結果出力
        pca_system.generate_outputs(params, pca_result)
        
        print("\n🎉 主成分分析完了！")
        print("\n💡 Webアプリ化用パラメータ:")
        for key, value in params.items():
            print(f"   {key}: {value}")
        
    except Exception as e:
        print(f"\n❌ エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())