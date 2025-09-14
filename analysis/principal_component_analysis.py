#!/usr/bin/env python3
"""
Principal Component Analysis for Yield Curve
スプライン補間によるイールドカーブの主成分分析システム
"""

import sys
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, date, timedelta
from sklearn.decomposition import PCA
from scipy.interpolate import CubicSpline
import warnings
warnings.filterwarnings('ignore')

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.utils.database_manager import DatabaseManager

class YieldCurvePCA:
    """イールドカーブ主成分分析クラス"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.maturity_range = np.linspace(0.5, 40, 100)  # 0.5年から40年、100点
        self.n_components = 5  # 第5主成分まで計算
        
    def get_user_inputs(self):
        """ユーザー入力を取得"""
        print("🔍 主成分分析パラメータ設定")
        print("=" * 50)
        
        # 分析対象日付の入力
        while True:
            try:
                analysis_date_str = input("📅 分析対象日付 (YYYY-MM-DD): ")
                analysis_date = datetime.strptime(analysis_date_str, "%Y-%m-%d").date()
                break
            except ValueError:
                print("❌ 正しい日付形式で入力してください (例: 2024-12-31)")
        
        # 分析日数の入力
        while True:
            try:
                analysis_days = int(input("📊 過去何日分のデータを使用しますか: "))
                if analysis_days > 0:
                    break
                else:
                    print("❌ 正の整数を入力してください")
            except ValueError:
                print("❌ 正の整数を入力してください")
        
        # 復元に使用する主成分数の入力
        while True:
            try:
                n_components_restore = int(input("🔧 復元に使用する主成分数 (1-5): "))
                if 1 <= n_components_restore <= 5:
                    break
                else:
                    print("❌ 1から5の間で入力してください")
            except ValueError:
                print("❌ 正の整数を入力してください")
        
        return analysis_date, analysis_days, n_components_restore
    
    def get_historical_data(self, end_date, days):
        """指定期間のヒストリカルデータを取得"""
        print(f"\n📂 データ取得中: {end_date} から過去{days}日分...")
        
        start_date = end_date - timedelta(days=days)
        
        # データベースからデータ取得
        query = """
        SELECT trade_date, maturity_years, ave_compound_yield, bond_name
        FROM bond_data 
        WHERE trade_date >= %s AND trade_date <= %s
        AND maturity_years >= 0.5
        AND ave_compound_yield IS NOT NULL
        ORDER BY trade_date, maturity_years
        """
        
        data = self.db_manager.fetch_data(query, (start_date, end_date))
        
        if not data:
            raise ValueError(f"指定期間 {start_date} ~ {end_date} のデータが見つかりません")
        
        df = pd.DataFrame(data, columns=['trade_date', 'maturity_years', 'yield_rate', 'bond_name'])
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        print(f"✅ {len(df)}件のデータを取得しました")
        print(f"📅 日付範囲: {df['trade_date'].min().date()} ~ {df['trade_date'].max().date()}")
        
        return df
    
    def interpolate_yield_curves(self, df):
        """各日のイールドカーブをスプライン補間"""
        print("\n🔄 スプライン補間処理中...")
        
        interpolated_data = []
        dates = df['trade_date'].unique()
        
        for trade_date in dates:
            day_data = df[df['trade_date'] == trade_date].copy()
            
            if len(day_data) < 3:  # スプライン補間には最低3点必要
                continue
            
            # 残存年数でソート
            day_data = day_data.sort_values('maturity_years')
            
            # 重複する残存年数がある場合は平均値を取る
            day_data = day_data.groupby('maturity_years')['yield_rate'].mean().reset_index()
            
            try:
                # スプライン補間
                cs = CubicSpline(day_data['maturity_years'], day_data['yield_rate'])
                
                # 補間範囲内のデータのみ使用
                min_maturity = max(0.5, day_data['maturity_years'].min())
                max_maturity = min(40, day_data['maturity_years'].max())
                
                if max_maturity <= min_maturity:
                    continue
                
                # 補間用の残存年数範囲を調整
                valid_range = (self.maturity_range >= min_maturity) & (self.maturity_range <= max_maturity)
                valid_maturities = self.maturity_range[valid_range]
                
                if len(valid_maturities) == 0:
                    continue
                
                interpolated_yields = cs(valid_maturities)
                
                # 結果を保存
                interpolated_data.append({
                    'date': trade_date,
                    'maturities': valid_maturities,
                    'yields': interpolated_yields,
                    'original_data': day_data
                })
                
            except Exception as e:
                print(f"⚠️ {trade_date.date()} のスプライン補間でエラー: {e}")
                continue
        
        print(f"✅ {len(interpolated_data)}日分の補間完了")
        return interpolated_data
    
    def perform_pca(self, interpolated_data):
        """主成分分析を実行"""
        print(f"\n🔬 主成分分析実行中 (第{self.n_components}主成分まで)...")
        
        if len(interpolated_data) < 2:
            raise ValueError("主成分分析には最低2日分のデータが必要です")
        
        # 全ての日付で共通の残存年数範囲を見つける
        common_maturities = None
        for data in interpolated_data:
            if common_maturities is None:
                common_maturities = set(np.round(data['maturities'], 2))
            else:
                common_maturities = common_maturities.intersection(set(np.round(data['maturities'], 2)))
        
        common_maturities = sorted(list(common_maturities))
        
        if len(common_maturities) < 10:
            raise ValueError("共通の残存年数範囲が不足しています")
        
        # データマトリックスを構築
        yield_matrix = []
        dates = []
        
        for data in interpolated_data:
            # 共通の残存年数に対応する利回りを取得
            cs = CubicSpline(data['maturities'], data['yields'])
            common_yields = cs(common_maturities)
            
            yield_matrix.append(common_yields)
            dates.append(data['date'])
        
        yield_matrix = np.array(yield_matrix)
        
        # PCA実行
        pca = PCA(n_components=self.n_components)
        pca_result = pca.fit_transform(yield_matrix)
        
        print(f"✅ PCA完了")
        print(f"📊 寄与率: {pca.explained_variance_ratio_}")
        print(f"📊 累積寄与率: {np.cumsum(pca.explained_variance_ratio_)}")
        
        return {
            'pca_model': pca,
            'pca_result': pca_result,
            'yield_matrix': yield_matrix,
            'common_maturities': np.array(common_maturities),
            'dates': dates,
            'interpolated_data': interpolated_data
        }
    
    def plot_principal_components(self, pca_result):
        """主成分ベクトルをプロット"""
        print("\n📈 主成分ベクトル描画中...")
        
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
        plt.show()
    
    def reconstruct_yield_curve(self, pca_result, target_date, n_components_restore):
        """指定日のイールドカーブを指定主成分数で復元"""
        print(f"\n🔧 {target_date.date()} のイールドカーブを第{n_components_restore}主成分で復元中...")
        
        # 対象日のデータを見つける
        target_index = None
        for i, date in enumerate(pca_result['dates']):
            if date.date() == target_date:
                target_index = i
                break
        
        if target_index is None:
            raise ValueError(f"指定日 {target_date} のデータが見つかりません")
        
        # 元のイールドカーブ
        original_curve = pca_result['yield_matrix'][target_index]
        
        # 指定主成分数で復元
        mean_curve = np.mean(pca_result['yield_matrix'], axis=0)
        pca_components = pca_result['pca_result'][target_index][:n_components_restore]
        principal_components = pca_result['pca_model'].components_[:n_components_restore]
        
        reconstructed_curve = mean_curve + np.dot(pca_components, principal_components)
        
        # 差分計算
        difference = original_curve - reconstructed_curve
        
        return {
            'original': original_curve,
            'reconstructed': reconstructed_curve,
            'difference': difference,
            'maturities': pca_result['common_maturities']
        }
    
    def plot_reconstruction_comparison(self, reconstruction_result, target_date, pca_result, n_components_restore):
        """復元結果と差分をプロット"""
        print(f"\n📊 復元結果グラフ描画中...")
        
        # 元の個別銘柄データを取得
        original_data = None
        for data in pca_result['interpolated_data']:
            if data['date'].date() == target_date:
                original_data = data['original_data']
                break
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # 上段: 元のカーブと復元カーブの比較
        ax1.plot(reconstruction_result['maturities'], reconstruction_result['original'], 
                'b-', linewidth=2, label='元のカーブ (スプライン補間)')
        ax1.plot(reconstruction_result['maturities'], reconstruction_result['reconstructed'], 
                'r--', linewidth=2, label=f'復元カーブ (第{n_components_restore}主成分)')
        
        # 元の個別銘柄データもプロット
        if original_data is not None:
            ax1.scatter(original_data['maturity_years'], original_data['yield_rate'], 
                       c='black', s=30, alpha=0.7, label='実際の銘柄データ', zorder=5)
        
        ax1.set_xlabel('残存年数')
        ax1.set_ylabel('利回り (%)')
        ax1.set_title(f'{target_date} イールドカーブ復元結果')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 下段: 差分グラフ（イールドカーブスタイル）
        if original_data is not None:
            # 各銘柄の差分を計算
            differences = []
            bond_names = []
            maturities = []
            
            for _, row in original_data.iterrows():
                maturity = row['maturity_years']
                
                # 最も近い補間点を見つける
                idx = np.argmin(np.abs(reconstruction_result['maturities'] - maturity))
                original_yield = reconstruction_result['original'][idx]
                reconstructed_yield = reconstruction_result['reconstructed'][idx]
                
                difference = original_yield - reconstructed_yield
                
                differences.append(difference)
                bond_names.append(row['bond_name'] if pd.notna(row['bond_name']) else f"{maturity:.1f}年")
                maturities.append(maturity)
            
            # 差分をプロット（各銘柄が点で表示）
            scatter = ax2.scatter(maturities, differences, c=differences, cmap='RdYlBu_r', 
                                s=50, alpha=0.8, edgecolors='black', linewidth=0.5)
            
            # ホバー情報のための注釈（簡易版）
            for i, (mat, diff, name) in enumerate(zip(maturities, differences, bond_names)):
                if abs(diff) > np.std(differences):  # 大きな差分のみ表示
                    ax2.annotate(f'{name}\n{diff:.3f}%', 
                               xy=(mat, diff), xytext=(5, 5), 
                               textcoords='offset points', fontsize=8,
                               bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7))
        
        ax2.axhline(y=0, color='black', linestyle='-', alpha=0.5)
        ax2.set_xlabel('残存年数')
        ax2.set_ylabel('差分 (元の利回り - 復元利回り) (%)')
        ax2.set_title(f'復元誤差 (第{n_components_restore}主成分使用)')
        ax2.grid(True, alpha=0.3)
        
        # カラーバー追加
        if original_data is not None:
            plt.colorbar(scatter, ax=ax2, label='差分 (%)')
        
        plt.tight_layout()
        plt.show()
        
        # 統計情報表示
        if len(differences) > 0:
            print(f"\n📊 復元誤差統計:")
            print(f"   平均誤差: {np.mean(differences):.4f}%")
            print(f"   標準偏差: {np.std(differences):.4f}%")
            print(f"   最大誤差: {np.max(differences):.4f}%")
            print(f"   最小誤差: {np.min(differences):.4f}%")

def main():
    """メイン実行関数"""
    print("🎯 イールドカーブ主成分分析システム")
    print("=" * 60)
    
    try:
        # 分析クラス初期化
        pca_analyzer = YieldCurvePCA()
        
        # ユーザー入力取得
        analysis_date, analysis_days, n_components_restore = pca_analyzer.get_user_inputs()
        
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
        
        print("\n🎉 主成分分析完了！")
        
    except Exception as e:
        print(f"\n❌ エラーが発生しました: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())