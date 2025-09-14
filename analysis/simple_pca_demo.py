#!/usr/bin/env python3
"""
シンプルな主成分分析デモ
サンプルデータを使って主成分分析の動作を確認
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from scipy.interpolate import CubicSpline
from datetime import datetime, date, timedelta
import warnings
warnings.filterwarnings('ignore')

def generate_sample_yield_data():
    """サンプルのイールドカーブデータを生成"""
    
    # 基本的な残存年数（0.5年から40年）
    maturities = np.array([0.5, 1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40])
    
    # 30日分のイールドカーブデータを生成
    dates = [date.today() - timedelta(days=i) for i in range(30)]
    
    yield_data = []
    
    for i, trade_date in enumerate(dates):
        # 基本的なイールドカーブの形状（上向き）
        base_curve = 0.5 + 0.02 * np.log(maturities + 0.1)
        
        # ランダムな変動を追加
        level_shift = np.random.normal(0, 0.2)  # 水準シフト
        slope_change = np.random.normal(0, 0.1) * np.log(maturities + 0.1)  # 傾きの変化
        curvature_change = np.random.normal(0, 0.05) * (maturities - 5)**2 / 25  # 曲率の変化
        
        yields = base_curve + level_shift + slope_change + curvature_change
        
        # 各銘柄のデータを作成
        for j, (maturity, yield_rate) in enumerate(zip(maturities, yields)):
            yield_data.append({
                'trade_date': trade_date,
                'maturity_years': maturity,
                'yield_rate': yield_rate,
                'bond_name': f'{maturity:.2f}年債'
            })
    
    return pd.DataFrame(yield_data)

def interpolate_yield_curves(df):
    """各日のイールドカーブをスプライン補間"""
    print("🔄 スプライン補間処理中...")
    
    interpolated_data = []
    dates = df['trade_date'].unique()
    maturity_range = np.linspace(0.5, 40, 100)  # 0.5年から40年
    
    for trade_date in dates:
        day_data = df[df['trade_date'] == trade_date].copy()
        day_data = day_data.sort_values('maturity_years')
        
        try:
            cs = CubicSpline(day_data['maturity_years'], day_data['yield_rate'])
            interpolated_yields = cs(maturity_range)
            
            interpolated_data.append({
                'date': trade_date,
                'maturities': maturity_range,
                'yields': interpolated_yields,
                'original_data': day_data
            })
        except Exception as e:
            print(f"⚠️ {trade_date} のスプライン補間でエラー: {e}")
            continue
    
    print(f"✅ {len(interpolated_data)}日分の補間完了")
    return interpolated_data

def perform_pca(interpolated_data, n_components=5):
    """主成分分析を実行"""
    print(f"🔬 主成分分析実行中 (第{n_components}主成分まで)...")
    
    # データマトリックスを構築
    yield_matrix = []
    dates = []
    
    for data in interpolated_data:
        yield_matrix.append(data['yields'])
        dates.append(data['date'])
    
    yield_matrix = np.array(yield_matrix)
    common_maturities = interpolated_data[0]['maturities']
    
    # PCA実行
    pca = PCA(n_components=n_components)
    pca_result = pca.fit_transform(yield_matrix)
    
    print(f"✅ PCA完了")
    print(f"📊 寄与率: {pca.explained_variance_ratio_}")
    print(f"📊 累積寄与率: {np.cumsum(pca.explained_variance_ratio_)}")
    
    return {
        'pca_model': pca,
        'pca_result': pca_result,
        'yield_matrix': yield_matrix,
        'common_maturities': common_maturities,
        'dates': dates,
        'interpolated_data': interpolated_data
    }

def plot_principal_components(pca_result):
    """主成分ベクトルをプロット"""
    print("📈 主成分ベクトル描画中...")
    
    plt.figure(figsize=(15, 10))
    colors = ['blue', 'red', 'green', 'orange', 'purple']
    
    for i in range(len(pca_result['pca_model'].components_)):
        plt.subplot(2, 3, i+1)
        component = pca_result['pca_model'].components_[i]
        
        plt.plot(pca_result['common_maturities'], component, 
                color=colors[i], linewidth=2, marker='o', markersize=4)
        plt.title(f'PC{i+1} (Explained Variance: {pca_result["pca_model"].explained_variance_ratio_[i]:.1%})')
        plt.xlabel('Maturity (Years)')
        plt.ylabel('Principal Component Vector')
        plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.suptitle('Principal Component Vectors', fontsize=14, y=1.02)
    print(f"📊 Displaying Principal Component Vectors Graph...")
    plt.show()

def reconstruct_yield_curve(pca_result, target_date, n_components_restore):
    """指定日のイールドカーブを指定主成分数で復元"""
    print(f"🔧 Reconstructing yield curve for {target_date} using {n_components_restore} components...")
    
    # 対象日のデータを見つける
    target_index = None
    for i, date_obj in enumerate(pca_result['dates']):
        if date_obj == target_date:
            target_index = i
            break
    
    if target_index is None:
        target_index = 0  # デモなので最初の日付を使用
        target_date = pca_result['dates'][0]
        print(f"指定日が見つからないため、{target_date} を使用します")
    
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
        'maturities': pca_result['common_maturities'],
        'target_date': target_date
    }

def plot_reconstruction_comparison(reconstruction_result, pca_result, n_components_restore):
    """復元結果と差分をプロット"""
    print("📊 復元結果グラフ描画中...")
    
    target_date = reconstruction_result['target_date']
    
    # 元の個別銘柄データを取得
    original_data = None
    for data in pca_result['interpolated_data']:
        if data['date'] == target_date:
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
                   c='black', s=50, alpha=0.7, label='Actual Bond Data', zorder=5)
    
    ax1.set_xlabel('Maturity (Years)')
    ax1.set_ylabel('Yield Rate (%)')
    ax1.set_title(f'{target_date} Yield Curve Reconstruction Result')
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
            bond_names.append(row['bond_name'])
            maturities.append(maturity)
        
        # 差分をプロット（各銘柄が点で表示）
        scatter = ax2.scatter(maturities, differences, c=differences, cmap='RdYlBu_r', 
                            s=60, alpha=0.8, edgecolors='black', linewidth=0.5)
        
        # 大きな差分のみ注釈表示
        for i, (mat, diff, name) in enumerate(zip(maturities, differences, bond_names)):
            if abs(diff) > np.std(differences):
                ax2.annotate(f'{name}\n{diff:.3f}%', 
                           xy=(mat, diff), xytext=(5, 5), 
                           textcoords='offset points', fontsize=8,
                           bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7))
    
    ax2.axhline(y=0, color='black', linestyle='-', alpha=0.5)
    ax2.set_xlabel('Maturity (Years)')
    ax2.set_ylabel('Difference (Original - Reconstructed) (%)')
    ax2.set_title(f'Reconstruction Error (Using PC{n_components_restore})')
    ax2.grid(True, alpha=0.3)
    
    # カラーバー追加
    if original_data is not None and len(differences) > 0:
        plt.colorbar(scatter, ax=ax2, label='Difference (%)')
        
        # 統計情報表示
        print(f"\n📊 Reconstruction Error Statistics:")
        print(f"   Mean Error: {np.mean(differences):.4f}%")
        print(f"   Std Deviation: {np.std(differences):.4f}%")
        print(f"   Max Error: {np.max(differences):.4f}%")
        print(f"   Min Error: {np.min(differences):.4f}%")
    
    plt.tight_layout()
    plt.suptitle('Yield Curve Reconstruction Comparison', fontsize=14, y=1.02)
    print(f"📊 Displaying Reconstruction Comparison Graph...")
    plt.show()

def main():
    """メイン実行関数"""
    print("🎯 Simple Principal Component Analysis Demo (Using Sample Data)")
    print("=" * 60)
    
    # デモパラメータ
    n_components_restore = 3
    
    print(f"📊 Generated Data: 30 days of yield curves")
    print(f"🔧 Reconstruction Components: {n_components_restore} components")
    print()
    
    try:
        # サンプルデータ生成
        df = generate_sample_yield_data()
        print(f"✅ Generated {len(df)} sample data points")
        
        # スプライン補間
        interpolated_data = interpolate_yield_curves(df)
        
        # 主成分分析実行
        pca_result = perform_pca(interpolated_data)
        
        # 主成分ベクトル描画
        plot_principal_components(pca_result)
        
        # 最新日のイールドカーブ復元
        target_date = pca_result['dates'][0]  # 最新日
        reconstruction_result = reconstruct_yield_curve(
            pca_result, target_date, n_components_restore)
        
        # 復元結果と差分の描画
        plot_reconstruction_comparison(
            reconstruction_result, pca_result, n_components_restore)
        
        print("\n🎉 Principal Component Analysis Demo Completed!")
        print("\n💡 This demo demonstrates the following features:")
        print("   ✅ Yield curve normalization using spline interpolation")
        print("   ✅ Dimensionality reduction using Principal Component Analysis")
        print("   ✅ Visualization of 1st to 5th principal component vectors")
        print("   ✅ Yield curve reconstruction using specified components")
        print("   ✅ Bond-level visualization of reconstruction errors")
        
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())