#!/usr/bin/env python3
"""
Yield Curve Analyzer
日本国債のイールドカーブ分析・可視化モジュール
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, date
import sys
import os
from typing import Optional, Tuple, List
import warnings
warnings.filterwarnings('ignore')

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.db.sync_client import DatabaseManager

class YieldCurveAnalyzer:
    """イールドカーブ分析クラス"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        
        # フォント設定（シンプル）
        plt.rcParams['font.family'] = 'DejaVu Sans'
        plt.rcParams['axes.unicode_minus'] = False
    
    def get_bond_data(self, target_date: str) -> pd.DataFrame:
        """
        指定日の債券データを取得
        
        Args:
            target_date (str): 対象日付 (YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: 債券データ
        """
        try:
            # select_as_dictを使用して辞書のリストを取得
            query = """
                SELECT * FROM bond_data 
                WHERE trade_date = %s 
                ORDER BY due_date ASC
            """
            
            rows = self.db_manager.select_as_dict(query, (target_date,))
            
            if rows:
                df = pd.DataFrame(rows)
                print(f"✅ データ取得成功: {len(df)}件 ({target_date})")
                return df
            else:
                print(f"📭 データなし: {target_date}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ データ取得例外: {e}")
            return pd.DataFrame()
    
    def calculate_years_to_maturity(self, df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        """
        満期までの年数を計算
        
        Args:
            df (pd.DataFrame): 債券データ
            trade_date (str): 取引日
            
        Returns:
            pd.DataFrame: 年数計算済みデータ
        """
        df = df.copy()
        
        # 日付文字列をdatetimeに変換
        trade_dt = datetime.strptime(trade_date, '%Y-%m-%d')
        df['due_date_dt'] = pd.to_datetime(df['due_date'])
        
        # 満期までの年数を計算
        df['years_to_maturity'] = (df['due_date_dt'] - trade_dt).dt.days / 365.25
        
        # 負の年数（既に満期を過ぎた債券）を除外
        df = df[df['years_to_maturity'] > 0]
        
        return df
    
    def filter_for_yield_curve(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        イールドカーブ作成用にデータをフィルタリング
        
        Args:
            df (pd.DataFrame): 債券データ
            
        Returns:
            pd.DataFrame: フィルタリング済みデータ
        """
        # 利回りがNullでないデータのみ
        df = df[df['ave_compound_yield'].notna()].copy()
        
        # 異常値を除外（利回り 0-10% の範囲）
        df = df[(df['ave_compound_yield'] >= 0) & (df['ave_compound_yield'] <= 10)]
        
        # 満期年数でソート
        df = df.sort_values('years_to_maturity')
        
        print(f"📊 イールドカーブ用データ: {len(df)}銘柄")
        print(f"   満期範囲: {df['years_to_maturity'].min():.1f} - {df['years_to_maturity'].max():.1f}年")
        print(f"   利回り範囲: {df['ave_compound_yield'].min():.3f} - {df['ave_compound_yield'].max():.3f}%")
        
        return df
    
    def create_yield_curve_plot(self, df: pd.DataFrame, target_date: str, 
                               save_path: Optional[str] = None) -> Tuple[plt.Figure, plt.Axes]:
        """
        イールドカーブのプロットを作成
        
        Args:
            df (pd.DataFrame): 処理済み債券データ
            target_date (str): 対象日付
            save_path (str, optional): 保存パス
            
        Returns:
            Tuple[plt.Figure, plt.Axes]: 図とAxesオブジェクト
        """
        # 図のサイズと設定
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # 基本プロット
        scatter = ax.scatter(
            df['years_to_maturity'], 
            df['ave_compound_yield'],
            alpha=0.7,
            s=60,
            c=df['years_to_maturity'],
            cmap='viridis'
        )
        
        # スムーズなカーブを描画（多項式フィッティング）
        if len(df) >= 3:
            try:
                # 年数でソートして重複を削除
                curve_df = df.drop_duplicates('years_to_maturity').sort_values('years_to_maturity')
                
                if len(curve_df) >= 3:
                    # 3次多項式でフィッティング
                    z = np.polyfit(curve_df['years_to_maturity'], curve_df['ave_compound_yield'], 3)
                    p = np.poly1d(z)
                    
                    # スムーズなx軸を作成
                    x_smooth = np.linspace(curve_df['years_to_maturity'].min(), 
                                         curve_df['years_to_maturity'].max(), 200)
                    y_smooth = p(x_smooth)
                    
                    ax.plot(x_smooth, y_smooth, 'r-', linewidth=2, alpha=0.8, label='イールドカーブ（3次多項式）')
            except Exception as e:
                print(f"⚠️  カーブフィッティング失敗: {e}")
        
        # グラフの装飾
        ax.set_xlabel('満期までの年数', fontsize=12)
        ax.set_ylabel('利回り (%)', fontsize=12)
        ax.set_title(f'日本国債イールドカーブ - {target_date}', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.legend()
        
        # カラーバー追加
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('満期までの年数', rotation=270, labelpad=15)
        
        # レイアウト調整
        plt.tight_layout()
        
        # 保存
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"💾 イールドカーブを保存: {save_path}")
        
        return fig, ax
    
    def create_maturity_distribution(self, df: pd.DataFrame, target_date: str) -> plt.Figure:
        """
        満期分布のヒストグラムを作成
        
        Args:
            df (pd.DataFrame): 処理済み債券データ
            target_date (str): 対象日付
            
        Returns:
            plt.Figure: 図オブジェクト
        """
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # 満期分布
        ax1.hist(df['years_to_maturity'], bins=20, alpha=0.7, color='skyblue', edgecolor='black')
        ax1.set_xlabel('満期までの年数')
        ax1.set_ylabel('債券数')
        ax1.set_title('満期分布')
        ax1.grid(True, alpha=0.3)
        
        # 利回り分布
        ax2.hist(df['ave_compound_yield'], bins=20, alpha=0.7, color='lightcoral', edgecolor='black')
        ax2.set_xlabel('利回り (%)')
        ax2.set_ylabel('債券数')
        ax2.set_title('利回り分布')
        ax2.grid(True, alpha=0.3)
        
        fig.suptitle(f'債券データ分布 - {target_date}', fontsize=14, fontweight='bold')
        plt.tight_layout()
        
        return fig
    
    def analyze_yield_curve(self, target_date: str, show_plots: bool = True, 
                          save_dir: Optional[str] = None) -> dict:
        """
        指定日のイールドカーブ分析を実行
        
        Args:
            target_date (str): 対象日付 (YYYY-MM-DD)
            show_plots (bool): プロット表示するか
            save_dir (str, optional): 保存ディレクトリ
            
        Returns:
            dict: 分析結果
        """
        print(f"🎯 イールドカーブ分析: {target_date}")
        print("-" * 50)
        
        # 1. データ取得
        df = self.get_bond_data(target_date)
        if df.empty:
            return {"error": "データが見つかりません"}
        
        # 2. 満期年数計算
        df = self.calculate_years_to_maturity(df, target_date)
        
        # 3. フィルタリング
        df_filtered = self.filter_for_yield_curve(df)
        
        if df_filtered.empty:
            return {"error": "フィルタリング後のデータがありません"}
        
        # 4. 基本統計
        stats = {
            'date': target_date,
            'total_bonds': len(df_filtered),
            'yield_stats': {
                'mean': df_filtered['ave_compound_yield'].mean(),
                'std': df_filtered['ave_compound_yield'].std(),
                'min': df_filtered['ave_compound_yield'].min(),
                'max': df_filtered['ave_compound_yield'].max(),
            },
            'maturity_stats': {
                'mean': df_filtered['years_to_maturity'].mean(),
                'min': df_filtered['years_to_maturity'].min(),
                'max': df_filtered['years_to_maturity'].max(),
            }
        }
        
        print(f"📊 基本統計:")
        print(f"   債券数: {stats['total_bonds']}銘柄")
        print(f"   平均利回り: {stats['yield_stats']['mean']:.3f}%")
        print(f"   利回り標準偏差: {stats['yield_stats']['std']:.3f}%")
        print(f"   平均満期: {stats['maturity_stats']['mean']:.1f}年")
        
        # 5. プロット作成
        if show_plots or save_dir:
            # イールドカーブ
            save_path1 = None
            if save_dir:
                os.makedirs(save_dir, exist_ok=True)
                save_path1 = os.path.join(save_dir, f"yield_curve_{target_date}.png")
            
            fig1, ax1 = self.create_yield_curve_plot(df_filtered, target_date, save_path1)
            
            # 分布図
            fig2 = self.create_maturity_distribution(df_filtered, target_date)
            
            save_path2 = None
            if save_dir:
                save_path2 = os.path.join(save_dir, f"distribution_{target_date}.png")
                fig2.savefig(save_path2, dpi=300, bbox_inches='tight')
                print(f"💾 分布図を保存: {save_path2}")
            
            if show_plots:
                plt.show()
            else:
                plt.close(fig1)
                plt.close(fig2)
        
        # 6. 結果にデータフレームを追加
        stats['data'] = df_filtered
        
        return stats

def main():
    """メイン実行"""
    if len(sys.argv) != 2:
        print("使用方法:")
        print("  python yield_curve_analyzer.py YYYY-MM-DD")
        print("\n例:")
        print("  python yield_curve_analyzer.py 2025-09-09")
        return 1
    
    target_date = sys.argv[1]
    
    # 分析実行
    analyzer = YieldCurveAnalyzer()
    result = analyzer.analyze_yield_curve(
        target_date=target_date,
        show_plots=True,
        save_dir="analysis/output"
    )
    
    if "error" in result:
        print(f"❌ エラー: {result['error']}")
        return 1
    
    print("\n✅ 分析完了!")
    return 0

if __name__ == "__main__":
    sys.exit(main())