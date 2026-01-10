import sys
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d
from sklearn.decomposition import PCA
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# プロジェクトルートをパスに追加
project_root = os.path.abspath(os.path.join(os.getcwd(), '../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Adjust path for running from root
if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())

try:
    from data.utils.database_manager import DatabaseManager
except ImportError:
    # Try relative import if running as script from folder
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
    from data.utils.database_manager import DatabaseManager

# 日本語フォント設定 (Mac用)
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'Hiragino Sans', 'Yu Gothic', 'Meirio']
plt.rcParams['axes.unicode_minus'] = False

def fetch_all_data(db_manager, start_date='2022-01-01'):
    print(f"Fetching data from {start_date}...")
    sql = """
        SELECT trade_date, bond_name, due_date, ave_compound_yield
        FROM bond_data
        WHERE trade_date >= %s
          AND ave_compound_yield IS NOT NULL
          AND ave_compound_yield BETWEEN -1 AND 10
        ORDER BY trade_date, due_date
    """
    try:
        data = db_manager.select_as_dict(sql, (start_date,))
    except AttributeError:
        rows = db_manager.execute_query(sql, (start_date,))
        cols = ['trade_date', 'bond_name', 'due_date', 'ave_compound_yield']
        data = [dict(zip(cols, row)) for row in rows]
    
    if not data:
        print("No data found.")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df['due_date'] = pd.to_datetime(df['due_date'])
    # Convert Decimal to float
    df['ave_compound_yield'] = df['ave_compound_yield'].astype(float)
    df['maturity'] = (df['due_date'] - df['trade_date']).dt.days / 365.25
    df = df[df['maturity'] >= 0.5]
    
    print(f"Data loaded: {len(df)} records")
    return df

class PCAStrategyBacktestDoubleHedged:
    def __init__(self, df, window=50, grid_points=None):
        self.df = df
        self.window = window
        if grid_points is None:
            self.grid_points = np.linspace(1, 40, 40)
        else:
            self.grid_points = grid_points
        
        self.dates = sorted(df['trade_date'].unique())
        self.results = []
    
    def interpolate_curve(self, daily_df, target_grid):
        daily_df = daily_df.sort_values('maturity')
        daily_df = daily_df.drop_duplicates(subset='maturity')
        x = daily_df['maturity'].values
        y = daily_df['ave_compound_yield'].values
        if len(x) < 3: return None
        try:
            f = interp1d(x, y, kind='linear', fill_value='extrapolate')
            return f(target_grid)
        except: return None

    def get_bond_loadings(self, maturity, pc_loadings):
        """指定された残存期間におけるPC1とPC2のLoadingを取得"""
        try:
            f1 = interp1d(self.grid_points, pc_loadings[0], kind='linear', fill_value='extrapolate')
            f2 = interp1d(self.grid_points, pc_loadings[1], kind='linear', fill_value='extrapolate')
            return np.array([float(f1(maturity)), float(f2(maturity))])
        except:
            return np.array([0.0, 0.0])

    def find_nearest_bond(self, daily_df, target_maturity):
        daily_df = daily_df.copy()
        daily_df['diff'] = (daily_df['maturity'] - target_maturity).abs()
        return daily_df.loc[daily_df['diff'].idxmin()]

    def run(self):
        print(f"Starting double hedged backtest over {len(self.dates)} days")
        
        for i in range(self.window, len(self.dates) - 1):
            current_date, next_date = self.dates[i], self.dates[i+1]
            
            # 1. PCA学習
            train_curves = []
            for d in self.dates[i-self.window : i]:
                curve = self.interpolate_curve(self.df[self.df['trade_date'] == d], self.grid_points)
                if curve is not None: train_curves.append(curve)
            
            if len(train_curves) < self.window * 0.8: continue
            pca = PCA(n_components=2).fit(np.array(train_curves))
            pc_loadings = pca.components_ # PC1 and PC2 loadings
            
            # 2. 当日の分析と銘柄選定
            current_df = self.df[self.df['trade_date'] == current_date]
            current_curve_grid = self.interpolate_curve(current_df, self.grid_points)
            if current_curve_grid is None: continue
            
            # 復元誤差計算
            scores = pca.transform(current_curve_grid.reshape(1, -1))
            reconstructed_grid = pca.inverse_transform(scores).flatten()
            model_interp = interp1d(self.grid_points, reconstructed_grid, kind='linear', fill_value='extrapolate')
            
            current_df = current_df.copy()
            current_df['error'] = current_df['ave_compound_yield'] - model_interp(current_df['maturity'])
            
            if current_df['error'].max() <= 0 or current_df['error'].min() >= 0: continue
            
            long_bond = current_df.loc[current_df['error'].idxmax()]
            short_bond = current_df.loc[current_df['error'].idxmin()]
            
            # ヘッジ銘柄の選定 (7Yと10Y)
            h7_bond = self.find_nearest_bond(current_df, 7.0)
            h10_bond = self.find_nearest_bond(current_df, 10.0)
            
            # Loadings取得
            L_v = self.get_bond_loadings(long_bond['maturity'], pc_loadings)
            S_v = self.get_bond_loadings(short_bond['maturity'], pc_loadings)
            H7_v = self.get_bond_loadings(h7_bond['maturity'], pc_loadings)
            H10_v = self.get_bond_loadings(h10_bond['maturity'], pc_loadings)
            
            # ポートフォリオの露出: E = L_v - S_v
            E = L_v - S_v
            
            # ヘッジ方程式: E + q7*H7_v + q10*H10_v = 0
            # [H7_v1 H10_v1] [q7 ] = [-E1]
            # [H7_v2 H10_v2] [q10] = [-E2]
            A = np.array([[H7_v[0], H10_v[0]], [H7_v[1], H10_v[1]]])
            B = -E
            
            try:
                q = np.linalg.solve(A, B)
                q7, q10 = np.clip(q, -10, 10) # 異常比率ガード
            except np.linalg.LinAlgError:
                q7, q10 = 0.0, 0.0
            
            # 3. 翌日の損益計算
            next_df = self.df[self.df['trade_date'] == next_date]
            def get_pnl(bond, side, qty=1.0):
                next_b = next_df[next_df['bond_name'] == bond['bond_name']]
                if next_b.empty: return 0.0
                diff = (bond['ave_compound_yield'] - next_b.iloc[0]['ave_compound_yield']) * 100
                return side * qty * diff

            lpnl = get_pnl(long_bond, 1)
            spnl = get_pnl(short_bond, -1)
            h7pnl = get_pnl(h7_bond, 1, q7)
            h10pnl = get_pnl(h10_bond, 1, q10)
            
            no_hedge = lpnl + spnl
            double_hedged = no_hedge + h7pnl + h10pnl
            
            self.results.append({
                'date': current_date,
                'no_hedge_pnl': no_hedge,
                'double_hedged_pnl': double_hedged,
                'q7': q7, 'q10': q10
            })
            
            if i % 20 == 0:
                print(f"Processed {current_date.date()}: NoHedge={no_hedge:.2f}, DoubleHedged={double_hedged:.2f}")
                
        return pd.DataFrame(self.results)

if __name__ == "__main__":
    db_manager = DatabaseManager()
    df_all = fetch_all_data(db_manager, start_date='2024-01-01')
    if not df_all.empty:
        backtester = PCAStrategyBacktestDoubleHedged(df_all, window=50)
        results_df = backtester.run()
        
        output_dir = 'notebooks/pca' if os.path.isdir('notebooks/pca') else '.'
        results_df.to_csv(os.path.join(output_dir, 'backtest_results_double_hedged.csv'), index=False)
        print(f"Results saved. Total Return (Double Hedged): {results_df['double_hedged_pnl'].sum():.2f} bp")
