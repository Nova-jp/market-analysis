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

class PCAStrategyBacktestHedged:
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
        
        if len(x) < 3:
            return None
        
        try:
            f = interp1d(x, y, kind='linear', fill_value='extrapolate')
            return f(target_grid)
        except:
            return None

    def get_bond_pc1_loading(self, maturity, pc1_grid_values):
        """指定された残存期間(maturity)におけるPC1のLoadingを補間で取得"""
        try:
            f = interp1d(self.grid_points, pc1_grid_values, kind='linear', fill_value='extrapolate')
            return float(f(maturity))
        except:
            return 0.0

    def find_hedge_bond(self, daily_df, target_maturity=7.0):
        """指定された残存期間に最も近い銘柄を探す"""
        daily_df = daily_df.copy()
        daily_df['diff'] = (daily_df['maturity'] - target_maturity).abs()
        nearest_bond = daily_df.loc[daily_df['diff'].idxmin()]
        return nearest_bond

    def run(self):
        print(f"Starting hedged backtest over {len(self.dates)} days with window={self.window}")
        
        for i in range(self.window, len(self.dates) - 1):
            current_date = self.dates[i]
            next_date = self.dates[i+1]
            
            # 1. 学習用データ構築
            train_dates = self.dates[i-self.window : i]
            train_curves = []
            
            for d in train_dates:
                d_df = self.df[self.df['trade_date'] == d]
                curve = self.interpolate_curve(d_df, self.grid_points)
                if curve is not None:
                    train_curves.append(curve)
            
            if len(train_curves) < self.window * 0.8:
                continue
            
            X_train = np.array(train_curves)
            
            # 2. PCA実行
            pca = PCA(n_components=2)
            try:
                pca.fit(X_train)
            except Exception as e:
                print(f"PCA fit failed on {current_date}: {e}")
                continue
            
            # PC1の固有ベクトル（Loading）を取得
            pc1_loadings = pca.components_[0]
            
            # 3. 当日の分析
            current_df = self.df[self.df['trade_date'] == current_date]
            current_curve_grid = self.interpolate_curve(current_df, self.grid_points)
            
            if current_curve_grid is None:
                continue
                
            scores = pca.transform(current_curve_grid.reshape(1, -1))
            reconstructed_grid = pca.inverse_transform(scores).flatten()
            
            model_interpolator = interp1d(self.grid_points, reconstructed_grid, 
                                          kind='linear', fill_value='extrapolate')
            
            current_df = current_df.copy()
            current_df['model_yield'] = model_interpolator(current_df['maturity'])
            current_df['error'] = current_df['ave_compound_yield'] - current_df['model_yield']
            
            # 4. シグナル生成（ロング・ショート候補）
            if current_df['error'].max() <= 0 or current_df['error'].min() >= 0:
                continue

            long_candidate = current_df.loc[current_df['error'].idxmax()]
            short_candidate = current_df.loc[current_df['error'].idxmin()]
            
            # --- ヘッジ計算 ---
            # 各銘柄のPC1 Loading（感応度）を取得
            l_loading = self.get_bond_pc1_loading(long_candidate['maturity'], pc1_loadings)
            s_loading = self.get_bond_pc1_loading(short_candidate['maturity'], pc1_loadings)
            
            # ポートフォリオのPC1感応度 (Long=+1, Short=-1)
            port_pc1_exposure = l_loading - s_loading
            
            # ヘッジ銘柄（7年債）の選定
            hedge_bond = self.find_hedge_bond(current_df, target_maturity=7.0)
            hedge_bond_name = hedge_bond['bond_name'] # 名前を固定
            h_loading = self.get_bond_pc1_loading(hedge_bond['maturity'], pc1_loadings)
            
            # ヘッジ比率 Q (Quantity) の計算
            # port_exposure + Q * h_loading = 0  =>  Q = - port_exposure / h_loading
            hedge_qty = 0.0
            if abs(h_loading) > 1e-4:
                raw_qty = - port_pc1_exposure / h_loading
                # ガード: 極端なヘッジ比率を制限（例: +/- 10倍まで）
                hedge_qty = np.clip(raw_qty, -10.0, 10.0)
            
            # 5. 翌日の損益計算
            next_df = self.df[self.df['trade_date'] == next_date]
            
            # (A) ヘッジなし PnL
            long_pnl = 0
            next_long = next_df[next_df['bond_name'] == long_candidate['bond_name']]
            if not next_long.empty:
                # Long: Yield低下で利益 => (Today - Next)
                long_pnl = (long_candidate['ave_compound_yield'] - next_long.iloc[0]['ave_compound_yield']) * 100
                
            short_pnl = 0
            next_short = next_df[next_df['bond_name'] == short_candidate['bond_name']]
            if not next_short.empty:
                # Short: Yield上昇で利益 => (Next - Today)
                short_pnl = (next_short.iloc[0]['ave_compound_yield'] - short_candidate['ave_compound_yield']) * 100
            
            no_hedge_total = long_pnl + short_pnl
            
            # (B) ヘッジ損益
            hedge_pnl = 0
            # 翌日のデータから「同じ銘柄名」を探す（再選定はしない）
            next_hedge = next_df[next_df['bond_name'] == hedge_bond_name]
            
            hedge_yield_change = 0
            if not next_hedge.empty:
                # Q > 0 ならLong、Q < 0 ならShortとして扱う
                # 共通式: Q * (Today_Yield - Next_Yield) * 100
                today_y = float(hedge_bond['ave_compound_yield'])
                next_y = float(next_hedge.iloc[0]['ave_compound_yield'])
                hedge_yield_change = today_y - next_y
                hedge_pnl = hedge_qty * hedge_yield_change * 100
            else:
                # 翌日にヘッジ銘柄が存在しない場合（償還など）
                # 今回のバックテストでは0とするが、実運用では代替銘柄での決済などが必要
                pass
            
            hedged_total = no_hedge_total + hedge_pnl
            
            # 異常値デバッグ
            if abs(hedged_total) > 20: # 20bp以上の変動は詳細ログ
                print(f"WARN: Large PnL on {current_date.date()}: {hedged_total:.2f}bp")
                print(f"  NoHedge: {no_hedge_total:.2f}bp, HedgePnL: {hedge_pnl:.2f}bp")
                print(f"  HedgeQty: {hedge_qty:.4f}, H_Loading: {h_loading:.4f}, Port_Expo: {port_pc1_exposure:.4f}")
                print(f"  HedgeBond: {hedge_bond_name} (Maturity: {hedge_bond['maturity']:.2f})")
                if not next_hedge.empty:
                    print(f"  Yield: {today_y:.3f}% -> {next_y:.3f}% (Diff: {hedge_yield_change*100:.1f}bp)")
            
            self.results.append({
                'date': current_date,
                # No Hedge Details
                'long_bond': long_candidate['bond_name'],
                'short_bond': short_candidate['bond_name'],
                'no_hedge_pnl': no_hedge_total,
                # Hedge Details
                'hedge_bond': hedge_bond_name,
                'hedge_qty': hedge_qty,
                'hedge_pnl': hedge_pnl,
                'hedged_total_pnl': hedged_total
            })
            
            if i % 20 == 0:
                print(f"Processed {current_date.date()}: NoHedge={no_hedge_total:.2f}bp, Hedged={hedged_total:.2f}bp")
        
        return pd.DataFrame(self.results)

if __name__ == "__main__":
    db_manager = DatabaseManager()
    df_all = fetch_all_data(db_manager, start_date='2024-01-01')
    
    if df_all.empty:
        print("No data loaded.")
        sys.exit(1)
        
    backtester = PCAStrategyBacktestHedged(df_all, window=50)
    results_df = backtester.run()
    
    if not results_df.empty:
        # 保存パスの設定 (安全策)
        try:
            output_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            if os.path.isdir('notebooks/pca'):
                output_dir = 'notebooks/pca'
            else:
                output_dir = '.'
        
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, 'backtest_results_hedged.csv')
        
        results_df.to_csv(output_path, index=False)
        print(f"\nResults saved to {output_path}")
        
        # 簡易集計
        results_df['cum_no_hedge'] = results_df['no_hedge_pnl'].cumsum()
        results_df['cum_hedged'] = results_df['hedged_total_pnl'].cumsum()
        
        print("\nPerformance Summary:")
        print(f"Total Return (No Hedge): {results_df['cum_no_hedge'].iloc[-1]:.2f} bp")
        print(f"Total Return (Hedged)  : {results_df['cum_hedged'].iloc[-1]:.2f} bp")
    else:
        print("No results generated.")
