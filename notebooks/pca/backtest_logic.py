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

class PCAStrategyBacktest:
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
        # Remove duplicates
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

    def run(self):
        print(f"Starting backtest over {len(self.dates)} days with window={self.window}")
        
        for i in range(self.window, len(self.dates) - 1):
            current_date = self.dates[i]
            next_date = self.dates[i+1]
            
            # Train window
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
            
            # PCA
            pca = PCA(n_components=2)
            try:
                pca.fit(X_train)
            except Exception as e:
                print(f"PCA fit failed on {current_date}: {e}")
                continue
            
            # Current Day Evaluation
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
            
            # Signal
            long_candidate = current_df.loc[current_df['error'].idxmax()]
            short_candidate = current_df.loc[current_df['error'].idxmin()]
            
            # Next Day P&L
            next_df = self.df[self.df['trade_date'] == next_date]
            
            long_pnl = 0
            if long_candidate['error'] > 0:
                next_bond = next_df[next_df['bond_name'] == long_candidate['bond_name']]
                if not next_bond.empty:
                    next_yield = next_bond.iloc[0]['ave_compound_yield']
                    long_pnl = (long_candidate['ave_compound_yield'] - next_yield) * 100
            
            short_pnl = 0
            if short_candidate['error'] < 0:
                next_bond = next_df[next_df['bond_name'] == short_candidate['bond_name']]
                if not next_bond.empty:
                    next_yield = next_bond.iloc[0]['ave_compound_yield']
                    short_pnl = (next_yield - short_candidate['ave_compound_yield']) * 100
            
            self.results.append({
                'date': current_date,
                'long_bond': long_candidate['bond_name'],
                'long_error': long_candidate['error'] * 100,
                'long_pnl': long_pnl,
                'short_bond': short_candidate['bond_name'],
                'short_error': short_candidate['error'] * 100,
                'short_pnl': short_pnl,
                'total_pnl': long_pnl + short_pnl
            })
            
            if i % 20 == 0:
                print(f"Processed {current_date.date()}: Total PnL = {long_pnl + short_pnl:.2f} bp")
        
        return pd.DataFrame(self.results)

if __name__ == "__main__":
    db_manager = DatabaseManager()
    # Fetch data from start of 2024 to save time for this check, or earlier
    df_all = fetch_all_data(db_manager, start_date='2024-01-01')
    
    if df_all.empty:
        print("No data loaded.")
        sys.exit(1)
        
    backtester = PCAStrategyBacktest(df_all, window=50)
    results_df = backtester.run()
    
    if not results_df.empty:
        results_df['cumulative_pnl'] = results_df['total_pnl'].cumsum()
        print("\nPerformance Summary:")
        print(f"Total Return: {results_df['cumulative_pnl'].iloc[-1]:.2f} bp")
        print(f"Average Daily P&L: {results_df['total_pnl'].mean():.2f} bp")
        print(f"Win Rate: {(results_df['total_pnl'] > 0).mean():.2%}")
        
        # Save to CSV for inspection
        try:
            output_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            # Fallback for Jupyter Notebook or interactive mode
            if os.path.isdir('notebooks/pca'):
                output_dir = 'notebooks/pca'
            else:
                output_dir = '.'
        
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, 'backtest_results_v1.csv')
        results_df.to_csv(output_path, index=False)
        print(f"Results saved to {output_path}")
    else:
        print("No results generated.")
