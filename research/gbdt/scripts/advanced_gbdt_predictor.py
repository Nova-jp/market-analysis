
import os
import sys
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import lightgbm as lgb
import shap
from sklearn.model_selection import train_test_split
from scipy import stats
from sklearn.preprocessing import StandardScaler

# MICE (IterativeImputer) 
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.linear_model import BayesianRidge

# プロジェクトルートをパスに追加
sys.path.append(os.getcwd())

from core.db.sync_client import DatabaseManager

class AdvancedGBDTPredictor:
    """
    円金利スワップの全テナーをプーリングして学習する高度なGBDT予測モデル。
    分数階差 (d=0.4) と MICE (IterativeImputer) を用いた特徴量エンジニアリング、
    および MLE/IC/SHAP による検証を含む。
    """
    
    def __init__(self, target_product='OIS', d=0.4, window=252):
        self.db = DatabaseManager()
        self.logger = logging.getLogger(__name__)
        self.target_product = target_product
        self.d = d
        self.window = window

    # --- 補助関数 ---
    def _get_weights(self, d, window):
        """分数階差の重みを算出"""
        w = [1.0]
        for k in range(1, window + 1):
            w.append(-w[-1] * (d - k + 1) / k)
        return np.array(w)

    def _frac_diff_fixed_window(self, series, d, window):
        """
        固定ウィンドウ法による分数階差の計算 (欠損値がないことが前提)
        """
        weights = self._get_weights(d, window)
        res = []
        for i in range(len(series)):
            if i < window:
                res.append(np.nan)
                continue
            window_series = series[i-window:i+1][::-1]
            res.append(np.dot(weights, window_series))
        return pd.Series(res, index=series.index)

    def _tenor_to_float(self, tenor_str):
        if not tenor_str: return 0.0
        try:
            num = float(''.join(filter(str.isdigit, tenor_str)))
            if 'M' in tenor_str: return num / 12.0
            return num
        except:
            return 0.0

    # --- データ取得・補完 ---
    def _impute_data(self, df):
        """
        MICE (IterativeImputer) を用いて、スケールを考慮して補完を実行
        """
        self.logger.info("Starting simple MICE imputation on raw data...")
        
        # 数値列の抽出
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if numeric_cols.empty: return df
        
        # 1. 補完フラグを立てる (M{n}_is_imputed)
        for col in numeric_cols:
            df[f"{col}_is_imputed"] = df[col].isna().astype(int)
            
        # 2. スケーリング (BayesianRidgeはスケールに敏感なため)
        scaler = StandardScaler()
        df_scaled = pd.DataFrame(scaler.fit_transform(df[numeric_cols]), 
                                columns=numeric_cols, index=df.index)
        
        # 3. MICE 実行
        imputer = IterativeImputer(estimator=BayesianRidge(), max_iter=10, random_state=42)
        df_imputed_scaled = imputer.fit_transform(df_scaled)
        
        # 4. 逆スケーリングして戻す
        df[numeric_cols] = scaler.inverse_transform(df_imputed_scaled)
        
        return df

    def _fetch_all_data(self):
        """DBから生データを取得して結合し、補完する"""
        self.logger.info("Fetching raw swap tenors and macro data...")
        
        # 1. Swap OIS
        rows = self.db.get_ois_data(product_type=self.target_product)
        df_swap_raw = pd.DataFrame(rows)
        if df_swap_raw.empty: return None, None
        df_swap_raw['trade_date'] = pd.to_datetime(df_swap_raw['trade_date'])
        df_swap_ts = df_swap_raw.pivot(index='trade_date', columns='tenor', values='rate')
        
        # 2. Macro Data (USDJPY, DXY, Nikkei, UST10Y)
        fx_query = "SELECT trade_date, currency_pair, close_price FROM exchange_rates WHERE currency_pair IN ('USDJPY', 'DXY')"
        df_fx = pd.DataFrame(self.db.select_as_dict(fx_query))
        df_fx['trade_date'] = pd.to_datetime(df_fx['trade_date'])
        df_fx = df_fx.pivot(index='trade_date', columns='currency_pair', values='close_price')
        
        stock_query = "SELECT trade_date, close_price as nikkei FROM stock_prices WHERE ticker = '^N225'"
        df_stock = pd.DataFrame(self.db.select_as_dict(stock_query)).set_index(pd.to_datetime(pd.DataFrame(self.db.select_as_dict(stock_query))['trade_date']))[['nikkei']]
        
        ust_query = "SELECT trade_date, yield_value as ust10y FROM foreign_yields WHERE region = 'US' AND tenor = '10Y'"
        df_ust = pd.DataFrame(self.db.select_as_dict(ust_query)).set_index(pd.to_datetime(pd.DataFrame(self.db.select_as_dict(ust_query))['trade_date']))[['ust10y']]
        
        # 3. 結合 ( inner join 的に、いずれかのデータが存在する日のみを対象とする)
        # まずは全てのインデックスを結合
        df_combined = df_swap_ts.join([df_fx, df_stock, df_ust], how='outer')
        
        # 全てが NaN の行（もしあれば）を削除
        df_combined = df_combined.dropna(how='all')
        
        # 数値型へ
        for col in df_combined.columns:
            df_combined[col] = pd.to_numeric(df_combined[col], errors='coerce').astype(float)
            
        # 4. MICE 補完の実行 (生データに対して直接)
        df_combined = self._impute_data(df_combined)
        
        # 分離
        tenors = df_swap_ts.columns
        macro_cols = list(df_fx.columns) + ['nikkei', 'ust10y']
        
        df_swap_imputed = df_combined[list(tenors) + [f"{c}_is_imputed" for c in tenors]]
        df_macro_imputed = df_combined[macro_cols + [f"{c}_is_imputed" for c in macro_cols]]
        
        return df_swap_imputed, df_macro_imputed

    def _prepare_features_and_labels(self, df_swap_ts, df_macro, horizon):
        """特徴量生成 (FD計算等)"""
        self.logger.info(f"Preparing features for {horizon}-day horizon...")
        
        all_samples = []
        tenor_names = [c for c in df_swap_ts.columns if not c.endswith('_is_imputed')]
        macro_names = [c for c in df_macro.columns if not c.endswith('_is_imputed')]
        
        # マクロの FD (d=0.4)
        df_macro_fd = df_macro[macro_names].apply(lambda x: self._frac_diff_fixed_window(x, self.d, self.window))
        
        for tenor in tenor_names:
            s_raw = df_swap_ts[tenor]
            fd_raw = self._frac_diff_fixed_window(s_raw, self.d, self.window)
            diff3 = s_raw.diff(3)
            fd_diff3 = self._frac_diff_fixed_window(diff3, self.d, self.window)
            
            # ターゲット: 変化幅
            label = s_raw.shift(-horizon) - s_raw
            
            data_dict = {
                'tenor_val': self._tenor_to_float(tenor),
                'swap_fd_raw': fd_raw,
                'swap_fd_diff3': fd_diff3,
                'label': label,
                'swap_is_imputed': df_swap_ts[f"{tenor}_is_imputed"]
            }
            
            for m in macro_names:
                data_dict[f'{m}_fd'] = df_macro_fd[m]
                data_dict[f'{m}_is_imputed'] = df_macro[f"{m}_is_imputed"]
                
            df_tenor = pd.DataFrame(data_dict, index=s_raw.index)
            all_samples.append(df_tenor)
            
        df_all = pd.concat(all_samples).dropna(subset=['label', 'swap_fd_raw', 'swap_fd_diff3'])
        return df_all

    def train_and_validate(self, horizon):
        """学習と検証"""
        df_swap_ts, df_macro = self._fetch_all_data()
        if df_swap_ts is None: return
        
        df_all = self._prepare_features_and_labels(df_swap_ts, df_macro, horizon)
        
        features = [col for col in df_all.columns if col != 'label']
        df_all = df_all.sort_index()
        split_idx = int(len(df_all) * 0.8)
        
        X_train, y_train = df_all.iloc[:split_idx][features], df_all.iloc[:split_idx]['label']
        X_test, y_test = df_all.iloc[split_idx:][features], df_all.iloc[split_idx:]['label']
        
        model = lgb.LGBMRegressor(n_estimators=300, learning_rate=0.03, importance_type='gain', verbose=-1)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)])
        
        y_pred = model.predict(X_test)
        self.logger.info(f"\n--- Results ({horizon}-day) ---")
        self.logger.info(f"IC: {np.corrcoef(y_pred, y_test)[0, 1]:.4f}")
        
        # MLE (Residuals)
        res = y_test - y_pred
        mu, std = stats.norm.fit(res)
        self.logger.info(f"Log-Likelihood: {np.sum(stats.norm.logpdf(res, mu, std)):.2f}")
        
        # Importance
        imp = pd.DataFrame({'f': features, 'v': model.feature_importances_}).sort_values('v', ascending=False)
        self.logger.info("\nTop 5 Importance:\n" + str(imp.head(5)))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    predictor = AdvancedGBDTPredictor()
    for h in [3, 5]:
        predictor.train_and_validate(h)
