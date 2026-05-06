
import os
import sys
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

# プロジェクトルートをパスに追加
sys.path.append(os.getcwd())

from core.db.sync_client import DatabaseManager

class GBDTRatePredictor:
    """
    GBDT (LightGBM) を用いてスワップ金利の3日後・5日後の変化を予測するクラス。
    """
    
    def __init__(self, target_product='OIS', target_tenor='10Y'):
        self.db = DatabaseManager()
        self.logger = logging.getLogger(__name__)
        self.target_product = target_product
        self.target_tenor = target_tenor
        
    def _fetch_data(self):
        """DBから必要なデータを取得し、マージする"""
        self.logger.info("Fetching data from database...")
        
        # 1. Target Swap Data (OIS 10Y)
        rows = self.db.get_ois_data(product_type=self.target_product)
        df_swap_all = pd.DataFrame(rows)
        if df_swap_all.empty: return None
        
        # 指定のテナーのみ抽出
        df_swap = df_swap_all[df_swap_all['tenor'] == self.target_tenor].copy()
        df_swap = df_swap.rename(columns={'rate': 'target_rate'})
        df_swap['trade_date'] = pd.to_datetime(df_swap['trade_date'])
        df_swap = df_swap[['trade_date', 'target_rate']].sort_values('trade_date')
        
        # 2. USD/JPY and DXY
        fx_query = """
            SELECT trade_date, currency_pair, close_price 
            FROM exchange_rates 
            WHERE currency_pair IN ('USDJPY', 'DXY')
        """
        df_fx = pd.DataFrame(self.db.select_as_dict(fx_query))
        df_fx['trade_date'] = pd.to_datetime(df_fx['trade_date'])
        df_fx = df_fx.pivot(index='trade_date', columns='currency_pair', values='close_price').reset_index()
        
        # 3. Nikkei 225
        stock_query = """
            SELECT trade_date, close_price as nikkei_close 
            FROM stock_prices 
            WHERE ticker = '^N225'
        """
        df_stock = pd.DataFrame(self.db.select_as_dict(stock_query))
        df_stock['trade_date'] = pd.to_datetime(df_stock['trade_date'])
        
        # 4. US Treasury 10Y
        ust_query = """
            SELECT trade_date, yield_value as ust10y 
            FROM foreign_yields 
            WHERE region = 'US' AND tenor = '10Y' AND instrument_type = 'sovereign'
        """
        df_ust = pd.DataFrame(self.db.select_as_dict(ust_query))
        df_ust['trade_date'] = pd.to_datetime(df_ust['trade_date'])
        
        # Merge all data
        df = df_swap.merge(df_fx, on='trade_date', how='left')
        df = df.merge(df_stock, on='trade_date', how='left')
        df = df.merge(df_ust, on='trade_date', how='left')
        
        # Convert numeric columns to float (they might be Decimal from DB)
        cols_to_convert = ['target_rate', 'USDJPY', 'DXY', 'nikkei_close', 'ust10y']
        for col in cols_to_convert:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        
        # Sort and handle missing values
        df = df.sort_values('trade_date').ffill()
        return df

    def _prepare_features(self, df):
        """特徴量エンジニアリング"""
        self.logger.info("Preparing features...")
        
        # 前日比（変化幅）の計算
        features = []
        for col in ['target_rate', 'USDJPY', 'DXY', 'nikkei_close', 'ust10y']:
            diff_col = f'{col}_diff'
            df[diff_col] = df[col].diff()
            features.append(diff_col)
            
            # ラグ特徴量 (1-3日前)
            for i in range(1, 4):
                lag_col = f'{diff_col}_lag{i}'
                df[lag_col] = df[diff_col].shift(i)
                features.append(lag_col)
                
        # 移動平均
        df['target_rate_ma5'] = df['target_rate'].rolling(window=5).mean()
        df['target_rate_ma20'] = df['target_rate'].rolling(window=20).mean()
        features.extend(['target_rate_ma5', 'target_rate_ma20'])
        
        return df, features

    def train_and_predict(self):
        """学習と最新の予測"""
        df = self._fetch_data()
        df, features = self._prepare_features(df)
        
        horizons = [3, 5]
        predictions = []
        
        for h in horizons:
            self.logger.info(f"Training for {h}-day horizon...")
            
            # 正解ラベル: h日後の金利
            df[f'label_{h}d'] = df['target_rate'].shift(-h)
            
            # 学習データ準備 (最新h日分はラベルがないので予測用)
            train_df = df.dropna(subset=[f'label_{h}d'] + features)
            latest_row = df.dropna(subset=features).iloc[-1:]
            
            X = train_df[features]
            y = train_df[f'label_{h}d']
            
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
            
            # LightGBM Model
            model = lgb.LGBMRegressor(n_estimators=100, learning_rate=0.05, importance_type='gain', verbose=-1)
            model.fit(X_train, y_train, eval_set=[(X_test, y_test)])
            
            # 最新の予測
            pred_val = model.predict(latest_row[features])[0]
            
            # 予測情報の構築
            pred_date = latest_row['trade_date'].iloc[0]
            target_date = pred_date + timedelta(days=h) # 簡易的に営業日を考慮せずカレンダー日
            
            predictions.append({
                "prediction_date": pred_date.date(),
                "target_date": target_date.date(),
                "product_type": self.target_product,
                "tenor": self.target_tenor,
                "horizon_days": h,
                "predicted_rate": float(pred_val),
                "model_name": f"LightGBM_{h}d",
                "feature_importance": model.feature_importances_.tolist()
            })
            
        # 予測結果をDBに保存
        if predictions:
            self.logger.info(f"Saving {len(predictions)} predictions to database...")
            self.db.batch_insert_data(
                data_list=predictions,
                table_name="rate_predictions",
                conflict_target="prediction_date, product_type, tenor, horizon_days",
                update_columns=["target_date", "predicted_rate", "updated_at"]
            )
            
        return predictions

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    predictor = GBDTRatePredictor()
    results = predictor.train_and_predict()
    for res in results:
        print(f"Prediction for {res['target_date']} ({res['horizon_days']}d): {res['predicted_rate']:.4f}%")
