"""
PCA Analysis Service
主成分分析サービス層 - Jupyter Notebookのロジックを関数化
"""
import numpy as np
import pandas as pd
import pickle
import os
import shutil
from datetime import date, datetime, timedelta
from scipy.interpolate import CubicSpline
from sklearn.decomposition import PCA
from typing import Dict, List, Tuple, Optional

from app.core.config import settings

class PCAService:
    """主成分分析サービスクラス"""

    def __init__(self):
        from data.utils.database_manager import DatabaseManager
        self.MIN_MATURITY = 0.5  # 最小残存期間（年）
        self.db_manager = DatabaseManager()
        self.cache_dir = ".cache/pca"

    def _get_cache_path(self, end_date: str, lookback_days: int) -> str:
        """キャッシュファイルのパスを生成"""
        # end_dateがNoneの場合は今日の日付を使用（ファイル名用）
        d_str = end_date if end_date else date.today().strftime('%Y-%m-%d')
        return os.path.join(self.cache_dir, f"pca_cache_{d_str}_{lookback_days}.pkl")

    def save_cache(self, end_date: str, lookback_days: int, data: Dict):
        """分析結果をキャッシュに保存"""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        
        path = self._get_cache_path(end_date, lookback_days)
        try:
            with open(path, 'wb') as f:
                pickle.dump(data, f)
        except Exception as e:
            print(f"Cache save error: {e}")

    def load_cache(self, end_date: str, lookback_days: int) -> Optional[Dict]:
        """キャッシュから分析結果を取得"""
        path = self._get_cache_path(end_date, lookback_days)
        if os.path.exists(path):
            try:
                with open(path, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                print(f"Cache load error: {e}")
        return None

    def clear_cache(self):
        """キャッシュディレクトリ内のすべてのファイルを削除"""
        if os.path.exists(self.cache_dir):
            for filename in os.listdir(self.cache_dir):
                file_path = os.path.join(self.cache_dir, filename)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                except Exception as e:
                    print(f"Error deleting cache file {file_path}: {e}")
            print("PCA cache cleared.")

    def normalize_bond_code(self, bond_code) -> str:
        """bond_codeを9桁に正規化（0パディング）"""
        if bond_code is None or bond_code == 'N/A':
            return 'N/A'
        code_str = str(bond_code).strip()
        try:
            code_int = int(code_str)
            return f'{code_int:09d}'
        except ValueError:
            return code_str

    def get_analysis_dates(self, limit: int = 200, end_date: Optional[str] = None) -> List[str]:
        """
        分析対象の日付を取得（基準日から過去へ）
        """
        params = []
        sql_parts = ["SELECT DISTINCT trade_date FROM bond_data"]
        
        if end_date and end_date.strip():
            # DATE型へのキャストを明示
            sql_parts.append("WHERE trade_date <= %s::date")
            params.append(end_date)
            
        sql_parts.append("ORDER BY trade_date DESC LIMIT %s")
        params.append(limit)
        
        query = " ".join(sql_parts)
        
        try:
            rows = self.db_manager.execute_query(query, tuple(params))
            if not rows:
                print(f"DEBUG: No dates found for query: {query} with params: {params}")
            return [str(row[0]) for row in rows]
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"日付取得エラー: {e}")
            raise Exception(f"Database error while fetching dates: {str(e)}")

    def get_yield_data_bulk(self, dates: List[str]) -> pd.DataFrame:
        """指定された複数の日付のデータを一括取得"""
        if not dates:
            return pd.DataFrame()
            
        # プレースホルダを作成
        placeholders = ', '.join(['%s'] * len(dates))
        
        query = f"""
            SELECT trade_date, due_date, ave_compound_yield, bond_code, bond_name
            FROM bond_data
            WHERE trade_date IN ({placeholders})
              AND ave_compound_yield IS NOT NULL
              AND due_date IS NOT NULL
        """
        
        try:
            # タプルとしてパラメータを渡す
            rows = self.db_manager.select_as_dict(query, tuple(dates))
            if not rows:
                return pd.DataFrame()
            
            # DataFrame作成
            df = pd.DataFrame(rows)
            
            # 日付型変換
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            df['due_date'] = pd.to_datetime(df['due_date']).dt.date
            
            # maturity計算
            # dateオブジェクト同士の減算はtimedeltaを返す -> daysプロパティで日数取得
            df['maturity'] = df.apply(lambda x: (x['due_date'] - x['trade_date']).days / 365.25, axis=1)
            
            # 最小残存期間フィルタ
            df = df[df['maturity'] >= self.MIN_MATURITY].copy()
            
            # データ型変換
            df['yield'] = df['ave_compound_yield'].astype(float)
            df['bond_code'] = df['bond_code'].apply(self.normalize_bond_code)
            df['bond_name'] = df['bond_name'].astype(str)
            df['maturity'] = df['maturity'].round(4)
            
            # trade_dateを文字列に戻す（後の処理のため）
            df['trade_date_str'] = df['trade_date'].astype(str)
            
            return df[['trade_date_str', 'maturity', 'yield', 'bond_code', 'bond_name']]
            
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"一括データ取得エラー: {e}")
            return pd.DataFrame()

    def get_yield_data_for_date(self, date: str, with_bond_code: bool = False) -> pd.DataFrame:
        """指定日のイールドカーブデータを取得 (互換性維持)"""
        df = self.get_yield_data_bulk([date])
        if df.empty:
            return pd.DataFrame()
            
        if not with_bond_code:
            return df[['maturity', 'yield']]
        return df[['maturity', 'yield', 'bond_code', 'bond_name']]

    def interpolate_yield_curves(
        self,
        daily_data: Dict[str, pd.DataFrame]
    ) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """
        各日のイールドカーブをスプライン補間

        Returns:
            X: 補間後のデータ行列 (日数 x 残存期間)
            common_grid: 共通残存期間軸
            valid_dates: 有効な日付リスト
        """
        if not daily_data:
            return np.array([]), np.array([]), []

        # 残存期間の和集合を作成
        all_maturities = set()
        for df in daily_data.values():
            all_maturities.update(df['maturity'].values)

        if not all_maturities:
            return np.array([]), np.array([]), []

        common_grid = np.sort(np.array(list(all_maturities)))

        # 3次スプライン補間
        interpolated_data = []
        valid_dates = []

        for date, df in daily_data.items():
            if len(df) < 2:
                continue

            # ソート＆重複除去
            df_sorted = df.sort_values('maturity').drop_duplicates('maturity')

            # 3次スプライン補間
            cs = CubicSpline(
                df_sorted['maturity'].values,
                df_sorted['yield'].values,
                extrapolate=False
            )

            interpolated = cs(common_grid)

            # NaNが50%未満の行のみ採用
            if np.isnan(interpolated).sum() / len(interpolated) < 0.5:
                interpolated_data.append(interpolated)
                valid_dates.append(date)

        if not interpolated_data:
            return np.array([]), np.array([]), []

        X = np.array(interpolated_data)

        return X, common_grid, valid_dates

    def perform_pca(
        self,
        X: np.ndarray,
        n_components: int = 3
    ) -> Tuple[PCA, np.ndarray]:
        """
        PCA実行

        Args:
            X: データ行列 (日数 x 残存期間)
            n_components: 主成分の数

        Returns:
            pca: PCAモデル
            X_pca: 主成分スコア
        """
        if X.size == 0 or len(X.shape) < 2:
            raise ValueError("PCA input data X is empty or invalid")

        # NaNを列平均で補完
        X_filled = X.copy()
        col_means = np.nanmean(X, axis=0)
        for i in range(X.shape[1]):
            mask = np.isnan(X_filled[:, i])
            X_filled[mask, i] = col_means[i]

        # PCA実行
        pca = PCA(n_components=n_components)
        X_pca = pca.fit_transform(X_filled)

        return pca, X_pca

    def reconstruct_date(
        self,
        date_str: str,
        date_index: int,
        pca_model: PCA,
        X_pca: np.ndarray,
        common_grid: np.ndarray,
        actual_data: pd.DataFrame = None 
    ) -> pd.DataFrame:
        """
        指定日のデータを復元

        Args:
            actual_data: その日の実データ (DataFrame)
        """
        if actual_data is None:
            actual_data = self.get_yield_data_for_date(date_str, with_bond_code=True)
            
        # 残存年限の昇順でソート
        actual_data = actual_data.sort_values('maturity', ascending=True).reset_index(drop=True)

        # 主成分スコアを取得
        pc_scores = X_pca[date_index, :]

        # 完全な復元データ
        mean_vec = pca_model.mean_
        reconstructed_full = mean_vec + np.dot(pc_scores, pca_model.components_)

        # 実データの各残存期間に対して復元値を計算
        reconstruction_results = []

        for idx, row in actual_data.iterrows():
            maturity = row['maturity']
            original_yield = row['yield']
            bond_code = row.get('bond_code', '')
            bond_name = row.get('bond_name', '')

            # common_gridで最も近いインデックスを探す
            grid_idx = np.argmin(np.abs(common_grid - maturity))
            reconstructed_yield = reconstructed_full[grid_idx]

            error = original_yield - reconstructed_yield

            reconstruction_results.append({
                'maturity': maturity,
                'bond_code': bond_code,
                'bond_name': bond_name,
                'original_yield': original_yield,
                'reconstructed_yield': reconstructed_yield,
                'error': error
            })

        df = pd.DataFrame(reconstruction_results)
        
        if not df.empty:
             df['bond_code'] = df['bond_code'].astype(str)
             df['bond_name'] = df['bond_name'].astype(str)

        return df

    def calculate_error_statistics(self, reconstruction_df: pd.DataFrame) -> Dict:
        """復元誤差の統計量を計算"""
        if reconstruction_df.empty:
             return {}
             
        errors = reconstruction_df['error'].values
        abs_errors = np.abs(errors)

        return {
            'mae': float(abs_errors.mean()),  # 平均絶対誤差
            'mse': float((errors ** 2).mean()),  # 平均二乗誤差
            'rmse': float(np.sqrt((errors ** 2).mean())),  # 二乗平均平方根誤差
            'max_error': float(abs_errors.max()),  # 最大誤差
            'std': float(errors.std()),  # 標準偏差
            'min': float(errors.min()),
            'max': float(errors.max())
        }

    def run_pca_analysis(
        self,
        lookback_days: int = 100,
        n_components: int = 3,
        end_date: Optional[str] = None
    ) -> Dict:
        """
        PCA分析を実行（キャッシュ対応）
        """
        import logging
        logger = logging.getLogger(__name__)

        # end_dateが空文字の場合はNoneにする
        if end_date and not end_date.strip():
            end_date = None

        # 0. キャッシュチェック
        available_dates = self.get_analysis_dates(limit=1, end_date=end_date)
        if not available_dates:
             return {"error": "No dates available"}
        actual_end_date = available_dates[0]
        
        cache_data = self.load_cache(actual_end_date, lookback_days)
        if cache_data and cache_data['pca'].n_components == n_components:
            logger.info(f"Using cached PCA model for {actual_end_date}")
            pca = cache_data['pca']
            X_pca = cache_data['X_pca']
            common_grid = cache_data['common_grid']
            valid_dates = cache_data['valid_dates']
            daily_data = cache_data['daily_data']
        else:
            # 1. データ取得（全期間）
            all_dates = self.get_analysis_dates(limit=lookback_days + 50, end_date=actual_end_date)
            target_dates = all_dates[:lookback_days]
            
            bulk_df = self.get_yield_data_bulk(target_dates)
            daily_data = {}
            for d in target_dates:
                day_df = bulk_df[bulk_df['trade_date_str'] == d].copy()
                if not day_df.empty:
                    daily_data[d] = day_df

            # 2. スプライン補間
            X, common_grid, valid_dates = self.interpolate_yield_curves(daily_data)
            if len(valid_dates) < 2:
                 return {"error": "Not enough valid data"}

            # 3. PCA実行
            pca, X_pca = self.perform_pca(X, n_components)
            
            # キャッシュ保存
            self.save_cache(actual_end_date, lookback_days, {
                'pca': pca,
                'X_pca': X_pca,
                'common_grid': common_grid,
                'valid_dates': valid_dates,
                'daily_data': daily_data
            })

        # 4. 最新日の復元データのみ計算して返す（初期表示用）
        latest_date = valid_dates[0]
        latest_rec_df = self.reconstruct_date(
            latest_date, 0, pca, X_pca, common_grid, actual_data=daily_data[latest_date]
        )
        
        result = {
            'parameters': {
                'lookback_days': lookback_days,
                'n_components': n_components,
                'actual_end_date': actual_end_date,
                'valid_dates_count': len(valid_dates),
                'date_range': {'start': valid_dates[-1], 'end': valid_dates[0]}
            },
            'pca_model': {
                'explained_variance_ratio': pca.explained_variance_ratio_.tolist(),
                'cumulative_variance_ratio': float(np.cumsum(pca.explained_variance_ratio_)[-1]),
                'components': pca.components_.tolist(),
                'mean': pca.mean_.tolist()
            },
            'principal_component_scores': {
                'dates': valid_dates[:30],
                'scores': X_pca[:30, :].tolist()
            },
            'reconstruction_dates': valid_dates, # 日付リストだけ返す
            'latest_reconstruction': {
                'date': latest_date,
                'data': latest_rec_df.to_dict(orient='records'),
                'statistics': self.calculate_error_statistics(latest_rec_df)
            },
            'common_grid': common_grid.tolist()
        }
        return result

    def get_reconstruction_for_date(
        self,
        target_date: str,
        end_date: str,
        lookback_days: int,
        n_components: int
    ) -> Dict:
        """
        キャッシュされたモデルを使用して、特定の日付の復元誤差を計算
        """
        cache_data = self.load_cache(end_date, lookback_days)
        if not cache_data:
            return {"error": "Cache not found. Please run analysis first."}
            
        valid_dates = cache_data['valid_dates']
        if target_date not in valid_dates:
            return {"error": f"Date {target_date} not in analysis period."}
            
        date_idx = valid_dates.index(target_date)
        pca = cache_data['pca']
        X_pca = cache_data['X_pca']
        common_grid = cache_data['common_grid']
        daily_data = cache_data['daily_data']
        
        rec_df = self.reconstruct_date(
            target_date, date_idx, pca, X_pca, common_grid, actual_data=daily_data[target_date]
        )
        
        return {
            'date': target_date,
            'data': rec_df.to_dict(orient='records'),
            'statistics': self.calculate_error_statistics(rec_df)
        }


    def get_swap_data(self, product_type: str = 'OIS', limit_days: int = 300) -> pd.DataFrame:
        """
        スワップ金利データを取得し、ピボットテーブル形式で返す
        """
        # 最新の日付を取得
        dates_query = """
            SELECT DISTINCT trade_date 
            FROM irs_data 
            WHERE product_type = %s
            ORDER BY trade_date DESC 
            LIMIT %s
        """
        try:
            date_rows = self.db_manager.execute_query(dates_query, (product_type, limit_days))
            if not date_rows:
                return pd.DataFrame()
            
            target_dates = [row[0] for row in date_rows]
            min_date = target_dates[-1]
            max_date = target_dates[0]
            
            # データ取得
            query = """
                SELECT trade_date, tenor, rate
                FROM irs_data
                WHERE product_type = %s
                  AND trade_date >= %s
                  AND trade_date <= %s
                  AND tenor LIKE '%%Y'
                ORDER BY trade_date DESC
            """
            
            rows = self.db_manager.select_as_dict(query, (product_type, min_date, max_date))
            if not rows:
                return pd.DataFrame()
            
            df = pd.DataFrame(rows)
            df['rate'] = df['rate'].astype(float)
            df['maturity'] = df['tenor'].str.replace('Y', '').astype(float)
            pivot_df = df.pivot(index='trade_date', columns='maturity', values='rate')
            pivot_df = pivot_df.sort_index(ascending=False)
            pivot_df = pivot_df.dropna()
            
            return pivot_df
            
        except Exception as e:
            print(f"スワップデータ取得エラー: {e}")
            return pd.DataFrame()

    def run_swap_pca_analysis(
        self,
        lookback_days: int = 100,
        n_components: int = 3,
        product_type: str = 'OIS'
    ) -> Dict:
        """
        スワップ金利の主成分分析を実行
        """
        import logging
        logger = logging.getLogger(__name__)

        # 1. データ取得
        fetch_days = int(lookback_days * 1.5)
        df_pivot = self.get_swap_data(product_type, limit_days=fetch_days)
        
        if df_pivot.empty:
             return {"error": "No data available"}

        df_target = df_pivot.head(lookback_days)
        
        if len(df_target) < 10:
             return {"error": f"Insufficient data: {len(df_target)} days available"}

        valid_dates = [d.strftime('%Y-%m-%d') for d in df_target.index]
        X = df_target.values
        common_grid = df_target.columns.values

        logger.info(f"Swap PCA対象データ: {X.shape}, 期間: {valid_dates[-1]} ~ {valid_dates[0]}")

        # 2. PCA実行
        pca, X_pca = self.perform_pca(X, n_components)

        # 3. 復元と誤差計算
        mean_vec = pca.mean_
        reconstructed_matrix = mean_vec + np.dot(X_pca, pca.components_)
        
        all_reconstructions = {}
        
        for i, date_str in enumerate(valid_dates):
            original_row = X[i, :]
            reconstructed_row = reconstructed_matrix[i, :]
            errors = original_row - reconstructed_row
            
            data_list = []
            for j, maturity in enumerate(common_grid):
                data_list.append({
                    'maturity': float(maturity),
                    'original_yield': float(original_row[j]),
                    'reconstructed_yield': float(reconstructed_row[j]),
                    'error': float(errors[j]),
                    'bond_code': f"SWAP_{int(maturity)}Y",
                    'bond_name': f"{product_type} {int(maturity)}Y"
                })
            
            abs_errors = np.abs(errors)
            stats = {
                'mae': float(abs_errors.mean()),
                'mse': float((errors ** 2).mean()),
                'rmse': float(np.sqrt((errors ** 2).mean())),
                'max_error': float(abs_errors.max()),
                'std': float(errors.std()),
                'min': float(errors.min()),
                'max': float(errors.max())
            }
            
            all_reconstructions[date_str] = {
                'data': data_list,
                'statistics': stats
            }

        latest_date = valid_dates[0]
        
        result = {
            'parameters': {
                'lookback_days': len(valid_dates),
                'n_components': n_components,
                'valid_dates_count': len(valid_dates),
                'date_range': {
                    'start': valid_dates[-1],
                    'end': valid_dates[0]
                },
                'product_type': product_type
            },
            'pca_model': {
                'explained_variance_ratio': pca.explained_variance_ratio_.tolist(),
                'cumulative_variance_ratio': float(np.cumsum(pca.explained_variance_ratio_)[-1]),
                'components': pca.components_.tolist(),
                'mean': pca.mean_.tolist()
            },
            'principal_component_scores': {
                'dates': valid_dates[:30],
                'scores': X_pca[:30, :].tolist()
            },
            'reconstruction': {
                'dates': valid_dates,
                'all_dates': all_reconstructions,
                'latest_date': latest_date,
                'data': all_reconstructions[latest_date]['data'],
                'statistics': all_reconstructions[latest_date]['statistics']
            },
            'common_grid': common_grid.tolist()
        }

        return result