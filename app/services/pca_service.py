"""
PCA Analysis Service
主成分分析サービス層 - Jupyter Notebookのロジックを関数化
"""
import numpy as np
import pandas as pd
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

    def get_latest_dates(self, limit: int = 200) -> List[str]:
        """
        最新の営業日を取得
        """
        query = """
            SELECT DISTINCT trade_date 
            FROM bond_data 
            ORDER BY trade_date DESC 
            LIMIT %s
        """
        try:
            rows = self.db_manager.execute_query(query, (limit,))
            return [str(row[0]) for row in rows]
        except Exception as e:
            print(f"日付取得エラー: {e}")
            return []

    def get_yield_data_for_date(self, date: str, with_bond_code: bool = False) -> pd.DataFrame:
        """指定日のイールドカーブデータを取得"""
        
        # 必要なカラムを選択
        columns = ['trade_date', 'due_date', 'ave_compound_yield']
        if with_bond_code:
            columns.extend(['bond_code', 'bond_name'])
            
        # SQLクエリ構築
        select_clause = ", ".join(columns)
        query = f"""
            SELECT {select_clause}
            FROM bond_data
            WHERE trade_date = %s
              AND ave_compound_yield IS NOT NULL
              AND due_date IS NOT NULL
        """
        
        try:
            # select_as_dictを使用して辞書のリストを取得
            rows = self.db_manager.select_as_dict(query, (date,))
            if not rows:
                return pd.DataFrame()
            
            # データ処理ループ
            data_list = []
            for row in rows:
                try:
                    # 辞書キーでアクセス
                    trade_date_val = row['trade_date']
                    due_date_val = row['due_date']
                    yield_val = row['ave_compound_yield']
                    
                    # 日付オブジェクトに確実に変換
                    if isinstance(trade_date_val, str):
                        trade_dt = datetime.strptime(trade_date_val, '%Y-%m-%d').date()
                    elif isinstance(trade_date_val, datetime):
                        trade_dt = trade_date_val.date()
                    else:
                        trade_dt = trade_date_val # date object
                        
                    if isinstance(due_date_val, str):
                        due_dt = datetime.strptime(due_date_val, '%Y-%m-%d').date()
                    elif isinstance(due_date_val, datetime):
                        due_dt = due_date_val.date()
                    else:
                        due_dt = due_date_val # date object
                    
                    # maturity計算 (dateオブジェクト同士の減算はtimedeltaを返す)
                    maturity_years = (due_dt - trade_dt).days / 365.25

                    if maturity_years >= self.MIN_MATURITY:
                        data_dict = {
                            'maturity': round(maturity_years, 4),
                            'yield': float(yield_val)
                        }
                        if with_bond_code:
                            data_dict['bond_code'] = self.normalize_bond_code(row['bond_code'])
                            data_dict['bond_name'] = str(row['bond_name']) if row['bond_name'] else 'N/A'
                        data_list.append(data_dict)
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).error(f"Row processing error: {e}")
                    continue

            df = pd.DataFrame(data_list)
            if with_bond_code and not df.empty:
                df['bond_code'] = df['bond_code'].astype(str)
                df['bond_name'] = df['bond_name'].astype(str)

            return df
            
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"イールドデータ取得エラー ({date}): {e}")
            return pd.DataFrame()

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
        common_grid: np.ndarray
    ) -> pd.DataFrame:
        """
        指定日のデータを復元

        Returns:
            DataFrame with columns: maturity, bond_code, bond_name, original_yield, reconstructed_yield, error
        """
        # 実データを取得
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
            bond_code = row['bond_code']
            bond_name = row['bond_name']

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
        n_components: int = 3
    ) -> Dict:
        """
        完全なPCA分析を実行

        Args:
            lookback_days: PCA学習に使用する営業日数
            n_components: 主成分の数

        Returns:
            分析結果の辞書
        """
        import logging
        logger = logging.getLogger(__name__)

        # 1. 日付取得
        available_dates = self.get_latest_dates(limit=200)
        target_dates = available_dates[:lookback_days]
        logger.info(f"取得した日付数: {len(target_dates)}")

        # 2. イールドカーブデータ取得
        daily_data = {}
        for date in target_dates:
            df = self.get_yield_data_for_date(date, with_bond_code=False)
            if not df.empty:
                daily_data[date] = df

        logger.info(f"有効な日数: {len(daily_data)} / {len(target_dates)}")

        # 3. スプライン補間
        X, common_grid, valid_dates = self.interpolate_yield_curves(daily_data)
        logger.info(f"補間後の有効日数: {len(valid_dates)}, データ形状: {X.shape}")

        # 4. PCA実行
        pca, X_pca = self.perform_pca(X, n_components)

        # 5. 全日付の復元データを計算
        all_reconstructions = {}
        for idx, date in enumerate(valid_dates):
            reconstruction_df = self.reconstruct_date(
                date, idx, pca, X_pca, common_grid
            )
            error_stats = self.calculate_error_statistics(reconstruction_df)

            all_reconstructions[date] = {
                'data': reconstruction_df.to_dict(orient='records'),
                'statistics': error_stats
            }

        # 6. 結果を整形
        latest_date = valid_dates[0]
        result = {
            'parameters': {
                'lookback_days': lookback_days,
                'n_components': n_components,
                'valid_dates_count': len(valid_dates),
                'date_range': {
                    'start': valid_dates[-1],
                    'end': valid_dates[0]
                }
            },
            'pca_model': {
                'explained_variance_ratio': pca.explained_variance_ratio_.tolist(),
                'cumulative_variance_ratio': float(np.cumsum(pca.explained_variance_ratio_)[-1]),
                'components': pca.components_.tolist(),
                'mean': pca.mean_.tolist()
            },
            'principal_component_scores': {
                'dates': valid_dates[:30],  # 直近30日分
                'scores': X_pca[:30, :].tolist()
            },
            'reconstruction': {
                'dates': valid_dates,  # 全日付リスト
                'all_dates': all_reconstructions,  # 全日付の復元データ
                'latest_date': latest_date,
                'data': all_reconstructions[latest_date]['data'],  # 最新日のデータ（後方互換性）
                'statistics': all_reconstructions[latest_date]['statistics']  # 最新日の統計（後方互換性）
            },
            'common_grid': common_grid.tolist()
        }

        return result

    def get_swap_data(self, product_type: str = 'OIS', limit_days: int = 300) -> pd.DataFrame:
        """
        スワップ金利データを取得し、ピボットテーブル形式で返す
        
        Args:
            product_type: プロダクトタイプ (OIS, 6M_TIBOR etc.)
            limit_days: 取得する最大日数
            
        Returns:
            pd.DataFrame: index=date, columns=maturity(float), values=rate
        """
        # 最新の日付を取得
        dates_query = """
            SELECT DISTINCT trade_date 
            FROM irs_settlement_rates 
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
                FROM irs_settlement_rates
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
            
            # rateをfloatに変換 (DecimalだとPCAでエラーになるため)
            df['rate'] = df['rate'].astype(float)
            
            # tenorを数値に変換 ('10Y' -> 10.0)
            df['maturity'] = df['tenor'].str.replace('Y', '').astype(float)
            
            # ピボット (行: 日付, 列: 残存期間, 値: 金利)
            pivot_df = df.pivot(index='trade_date', columns='maturity', values='rate')
            
            # 日付で降順ソート
            pivot_df = pivot_df.sort_index(ascending=False)
            
            # 欠損がある行を削除（PCAには完全なデータが必要）
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

        Args:
            lookback_days: PCA学習に使用する営業日数
            n_components: 主成分の数
            product_type: 'OIS' など

        Returns:
            分析結果の辞書
        """
        import logging
        logger = logging.getLogger(__name__)

        # 1. データ取得
        # 少し多めに取得して、欠損除去後にlookback_days分確保できるようにする
        fetch_days = int(lookback_days * 1.5)
        df_pivot = self.get_swap_data(product_type, limit_days=fetch_days)
        
        if df_pivot.empty:
             return {"error": "No data available"}

        # 指定日数分に切り詰め
        df_target = df_pivot.head(lookback_days)
        
        if len(df_target) < 10: # データが少なすぎる場合
             return {"error": f"Insufficient data: {len(df_target)} days available"}

        valid_dates = [d.strftime('%Y-%m-%d') for d in df_target.index]
        X = df_target.values
        common_grid = df_target.columns.values # float array of maturities

        logger.info(f"Swap PCA対象データ: {X.shape}, 期間: {valid_dates[-1]} ~ {valid_dates[0]}")

        # 2. PCA実行
        pca, X_pca = self.perform_pca(X, n_components)

        # 3. 復元と誤差計算
        # Swapの場合は補間がないので、直接PCA結果から復元して比較
        mean_vec = pca.mean_
        reconstructed_matrix = mean_vec + np.dot(X_pca, pca.components_)
        
        # 全日付の結果を構築
        all_reconstructions = {}
        
        for i, date_str in enumerate(valid_dates):
            original_row = X[i, :]
            reconstructed_row = reconstructed_matrix[i, :]
            errors = original_row - reconstructed_row
            
            # データリスト作成
            data_list = []
            for j, maturity in enumerate(common_grid):
                data_list.append({
                    'maturity': float(maturity),
                    'original_yield': float(original_row[j]),
                    'reconstructed_yield': float(reconstructed_row[j]),
                    'error': float(errors[j]),
                    'bond_code': f"SWAP_{int(maturity)}Y", # ダミーコード
                    'bond_name': f"{product_type} {int(maturity)}Y"
                })
            
            # 統計量
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
