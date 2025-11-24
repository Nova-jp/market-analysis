"""
PCA Analysis Service
主成分分析サービス層 - Jupyter Notebookのロジックを関数化
"""
import numpy as np
import pandas as pd
import requests
from datetime import datetime, timedelta
from scipy.interpolate import CubicSpline
from sklearn.decomposition import PCA
from typing import Dict, List, Tuple, Optional

from app.core.config import settings


class PCAService:
    """主成分分析サービスクラス"""

    def __init__(self):
        self.MIN_MATURITY = 0.5  # 最小残存期間（年）
        self.headers = {
            'apikey': settings.supabase_key,
            'Authorization': f'Bearer {settings.supabase_key}'
        }
        # HTTPセッションを使用してコネクションプーリングを有効化
        self.session = requests.Session()
        self.session.headers.update(self.headers)

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

        注意: Supabase REST APIの制限により、1回のクエリで最大10,000レコードまで。
        より多くの日付を取得するには、ページネーションを使用する。
        """
        all_dates = set()
        offset = 0
        batch_size = 10000  # Supabaseの最大値

        # 必要な日付数を取得するまでページネーション
        # 安全のため、最大10回まで（= 最大100,000レコード）
        max_iterations = 10

        for iteration in range(max_iterations):
            response = self.session.get(
                f'{settings.supabase_url}/rest/v1/bond_data',
                params={
                    'select': 'trade_date',
                    'order': 'trade_date.desc',
                    'limit': batch_size,
                    'offset': offset
                },
                timeout=60  # Cloud Run用にタイムアウトを延長
            )

            if response.status_code != 200:
                raise Exception(f'データ取得失敗: {response.status_code}')

            data = response.json()

            # データが空なら終了
            if not data:
                break

            # 日付を追加
            batch_dates = set([row['trade_date'] for row in data])
            all_dates.update(batch_dates)

            # 必要な日付数に達したら終了
            if len(all_dates) >= limit:
                break

            offset += batch_size

        # ソートして必要な件数だけ返す
        dates = sorted(list(all_dates), reverse=True)
        return dates[:limit]

    def get_yield_data_for_date(self, date: str, with_bond_code: bool = False) -> pd.DataFrame:
        """指定日のイールドカーブデータを取得"""
        response = self.session.get(
            f'{settings.supabase_url}/rest/v1/bond_data',
            params={
                'select': 'trade_date,due_date,ave_compound_yield,bond_code,bond_name',
                'trade_date': f'eq.{date}',
                'ave_compound_yield': 'not.is.null',
                'due_date': 'not.is.null'
            },
            timeout=60  # Cloud Run用にタイムアウトを延長
        )

        if response.status_code != 200:
            raise Exception(f'データ取得失敗: {response.status_code}')

        data = response.json()
        data_list = []

        for row in data:
            try:
                trade_dt = datetime.strptime(row['trade_date'], '%Y-%m-%d')
                due_dt = datetime.strptime(row['due_date'], '%Y-%m-%d')
                maturity_years = (due_dt - trade_dt).days / 365.25

                if maturity_years >= self.MIN_MATURITY:
                    data_dict = {
                        'maturity': round(maturity_years, 4),
                        'yield': float(row['ave_compound_yield'])
                    }
                    if with_bond_code:
                        data_dict['bond_code'] = self.normalize_bond_code(row['bond_code'])
                        data_dict['bond_name'] = row.get('bond_name', 'N/A')
                    data_list.append(data_dict)
            except (ValueError, TypeError, KeyError):
                continue

        df = pd.DataFrame(data_list)
        if with_bond_code and not df.empty:
            df['bond_code'] = df['bond_code'].astype(str)
            df['bond_name'] = df['bond_name'].astype(str)

        return df

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
        # 残存期間の和集合を作成
        all_maturities = set()
        for df in daily_data.values():
            all_maturities.update(df['maturity'].values)

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
