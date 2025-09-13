#!/usr/bin/env python3
"""
Principal Component Analysis for Yield Curve Data
イールドカーブデータの主成分分析
"""

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from scipy import interpolate
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime, timedelta

class YieldCurvePCAAnalyzer:
    """イールドカーブの主成分分析クラス"""
    
    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
        
    def get_recent_data(self, days: int = 100) -> pd.DataFrame:
        """
        最近N日分のイールドカーブデータを取得
        
        Args:
            days: 取得する日数
        
        Returns:
            pd.DataFrame: 取引日、残存年数、利回りのデータフレーム
        """
        try:
            import requests
            
            # Supabaseから最近のデータを取得
            url = f"{self.db_manager.supabase_url}/rest/v1/clean_bond_data"
            
            # 最近N日の日付を計算
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days * 2)  # 余裕を持って多めに取得
            
            params = {
                'select': 'trade_date,due_date,ave_compound_yield',
                'trade_date': f'gte.{start_date}',
                'order': 'trade_date.desc,due_date.asc',
                'limit': 50000  # 十分大きな値
            }
            
            self.logger.info(f"PCAデータ取得リクエスト: {url}")
            self.logger.info(f"パラメータ: {params}")
            
            response = requests.get(url, params=params, headers=self.db_manager.headers)
            
            self.logger.info(f"レスポンスステータス: {response.status_code}")
            if response.status_code != 200:
                self.logger.error(f"レスポンス内容: {response.text}")
            
            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data)
                
                if not df.empty:
                    # データ型変換
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                    df['due_date'] = pd.to_datetime(df['due_date']).dt.date
                    df['ave_compound_yield'] = pd.to_numeric(df['ave_compound_yield'], errors='coerce')
                    
                    # 残存年数を計算
                    df['years_to_maturity'] = (
                        (pd.to_datetime(df['due_date']) - pd.to_datetime(df['trade_date'])).dt.days / 365.25
                    )
                    
                    # NaN値を除去
                    df = df.dropna()
                    
                    # 最近N日に限定
                    unique_dates = sorted(df['trade_date'].unique(), reverse=True)[:days]
                    df = df[df['trade_date'].isin(unique_dates)]
                    
                    self.logger.info(f"PCA用データ取得: {len(unique_dates)}日分, {len(df)}レコード")
                    return df
                else:
                    self.logger.warning("データが空です")
                    return pd.DataFrame()
            else:
                self.logger.error(f"データ取得エラー: {response.status_code}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"データ取得例外: {e}")
            return pd.DataFrame()
    
    def create_yield_matrix(self, df: pd.DataFrame, maturity_grid: np.ndarray) -> Tuple[np.ndarray, List[str]]:
        """
        スプライン補間を使用して均一な残存年数グリッドでの利回り行列を作成
        
        Args:
            df: 生データ
            maturity_grid: 補間したい残存年数のグリッド
        
        Returns:
            Tuple[np.ndarray, List[str]]: (利回り行列, 日付リスト)
        """
        try:
            unique_dates = sorted(df['trade_date'].unique())
            yield_matrix = []
            valid_dates = []
            
            for trade_date in unique_dates:
                day_data = df[df['trade_date'] == trade_date].copy()
                
                if len(day_data) < 5:  # 最低限のデータ点数が必要
                    continue
                    
                # 残存年数でソート
                day_data = day_data.sort_values('years_to_maturity')
                
                # 重複する残存年数がある場合は平均値を使用
                day_data = day_data.groupby('years_to_maturity')['ave_compound_yield'].mean().reset_index()
                
                maturities = day_data['years_to_maturity'].values
                yields = day_data['ave_compound_yield'].values
                
                # グリッド範囲内のデータのみを使用
                valid_range = (maturities >= maturity_grid.min()) & (maturities <= maturity_grid.max())
                
                if np.sum(valid_range) < 3:  # 最低3点は必要
                    continue
                
                maturities_valid = maturities[valid_range]
                yields_valid = yields[valid_range]
                
                try:
                    # スプライン補間
                    # 3次スプラインを使用し、外挿は線形で行う
                    spline = interpolate.UnivariateSpline(
                        maturities_valid, yields_valid, 
                        k=min(3, len(maturities_valid) - 1), 
                        s=0
                    )
                    
                    # グリッドポイントで補間
                    interpolated_yields = spline(maturity_grid)
                    
                    yield_matrix.append(interpolated_yields)
                    valid_dates.append(str(trade_date))
                    
                except Exception as interp_e:
                    self.logger.warning(f"補間エラー {trade_date}: {interp_e}")
                    continue
            
            if len(yield_matrix) == 0:
                raise ValueError("有効なデータがありません")
                
            yield_matrix = np.array(yield_matrix)
            
            self.logger.info(f"利回り行列作成完了: {yield_matrix.shape} (日数×満期点数)")
            return yield_matrix, valid_dates
            
        except Exception as e:
            self.logger.error(f"利回り行列作成エラー: {e}")
            raise
    
    def perform_pca(self, yield_matrix: np.ndarray, n_components: int = 3) -> Dict:
        """
        主成分分析を実行
        
        Args:
            yield_matrix: 利回り行列 (日数 × 満期点数)
            n_components: 主成分の数
        
        Returns:
            Dict: PCA結果
        """
        try:
            # データの標準化
            scaler = StandardScaler()
            scaled_yields = scaler.fit_transform(yield_matrix)
            
            # PCA実行
            pca = PCA(n_components=n_components)
            pca_result = pca.fit_transform(scaled_yields)
            
            # 主成分ベクトル（固有ベクトル）
            components = pca.components_
            
            # 寄与率
            explained_variance_ratio = pca.explained_variance_ratio_ * 100
            
            # データ復元
            reconstructed_scaled = pca.inverse_transform(pca_result)
            reconstructed = scaler.inverse_transform(reconstructed_scaled)
            
            # 復元誤差計算
            reconstruction_error = np.mean(np.abs(yield_matrix - reconstructed), axis=0)
            mean_error = np.mean(reconstruction_error)
            
            self.logger.info(f"PCA完了: {n_components}成分, 寄与率合計={np.sum(explained_variance_ratio):.1f}%")
            
            return {
                'n_components': n_components,
                'explained_variance_ratio': explained_variance_ratio.tolist(),
                'principal_components': components.tolist(),
                'pca_scores': pca_result.tolist(),
                'reconstruction_error': reconstruction_error.tolist(),
                'mean_reconstruction_error': mean_error,
                'original_yields': yield_matrix.tolist(),
                'reconstructed_yields': reconstructed.tolist(),
                'scaler_mean': scaler.mean_.tolist(),
                'scaler_scale': scaler.scale_.tolist()
            }
            
        except Exception as e:
            self.logger.error(f"PCA実行エラー: {e}")
            raise
    
    def analyze_yield_curve_pca(self, days: int = 100, n_components: int = 3, 
                               spline_points: int = 50, maturity_range: Tuple[float, float] = (0.1, 30.0)) -> Dict:
        """
        イールドカーブの主成分分析を実行
        
        Args:
            days: 分析対象日数
            n_components: 主成分数
            spline_points: スプライン補間点数
            maturity_range: 残存年数の範囲
        
        Returns:
            Dict: 分析結果
        """
        try:
            # データ取得
            df = self.get_recent_data(days)
            
            if df.empty:
                raise ValueError("分析用データが取得できません")
            
            # 残存年数グリッド作成
            maturity_grid = np.linspace(maturity_range[0], maturity_range[1], spline_points)
            
            # 利回り行列作成
            yield_matrix, valid_dates = self.create_yield_matrix(df, maturity_grid)
            
            if len(yield_matrix) < 10:
                raise ValueError(f"分析に十分なデータがありません（{len(yield_matrix)}日分）")
            
            # PCA実行
            pca_results = self.perform_pca(yield_matrix, n_components)
            
            # 結果を統合
            results = {
                'status': 'success',
                'sample_count': len(yield_matrix),
                'maturity_grid': maturity_grid.tolist(),
                'valid_dates': valid_dates,
                'analysis_params': {
                    'days_requested': days,
                    'days_analyzed': len(valid_dates),
                    'n_components': n_components,
                    'spline_points': spline_points,
                    'maturity_range': maturity_range
                },
                **pca_results
            }
            
            return results
            
        except Exception as e:
            self.logger.error(f"PCA分析エラー: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }