import pandas as pd
import numpy as np
import QuantLib as ql
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, date
from sklearn.decomposition import PCA
from app.core.database import db_manager

class PrivateAnalysisService:
    """
    Service for private analytics (Swap Curve & PCA).
    Uses ThreadPoolExecutor for CPU-bound QuantLib/PCA tasks to avoid blocking the event loop.
    """

    def __init__(self):
        self.db = db_manager
        # Executor for CPU-bound tasks
        self.executor = ThreadPoolExecutor(max_workers=2)

    def _convert_tenor(self, t: str) -> Optional[ql.Period]:
        """Convert tenor string to QuantLib.Period."""
        if not t: return None
        t = str(t).upper()
        if 'Y' in t: 
            val = t.replace('Y', '').split('(')[0]
            return ql.Period(int(val), ql.Years)
        if 'M' in t: 
            val = t.replace('M', '').split('(')[0]
            return ql.Period(int(val), ql.Months)
        return None

    def _build_curve_sync(self, day_rates: pd.DataFrame, eval_date: ql.Date) -> Optional[ql.YieldTermStructure]:
        """Synchronous curve building logic."""
        ql.Settings.instance().evaluationDate = eval_date
        
        helpers = []
        for _, row in day_rates.iterrows():
            tenor = self._convert_tenor(row['tenor'])
            if tenor:
                rate = float(row['rate']) / 100.0
                helpers.append(
                    ql.OISRateHelper(2, tenor, ql.QuoteHandle(ql.SimpleQuote(rate)), 
                                     ql.OvernightIndex("TONA", 2, ql.JPYCurrency(), ql.Japan(), ql.Actual365Fixed()))
                )
        
        if not helpers:
            return None
            
        try:
            curve = ql.PiecewiseLogCubicDiscount(eval_date, helpers, ql.Actual365Fixed())
            curve.enableExtrapolation()
            return curve
        except Exception:
            return None

    def _calculate_forward_curve_sync(self, df: pd.DataFrame, target_date_str: str) -> Dict[str, Any]:
        """CPU-bound calculation for forward curve."""
        if df.empty:
            return {"error": f"No data found for {target_date_str}"}

        # Ensure trade_date is datetime object for ql.Date
        # df comes from db_manager which converts dates to strings.
        first_date = df['trade_date'].iloc[0]
        if isinstance(first_date, str):
            d = datetime.strptime(first_date, '%Y-%m-%d').date()
        else:
            d = first_date

        ql_date = ql.Date(d.day, d.month, d.year)
        curve = self._build_curve_sync(df, ql_date)
        
        if not curve:
            return {"error": "Failed to construct curve"}

        # 1. Spot Curve
        spot_x = np.arange(0.5, 30.5, 0.5).tolist()
        spot_y = []
        for t in spot_x:
            mat_date = ql_date + ql.Period(int(t*12), ql.Months)
            rate = curve.zeroRate(mat_date, ql.Actual365Fixed(), ql.Compounded, ql.Annual).rate() * 100
            spot_y.append(round(rate, 4))

        # 2. Forward Curve (n-year ahead, 1Y tenor)
        fwd_x = np.arange(0.0, 20.25, 0.25).tolist() 
        fwd_y = []
        for s in fwd_x:
            start_node = ql_date + ql.Period(int(s*12), ql.Months) if s > 0 else ql_date
            end_node = start_node + ql.Period(1, ql.Years)
            rate = curve.forwardRate(start_node, end_node, ql.Actual365Fixed(), ql.Compounded, ql.Annual).rate() * 100
            fwd_y.append(round(rate, 4))

        return {
            "date": str(d),
            "spot": {"x": spot_x, "y": spot_y},
            "forward_1y": {"x": fwd_x, "y": fwd_y}
        }

    def _run_pca_sync(self, df: pd.DataFrame, n_components: int) -> Dict[str, Any]:
        """CPU-bound PCA calculation."""
        if df.empty:
            return {"error": "No data found"}
            
        # Ensure dates are date objects for grouping
        if pd.api.types.is_string_dtype(df['trade_date']):
             df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        unique_dates = sorted(df['trade_date'].unique())
        
        calc_starts = np.arange(0.0, 10.5, 0.5).tolist()
        calc_tenors = (np.arange(0.5, 10.5, 0.5).tolist() + [15.0, 20.0, 30.0])
        
        data_matrix = []
        valid_dates = []

        for d in unique_dates:
            day_data = df[df['trade_date'] == d]
            ql_date = ql.Date(d.day, d.month, d.year)
            curve = self._build_curve_sync(day_data, ql_date)
            
            if curve:
                row = []
                try:
                    for s in calc_starts:
                        start_node = ql_date + ql.Period(int(s*12), ql.Months) if s > 0 else ql_date
                        for t in calc_tenors:
                            end_node = start_node + ql.Period(int(t*12), ql.Months)
                            fwd = curve.forwardRate(start_node, end_node, ql.Actual365Fixed(), ql.Compounded, ql.Annual).rate() * 100
                            row.append(fwd)
                    data_matrix.append(row)
                    valid_dates.append(str(d))
                except Exception:
                    continue

        if not data_matrix:
            return {"error": "Could not construct enough curves"}

        X = np.array(data_matrix)
        pca = PCA(n_components=n_components)
        scores = pca.fit_transform(X)
        X_recon = pca.inverse_transform(scores)
        residuals = X - X_recon
        
        components_data = []
        for i in range(min(n_components, X.shape[0])):
            comp_flat = pca.components_[i]
            matrix = []
            idx = 0
            for s in calc_starts:
                row_vals = []
                for t in calc_tenors:
                    row_vals.append(round(float(comp_flat[idx]), 6))
                    idx += 1
                matrix.append(row_vals)
            components_data.append({
                "pc": i + 1,
                "explained_variance": round(float(pca.explained_variance_ratio_[i]), 4),
                "matrix": matrix
            })

        residuals_map = {}
        for i, date_str in enumerate(valid_dates):
            res_flat = residuals[i]
            res_matrix = []
            idx = 0
            for s in calc_starts:
                row_vals = []
                for t in calc_tenors:
                    row_vals.append(round(float(res_flat[idx]), 6))
                    idx += 1
                res_matrix.append(row_vals)
            residuals_map[date_str] = res_matrix

        return {
            "dates": valid_dates,
            "starts": calc_starts,
            "tenors": calc_tenors,
            "components": components_data,
            "residuals": residuals_map,
            "scores": scores.tolist()
        }

    async def get_forward_curve_data(self, target_date: str) -> Dict[str, Any]:
        """Async wrapper for forward curve calculation."""
        # Convert string to date object for DB query
        if isinstance(target_date, str):
            d_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
        else:
            d_obj = target_date
            
        # 1. Fetch Data (Async I/O)
        data = await self.db.get_ois_data_range(d_obj, d_obj)
        df = pd.DataFrame(data)
        
        # 2. Run Calculation in ThreadPool (CPU)
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self.executor,
            self._calculate_forward_curve_sync,
            df,
            str(target_date)
        )
        return result

    async def run_pca_analysis(self, start_date: str, end_date: str, n_components: int = 3) -> Dict[str, Any]:
        """Async wrapper for PCA analysis."""
        # Convert string to date object
        s_date = datetime.strptime(start_date, '%Y-%m-%d').date() if isinstance(start_date, str) else start_date
        e_date = datetime.strptime(end_date, '%Y-%m-%d').date() if isinstance(end_date, str) else end_date

        # 1. Fetch Data (Async I/O)
        data = await self.db.get_ois_data_range(s_date, e_date)
        df = pd.DataFrame(data)
        
        # 2. Run Calculation in ThreadPool (CPU)
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self.executor,
            self._run_pca_sync,
            df,
            n_components
        )
        return result