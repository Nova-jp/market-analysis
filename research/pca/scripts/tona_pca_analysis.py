import pandas as pd
import QuantLib as ql
import numpy as np
import os
import re
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from datetime import datetime

# --- Configuration ---
DATA_DIR = "external_data"
FILES = ["1-9.xlsx", "10-40 (1).xlsx"]
MIN_YEAR = 1
MAX_YEAR = 40

def parse_japanese_date(date_str):
    """Converts 'YYYY年MM月DD日' to datetime object."""
    try:
        match = re.match(r'(\d{4})年(\d{1,2})月(\d{1,2})日', str(date_str))
        if match:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except:
        pass
    return None

def load_and_merge_data():
    """Loads Excel files and merges them into a single DataFrame."""
    dfs = []
    
    for f in FILES:
        path = os.path.join(DATA_DIR, f)
        print(f"Loading {path}...")
        # Skip the second row (index 0) which contains "終値"
        df = pd.read_excel(path)
        # The real header is row 0 in pandas if we read normally, but row 1 is garbage.
        # Actually, looking at the previous head() output:
        # Row 0 (index 0) is NaN, "終値", etc.
        # Row 1 (index 1) is "2025年...", data...
        # So we should drop index 0.
        df = df.drop(0)
        
        # Parse Dates
        df['Date'] = df['日付'].apply(parse_japanese_date)
        df = df.dropna(subset=['Date'])
        dfs.append(df)
    
    # Merge on Date
    full_df = dfs[0]
    for i in range(1, len(dfs)):
        full_df = pd.merge(full_df, dfs[i], on='Date', how='inner', suffixes=('', '_drop'))
        # Remove duplicate columns if any
        cols_to_drop = [c for c in full_df.columns if '_drop' in c or c == '日付_drop']
        full_df = full_df.drop(columns=cols_to_drop)
        
    # Extract columns relevant to TONA OIS
    # Pattern: JPYTOCOIS{N}Y=TRDJ (BID)
    tenor_map = {}
    for col in full_df.columns:
        match = re.search(r'JPYTOCOIS(\d+)Y=TRDJ \(BID\)', col)
        if match:
            year = int(match.group(1))
            if year >= MIN_YEAR:
                tenor_map[year] = col
                
    # Filter only relevant columns + Date
    cols = ['Date'] + list(tenor_map.values())
    final_df = full_df[cols].copy()
    
    # Rename columns to just integer years
    final_df = final_df.rename(columns={v: k for k, v in tenor_map.items()})
    
    # Sort by Date
    final_df = final_df.sort_values('Date').reset_index(drop=True)
    
    # Convert rates to float
    for y in tenor_map.keys():
        final_df[y] = pd.to_numeric(final_df[y], errors='coerce')
        
    final_df = final_df.dropna()
    return final_df

def calculate_forward_rates(df):
    """
    Constructs QL curve for each date and calculates 1Y forwards.
    """
    
    # Setup QuantLib objects
    calendar = ql.Japan()
    day_count = ql.Actual365Fixed()
    
    # TONA Index
    # TONA is usually T+0 settlement (overnight), but the OIS swaps are standard.
    # We'll use a standard OvernightIndex.
    # Note: OISRateHelper expects a specific structure.
    tona = ql.OvernightIndex("TONA", 0, ql.JPYCurrency(), calendar, day_count)
    
    forward_results = []
    dates = []
    
    print("Calculating Yield Curves and Forward Rates...")
    
    # Identify available tenors in the dataframe (excluding 'Date')
    available_tenors = [c for c in df.columns if isinstance(c, int)]
    available_tenors.sort()
    
    for idx, row in df.iterrows():
        eval_date = row['Date']
        ql_eval_date = ql.Date(eval_date.day, eval_date.month, eval_date.year)
        
        ql.Settings.instance().evaluationDate = ql_eval_date
        
        # Create Helpers
        helpers = []
        for t in available_tenors:
            rate = row[t]
            if np.isnan(rate):
                continue
            
            # Rate is in percent, QuantLib needs decimal
            quote = ql.SimpleQuote(rate / 100.0)
            
            # OIS Helper
            # Settlement days = 2 is standard for many OIS swaps, or 0.
            # Usually JPY OIS starts T+2? 
            # Looking at market conventions: JPY OIS (TONA) is typically T+0 (Spot Start) or T+2.
            # Given the lack of specific instruction, T+2 is safer for standard swaps, 
            # but TONA is Overnight. Let's stick to 2 which is common for "Swaps".
            helper = ql.OISRateHelper(2, ql.Period(t, ql.Years), ql.QuoteHandle(quote), tona)
            helpers.append(helper)
            
        # Build Curve
        # "補完はスプライン補完を使用してください" -> Piecewise Cubic Spline
        # PiecewiseLogCubicDiscount is a very stable spline on discount factors.
        try:
            curve = ql.PiecewiseLogCubicDiscount(0, calendar, helpers, day_count)
            curve.enableExtrapolation()
            
            # Calculate 1Y Forwards
            # We want forwards starting at 1Y, 2Y, ... up to MaxYear-1
            # "フォワードは一年ずつ計算してください" -> 1y1y, 2y1y, 3y1y...
            
            # Since we have data up to 40Y, we can calculate forwards ending at 40Y.
            # i.e. Start at 39Y, End at 40Y.
            
            row_forwards = {}
            
            # Calculate from 1Y up to (Max available year - 1)
            # If we have 40Y data, the last 1Y forward is 39y1y (starts 39, ends 40).
            max_tenor = max(available_tenors)
            
            for start_year in range(1, max_tenor):
                d1 = calendar.advance(ql_eval_date, ql.Period(start_year, ql.Years))
                d2 = calendar.advance(ql_eval_date, ql.Period(start_year + 1, ql.Years))
                
                fwd_rate = curve.forwardRate(d1, d2, day_count, ql.Simple).rate()
                row_forwards[f"Fwd_{start_year}Y_1Y"] = fwd_rate * 100.0 # Convert back to %
                
            dates.append(eval_date)
            forward_results.append(row_forwards)
            
        except RuntimeError as e:
            print(f"Error on {eval_date}: {e}")
            continue

    fwd_df = pd.DataFrame(forward_results)
    fwd_df['Date'] = dates
    fwd_df = fwd_df.set_index('Date')
    return fwd_df

def run_pca(fwd_df):
    """
    Runs PCA on the forward rates dataframe.
    """
    print("\nRunning PCA...")
    
    # Drop rows with any NaNs (if curve building failed or ranges mismatch)
    data = fwd_df.dropna()
    
    if data.empty:
        print("No valid data for PCA.")
        return
    
    # Standardize
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(data)
    
    # PCA
    # We usually look at first 3 components (Level, Slope, Curvature)
    n_components = min(5, len(data.columns))
    pca = PCA(n_components=n_components)
    pca.fit(X_scaled)
    
    # Explained Variance
    print("\nExplained Variance Ratio:")
    for i, var in enumerate(pca.explained_variance_ratio_):
        print(f"PC{i+1}: {var:.4f}")
        
    # Reconstruct data to find residuals
    # Transform to PC space
    X_pca = pca.transform(X_scaled)
    
    # Reconstruct using top 3 components (assuming they capture "fair value")
    # If the user wants to find "Rich/Cheap" relative to the MAIN factors.
    # Typically we use top 3.
    pca_3 = PCA(n_components=3)
    X_pca_3 = pca_3.fit_transform(X_scaled)
    X_reconstructed_scaled = pca_3.inverse_transform(X_pca_3)
    X_reconstructed = scaler.inverse_transform(X_reconstructed_scaled)
    
    reconstructed_df = pd.DataFrame(X_reconstructed, index=data.index, columns=data.columns)
    
    # Residuals = Actual - Model (Reconstructed)
    # Positive Residual -> Actual Rate > Model Rate -> Cheap (High Yield) ? 
    # Wait, Rate is high means Price is Low. 
    # If Actual Rate > Fair Rate, it means the bond/swap is yielding MORE than it should.
    # High Yield = Cheap Price.
    # So Positive Residual = Cheap.
    # Negative Residual = Expensive (Rich).
    residuals = data - reconstructed_df
    
    # Show Latest Analysis
    latest_date = data.index[-1]
    latest_resid = residuals.loc[latest_date]
    
    print(f"\n--- Analysis for {latest_date.strftime('%Y-%m-%d')} ---")
    print("Residuals (Actual - Model (PC1-3)). Unit: % (basis points if *100)")
    print("Positive = Cheap (Rate too high), Negative = Rich (Rate too low)")
    
    # Sort residuals to find most mispriced points
    sorted_resid = latest_resid.sort_values(ascending=False) # Descending: Cheap to Rich
    
    print("\nTop 5 Cheapest (Highest Positive Residuals):")
    print(sorted_resid.head(5))
    
    print("\nTop 5 Richest (Lowest Negative Residuals):")
    print(sorted_resid.tail(5))

    # Save Results
    output_path = "analysis/tona_pca_results.csv"
    residuals.to_csv(output_path)
    print(f"\nFull residual history saved to {output_path}")

    # Return loadings for inspection
    loadings = pd.DataFrame(pca.components_.T, columns=[f'PC{i+1}' for i in range(n_components)], index=data.columns)
    return loadings

if __name__ == "__main__":
    # 1. Load Data
    raw_df = load_and_merge_data()
    print(f"Loaded data range: {raw_df['Date'].min()} to {raw_df['Date'].max()}")
    print(f"Columns (Tenors): {list(raw_df.columns[1:])}")
    
    # 2. Calculate Forwards
    fwd_df = calculate_forward_rates(raw_df)
    print(f"Calculated {len(fwd_df)} days of forward curves.")
    if not fwd_df.empty:
        print(f"Forward Tenors: {list(fwd_df.columns)}")
    
    # 3. Run PCA
    loadings = run_pca(fwd_df)
    
    if loadings is not None:
        print("\nPC1 Loadings (First 10):")
        print(loadings['PC1'].head(10))