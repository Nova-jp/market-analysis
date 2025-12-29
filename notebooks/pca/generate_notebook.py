import nbformat as nbf
import os

b = nbf.v4.new_notebook()

# --- Cell 1: Intro ---
text_intro = """# TONA OIS PCA Visualization Tool (2D Surface Analysis)

This notebook analyzes the TONA OIS market using PCA on the **Forward Rate Surface**.
We construct a matrix of Forward Rates:
- **Start Years (Horizon):** 1Y to 10Y (Forward Start)
- **Tenors (Duration):** 1Y to 40Y (Underlying Rate)

This results in a 40x10 grid (400 points) per date. We analyze the principal components of this surface and visualize the residuals (Rich/Cheap) as a 2D Heatmap."""

# --- Cell 2: Imports ---
code_imports = """import pandas as pd
import QuantLib as ql
import numpy as np
import os
import re
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import ipywidgets as widgets
from ipywidgets import interact, interactive

%matplotlib inline
sns.set_style("whitegrid")"""

# --- Cell 3: Data Loading & Processing ---
code_data = r'''# --- Data Configuration ---
DATA_DIR = "../../external_data"
FILES = ["1-9.xlsx", "10-40 (1).xlsx"]
MIN_YEAR = 1

# Configuration for Analysis Grid
MAX_START_YEAR = 10  # "10年先まで"
MAX_TENOR_YEAR = 40  # "40年(のデータ)"

def parse_japanese_date(date_str):
    try:
        match = re.match(r'(\d{4})年(\d{1,2})月(\d{1,2})日', str(date_str))
        if match:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except:
        pass
    return None

def load_and_merge_data():
    dfs = []
    for f in FILES:
        path = os.path.join(DATA_DIR, f)
        if not os.path.exists(path):
            # Fallback paths
            path = os.path.abspath(os.path.join(os.getcwd(), "../../external_data", f))
            if not os.path.exists(path):
                 path = os.path.abspath(os.path.join(os.getcwd(), "../../external_data", f))
        
        try:
            df = pd.read_excel(path)
            df = df.drop(0)
            df['Date'] = df['日付'].apply(parse_japanese_date)
            df = df.dropna(subset=['Date'])
            dfs.append(df)
        except Exception as e:
            print(f"Error loading {f}: {e}")
            return pd.DataFrame()
    
    if not dfs: return pd.DataFrame()

    full_df = dfs[0]
    for i in range(1, len(dfs)):
        full_df = pd.merge(full_df, dfs[i], on='Date', how='inner', suffixes=('', '_drop'))
        cols_to_drop = [c for c in full_df.columns if '_drop' in c or c == '日付_drop']
        full_df = full_df.drop(columns=cols_to_drop)
        
    tenor_map = {}
    for col in full_df.columns:
        match = re.search(r'JPYTOCOIS(\d+)Y=TRDJ \(BID\)', col)
        if match:
            year = int(match.group(1))
            if year >= MIN_YEAR:
                tenor_map[year] = col
                
    cols = ['Date'] + list(tenor_map.values())
    final_df = full_df[cols].copy()
    final_df = final_df.rename(columns={v: k for k, v in tenor_map.items()})
    final_df = final_df.sort_values('Date').reset_index(drop=True)
    
    for y in tenor_map.keys():
        final_df[y] = pd.to_numeric(final_df[y], errors='coerce')
        
    final_df = final_df.dropna()
    return final_df

def calculate_forward_matrix(df):
    """
    Constructs QL curve and calculates a matrix of Forward Rates.
    Start: 1..MAX_START_YEAR
    Tenor: 1..MAX_TENOR_YEAR
    """
    calendar = ql.Japan()
    day_count = ql.Actual365Fixed()
    tona = ql.OvernightIndex("TONA", 0, ql.JPYCurrency(), calendar, day_count)
    
    forward_results = []
    dates = []
    
    available_tenors = [c for c in df.columns if isinstance(c, int)]
    available_tenors.sort()
    
    print(f"Calculating {MAX_START_YEAR}x{MAX_TENOR_YEAR} Forward Matrix for {len(df)} days...")
    
    for idx, row in df.iterrows():
        eval_date = row['Date']
        ql_eval_date = ql.Date(eval_date.day, eval_date.month, eval_date.year)
        ql.Settings.instance().evaluationDate = ql_eval_date
        
        helpers = []
        for t in available_tenors:
            rate = row[t]
            if np.isnan(rate): continue
            quote = ql.SimpleQuote(rate / 100.0)
            helper = ql.OISRateHelper(2, ql.Period(t, ql.Years), ql.QuoteHandle(quote), tona)
            helpers.append(helper)
            
        try:
            curve = ql.PiecewiseLogCubicDiscount(0, calendar, helpers, day_count)
            curve.enableExtrapolation() # Needed for Start+Tenor > 40
            
            row_data = {}
            
            # Calculate Grid
            for start in range(1, MAX_START_YEAR + 1):
                for tenor in range(1, MAX_TENOR_YEAR + 1):
                    # For Tenor=1, we can use simple forward rate.
                    # For Tenor>1, ideally Par Swap Rate.
                    # Approximation: (DF_start - DF_end) / Annuity
                    # Given "accurately draw fwd curve", we should be careful.
                    # However, calling MakeVanillaSwap is slow.
                    # We will use the proper forwardRate method from QuantLib which computes 
                    # simple compounding rate for the given period. 
                    # For long tenors, this is NOT the Swap Rate, it's the simple rate.
                    # BUT, "Forward Rate" in loose context often means the Swap Rate for that tenor.
                    # Let's assume implied Swap Rate.
                    
                    d_start = calendar.advance(ql_eval_date, ql.Period(start, ql.Years))
                    d_end = calendar.advance(d_start, ql.Period(tenor, ql.Years))
                    
                    # Calculate Par Rate approximately using discount factors to be fast
                    # Swap Rate = (DF(Start) - DF(End)) / Sum(DF(PaymentDates))
                    # Assuming annual payments for simplicity and speed
                    
                    df_start = curve.discount(d_start)
                    df_end = curve.discount(d_end)
                    
                    # Annuity approximation (annual steps)
                    annuity = 0.0
                    for k in range(1, tenor + 1):
                        d_pay = calendar.advance(d_start, ql.Period(k, ql.Years))
                        annuity += curve.discount(d_pay)
                        
                    if annuity == 0:
                        swap_rate = 0.0
                    else:
                        swap_rate = (df_start - df_end) / annuity
                        
                    # Store as "S{start}_T{tenor}"
                    col_name = f"S{start}_T{tenor}"
                    row_data[col_name] = swap_rate * 100.0
            
            dates.append(eval_date)
            forward_results.append(row_data)
            
        except RuntimeError:
            continue

    fwd_df = pd.DataFrame(forward_results)
    fwd_df['Date'] = dates
    fwd_df = fwd_df.set_index('Date')
    return fwd_df

# Load and Process Data
raw_df = load_and_merge_data()
if raw_df.empty:
    print("No data loaded.")
else:
    full_fwd_df = calculate_forward_matrix(raw_df)
    print(f"Data Ready: {len(full_fwd_df)} days. Columns: {len(full_fwd_df.columns)}")'''

# --- Cell 4: Interactive PCA ---
code_pca = r'''def run_interactive_pca(pcs_to_use):
    data = full_fwd_df.dropna()
    if data.empty:
        print("No valid data.")
        return

    # Standardize
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(data)
    
    # PCA
    n_components = min(50, len(data)) # Allow more PCs since we have 400 features
    pca = PCA(n_components=n_components)
    pca.fit(X_scaled)
    
    # Reconstruct
    n_use = min(pcs_to_use, n_components)
    pca_subset = PCA(n_components=n_use)
    X_subset = pca_subset.fit_transform(X_scaled)
    X_recon_scaled = pca_subset.inverse_transform(X_subset)
    X_recon = scaler.inverse_transform(X_recon_scaled)
    
    recon_df = pd.DataFrame(X_recon, index=data.index, columns=data.columns)
    residuals = data - recon_df
    
    # Latest Analysis
    latest_date = data.index[-1]
    latest_resid = residuals.loc[latest_date]
    
    # --- Transform 1D Residuals back to 2D Matrix for Heatmap ---
    # Our columns are "S{start}_T{tenor}"
    # Start: 1..10 (Rows of Heatmap? Or Cols?)
    # Tenor: 1..40 (Cols of Heatmap? Or Rows?)
    # Let's put Start Year on X-axis (1..10) and Tenor on Y-axis (1..40)
    
    matrix = np.zeros((40, 10)) # Rows=Tenor(40), Cols=Start(10)
    
    for start in range(1, 11):
        for tenor in range(1, 41):
            col_name = f"S{start}_T{tenor}"
            if col_name in latest_resid:
                # Array indices are 0-based
                matrix[tenor-1, start-1] = latest_resid[col_name]
                
    # Visualization
    plt.figure(figsize=(12, 10))
    
    sns.heatmap(matrix, cmap="RdYlGn_r", center=0,
                xticklabels=range(1, 11), 
                yticklabels=range(1, 41),
                cbar_kws={'label': 'Residual (bps)'})
                
    plt.title(f"TONA Forward Swap Residuals (Actual - Model) on {latest_date.strftime('%Y-%m-%d')}
              f"Model: PCA 1-{n_use} | X: Start Year (1-10) | Y: Tenor (1-40)")
    plt.xlabel("Forward Start Year (In X Years)")
    plt.ylabel("Swap Tenor (Duration)")
    plt.gca().invert_yaxis() # Usually short tenor at bottom? No, heatmap standard is top-down.
    # Let's keep Tenor 1 at top or bottom?
    # Standard matrix: index 0 is top. So Tenor 1 is top.
    # If we want Tenor 1 at bottom, invert.
    plt.gca().invert_yaxis()
    
    plt.show()
    
    # Stats
    print(f"Explained Variance (first {n_use} PCs): {np.sum(pca.explained_variance_ratio_[:n_use]):.2%}")
    
    # Find max mispricing
    cheapest = latest_resid.idxmax()
    richest = latest_resid.idxmin()
    print(f"Cheapest Point: {cheapest} ({latest_resid.max():.2f} bps)")
    print(f"Richest Point:  {richest} ({latest_resid.min():.2f} bps)")

# Create Widgets
style = {'description_width': 'initial'}
w_pcs = widgets.IntSlider(value=3, min=1, max=10, step=1, description='PCs to Use:', style=style)

ui = widgets.VBox([w_pcs])
out = widgets.interactive_output(run_interactive_pca, {'pcs_to_use': w_pcs})

display(ui, out)'''

b['cells'] = [
    nbf.v4.new_markdown_cell(text_intro),
    nbf.v4.new_code_cell(code_imports),
    nbf.v4.new_code_cell(code_data),
    nbf.v4.new_code_cell(code_pca)
]

output_path = os.path.join("notebooks/pca", "tona_pca_visualization.ipynb")
with open(output_path, 'w') as f:
    nbf.write(b, f)

print(f"Created notebook at {output_path}")