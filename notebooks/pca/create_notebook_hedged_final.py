import json
import os

script_path = 'notebooks/pca/backtest_logic_hedged.py'
notebook_path = 'notebooks/pca/backtest_strategy_hedged.ipynb'

with open(script_path, 'r') as f:
    code = f.read()

# Split logic for better notebook readability
imports_end = code.find('def fetch_all_data')
class_start = code.find('class PCAStrategyBacktestHedged')
main_start = code.find('if __name__ == "__main__":')

imports_code = code[:imports_end]
fetch_data_code = code[imports_end:class_start]
class_code = code[class_start:main_start]

# Main Execution Logic for Notebook
main_execution_code = """

db_manager = DatabaseManager()
# Fetch data from 2023 to compare over a longer period
df_all = fetch_all_data(db_manager, start_date='2023-01-01')

if not df_all.empty:
    # Run Backtest
    backtester = PCAStrategyBacktestHedged(df_all, window=50)
    results_df = backtester.run()
else:
    print("No data found.")
"""

# Visualization Logic
visualization_code = """

if 'results_df' in locals() and not results_df.empty:
    # Cumulative PnL Calculation
    results_df['cum_no_hedge'] = results_df['no_hedge_pnl'].cumsum()
    results_df['cum_hedged'] = results_df['hedged_total_pnl'].cumsum()

    # Plot
    plt.figure(figsize=(14, 7))
    plt.plot(results_df['date'], results_df['cum_no_hedge'], label='No Hedge (Original)', linewidth=2, color='blue')
    plt.plot(results_df['date'], results_df['cum_hedged'], label='PC1 Hedged (with 7Y Bond)', linewidth=2, color='green', linestyle='--')
    
    plt.title('Strategy Performance Comparison: No Hedge vs PC1 Hedged (bp)')
    plt.xlabel('Date')
    plt.ylabel('Cumulative P&L (bp)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.axhline(0, color='black', linewidth=0.5)
    plt.show()
    
    # Statistics
    print("=== Performance Summary ===")
    print(f"Total Return (No Hedge): {results_df['cum_no_hedge'].iloc[-1]:.2f} bp")
    print(f"Total Return (Hedged)  : {results_df['cum_hedged'].iloc[-1]:.2f} bp")
    print("-" * 30)
    print(f"Daily Volatility (No Hedge): {results_df['no_hedge_pnl'].std():.2f} bp")
    print(f"Daily Volatility (Hedged)  : {results_df['hedged_total_pnl'].std():.2f} bp")
    
    # Save Results
    try:
        output_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        output_dir = 'notebooks/pca' if os.path.isdir('notebooks/pca') else '.'
    
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, 'backtest_comparison_results.csv')
    results_df.to_csv(csv_path, index=False)
    print(f"Detailed results saved to: {csv_path}")
"""

notebook = {
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# JGB PCA Strategy: PC1 Hedge Analysis\n",
    "\n",
    "## Overview\n",
    "Comparison of two strategies:\n",
    "1. **Original Strategy**: Long/Short based on PCA reconstruction error (PC1+PC2).\n",
    "2. **Hedged Strategy**: Adds a hedge using **7-year JGBs** to neutralize the portfolio's exposure to the **First Principal Component (PC1)**.\n",
    "\n",
    "### Logic for Hedging\n",
    "- Calculate PC1 loading (sensitivity) for the Long bond ($v_L$) and Short bond ($v_S$).\n",
    "- Calculate Portfolio PC1 Exposure: $E_{port} = v_L - v_S$ (assuming equal notional).\n",
    