import json
import os

# Define the paths
script_path = 'notebooks/pca/backtest_logic_hedged.py'
notebook_path = 'notebooks/pca/backtest_strategy_hedged.ipynb'

# Read the script content
with open(script_path, 'r') as f:
    full_code = f.read()

# Split the code into sections for the notebook
imports_end = full_code.find('def fetch_all_data')
class_start = full_code.find('class PCAStrategyBacktestHedged')
main_start = full_code.find('if __name__ == "__main__":')

imports_code = full_code[:imports_end].strip()
fetch_data_code = full_code[imports_end:class_start].strip()
class_code = full_code[class_start:main_start].strip()

# Create the notebook structure
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
                "1. **Original Strategy**: Long/Short based on PCA reconstruction error.\n",
                "2. **Hedged Strategy**: Adds a hedge using **7-year JGBs** to neutralize the portfolio's exposure to the **First Principal Component (PC1)**.\n",
                "\n",
                "### Improvements Made\n",
                "- **Bond Tracking**: The hedge bond is selected on the entry day and tracked by its **unique name** on the exit day, ensuring consistency even if the \"nearest 7-year bond\" changes.\n",
                "- **Spike Mitigation**: Hedge quantity $Q$ is clipped to avoid extreme positions caused by near-zero loadings.\n",
                "- **Debug Logging**: Added warnings for daily PnL swings larger than 20bp."
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [imports_code + "\n"]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [fetch_data_code + "\n"]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [class_code + "\n"]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "db_manager = DatabaseManager()\n",
                "df_all = fetch_all_data(db_manager, start_date='2024-01-01')\n",
                "\n",
                "if not df_all.empty:\n",
                "    backtester = PCAStrategyBacktestHedged(df_all, window=50)\n",
                "    results_df = backtester.run()\n",
                "else:\n",
                "    print(\"No data found.\")\n"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "if 'results_df' in locals() and not results_df.empty:\n",
                "    results_df['cum_no_hedge'] = results_df['no_hedge_pnl'].cumsum()\n",
                "    results_df['cum_hedged'] = results_df['hedged_total_pnl'].cumsum()\n",
                "\n",
                "    plt.figure(figsize=(14, 7))\n",
                "    plt.plot(results_df['date'], results_df['cum_no_hedge'], label='No Hedge (Original)', linewidth=2, color='blue')\n",
                "    plt.plot(results_df['date'], results_df['cum_hedged'], label='PC1 Hedged (7Y Bond)', linewidth=2, color='green', linestyle='--')\n",
                "    \n",
                "    plt.title('Strategy Performance Comparison: No Hedge vs PC1 Hedged (bp)')\n",
                "    plt.xlabel('Date')\n",
                "    plt.ylabel('Cumulative P&L (bp)')\n",
                "    plt.legend()\n",
                "    plt.grid(True, alpha=0.3)\n",
                "    plt.axhline(0, color='black', linewidth=0.5)\n",
                "    plt.show()\n",
                "    \n",
                "    print(\"=== Performance Summary ===")\n",
                "    print(f\"Total Return (No Hedge): {results_df['cum_no_hedge'].iloc[-1]:.2f} bp\")\n",
                "    print(f\"Total Return (Hedged)  : {results_df['cum_hedged'].iloc[-1]:.2f} bp\")\n",
                "    print(\"-" * 30)\n",
                "    print(f\"Daily Volatility (No Hedge): {results_df['no_hedge_pnl'].std():.2f} bp\")\n",
                "    print(f\"Daily Volatility (Hedged)  : {results_df['hedged_total_pnl'].std():.2f} bp\")\n"
            ]
        }
    ],
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3"
        },
        "language_info": {
            "codemirror_mode": {"name": "ipython", "version": 3},
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "nbconvert_exporter": "python",
            "pygments_lexer": "ipython3",
            "version": "3.8.5"
        }
    },
    "nbformat": 4,
    "nbformat_minor": 4
}

# Write the notebook
with open(notebook_path, 'w', encoding='utf-8') as f:
    json.dump(notebook, f, indent=1, ensure_ascii=False)

print(f"Successfully updated notebook at {notebook_path}")
