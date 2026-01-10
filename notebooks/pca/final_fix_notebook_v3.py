import json
import os

notebook_path = 'notebooks/pca/backtest_strategy_double_hedged.ipynb'

notebook = {
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# JGB PCA Strategy: PC1 & PC2 Double Hedge Analysis\n",
    "\n",
    "## Overview\n",
    "This strategy aims to capture the reconstruction error while neutralizing risks from the first two principal components:\n",
    "1. PC1 (Level): Shift in the overall level of yields.\n",
    "2. PC2 (Slope/Twist): Change in the slope of the yield curve.\n",
    "\n",
    "### Hedging Logic\n",
    "We use two liquid instruments: 7-year JGBs and 10-year JGBs.\n",
    "- Let v_L1, v_L2 be the PC1 and PC2 loadings of the Long bond.\n",
    "- Let v_S1, v_S2 be the PC1 and PC2 loadings of the Short bond.\n",
    "- Portfolio Exposure E = [v_L1 - v_S1, v_L2 - v_S2).\n",
    "- We solve for quantities q7 and q10 to neutralize both PC1 and PC2 exposures."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "import sys, os\n",
    "import numpy as np\n",
    "import pandas as pd\n",
    "import matplotlib.pyplot as plt\n",
    "from scipy.interpolate import interp1d\n",
    "from sklearn.decomposition import PCA\n",
    "from datetime import datetime\n",
    "\n",
    "project_root = os.path.abspath(os.path.join(os.getcwd(), '../..'))\n",
    "if project_root not in sys.path: sys.path.insert(0, project_root)\n",
    "from data.utils.database_manager import DatabaseManager\n",
    "from notebooks.pca.backtest_logic_double_hedged import PCAStrategyBacktestDoubleHedged, fetch_all_data\n",
    "\n",
    "plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'Hiragino Sans', 'Yu Gothic', 'Meirio']\n",
    "plt.rcParams['axes.unicode_minus'] = False\n",
    "\n",
    "db_manager = DatabaseManager()\n",
    "df_all = fetch_all_data(db_manager, start_date='2023-01-01')\n",
    "\n",
    "if not df_all.empty:\n",
    "    backtester = PCAStrategyBacktestDoubleHedged(df_all, window=50)\n",
    "    results_df = backtester.run()\n",
    "    \n",
    "    results_df['cum_no_hedge'] = results_df['no_hedge_pnl'].cumsum()\n",
    "    results_df['cum_double_hedge'] = results_df['double_hedged_pnl'].cumsum()\n",
    "    \n",
    "    plt.figure(figsize=(14, 8))\n",
    "    plt.plot(results_df['date'], results_df['cum_no_hedge'], label='No Hedge', color='blue', alpha=0.6)\n",
    "    plt.plot(results_df['date'], results_df['cum_double_hedge'], label='PC1+PC2 Hedged (7Y+10Y)', color='red', linewidth=2)\n",
    "    plt.title('Performance Comparison: No Hedge vs PC1 & PC2 Double Hedge')\n",
    "    plt.ylabel('Cumulative P&L (bp)')\n",
    "    plt.legend(); plt.grid(True, alpha=0.3); plt.show()\n",
    "    \n",
    "    print(f'Final Return (No Hedge): {results_df["cum_no_hedge"].iloc[-1]:.2f} bp')\n",
    "    print(f'Final Return (Double Hedge): {results_df["cum_double_hedge"].iloc[-1]:.2f} bp')\n",
    "else:\n",
    "    print('Data not found.')"
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
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
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

with open(notebook_path, 'w', encoding='utf-8') as f:
    json.dump(notebook, f, indent=1, ensure_ascii=False)
