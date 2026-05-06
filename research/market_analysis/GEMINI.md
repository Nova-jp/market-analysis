# 市場分析プロジェクト

## 目的

国債市場の市中残存額・スワップカーブ等の市場データ分析。

## ディレクトリ構成

```
market_analysis/
├── GEMINI.md
├── notebooks/
│   ├── bond_market_amount_checker.ipynb
│   ├── market_amount_check.ipynb
│   ├── market_amount_timeseries_investigation.ipynb
│   ├── market_outstanding_analysis.ipynb
│   ├── maturity_distribution_analysis.ipynb
│   └── swap_curve_analysis.ipynb
└── outputs/
```

## データベース接続

```python
import sys
sys.path.insert(0, '../../')
from data.utils.database_manager import DatabaseManager
```
