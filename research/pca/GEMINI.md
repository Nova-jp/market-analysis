# PCA分析プロジェクト

## 目的

日本国債イールドカーブの**主成分分析（PCA）**とバックテスト。
Webアプリの `/pca` ページに統合済みの分析の実験・改善ゾーン。

## ディレクトリ構成

```
pca/
├── GEMINI.md
├── notebooks/
│   ├── bond_pca_analysis.ipynb
│   ├── tona_pca_visualization.ipynb
│   ├── yield_curve_pca.ipynb
│   ├── yield_curve_pca_100days.ipynb
│   ├── yield_curve_pca_reconstruction.ipynb
│   ├── yield_curve_pca_simple.ipynb
│   ├── backtest_strategy.ipynb
│   ├── backtest_strategy_hedged.ipynb
│   └── backtest_strategy_double_hedged.ipynb
├── scripts/
│   ├── backtest_logic.py
│   ├── backtest_logic_hedged.py
│   ├── backtest_logic_double_hedged.py
│   └── analyze_bond_code_format.py
└── outputs/
    └── *.csv  # バックテスト結果
```

## 本番実装との関係

PCAのWebアプリ実装は `app/services/pca_service.py` にあり。
ここでの実験結果をもとに本番サービスを改善する。

## データベース接続

```python
import sys
sys.path.insert(0, '../../')
from data.utils.database_manager import DatabaseManager
```
