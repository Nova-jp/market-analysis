# GBDT分析プロジェクト

## 目的

日本国債の**スポットレート予測**を GBDTモデルで実装する。
分析結果が良ければ `app/` のWebアプリに統合される。

## ディレクトリ構成

```
gbdt/
├── GEMINI.md         # このファイル
├── designs/          # 設計書（Claudeが作成、Geminiはここを読んで実装する）
│   └── spot_rate_7y_base.md            # 7Y以下テナー・ベースラインモデル設計書
├── notebooks/        # 実験用Jupyter Notebook（設計書に対応）
│   ├── spot_rate_7y_base.ipynb         # 7Y以下ベースライン（要実装）
│   ├── spot_rate_prediction_v1.ipynb   # 旧バージョン（参考用）
│   └── imputation_visualization.ipynb  # 欠損値補完の可視化（参考用）
├── scripts/          # 再利用可能なPythonスクリプト
│   ├── gbdt_rate_predictor.py          # GBDTモデル基本実装
│   └── advanced_gbdt_predictor.py      # 高度なGBDTモデル
└── outputs/          # 分析出力（自動生成、Gitに含めない）
```

## データソース

- **国債利回り**: Neon DB `bond_data` テーブル（`data/utils/database_manager.py` 経由）
- **OIS/TONA**: Neon DB `irs_data` テーブル
- **マクロ指標**: Neon DB `economic_indicators` テーブル
- **外部データ（Excel）**: `data/raw/` 以下のExcelファイル

## データベース接続

```python
# research/ では同期版DatabaseManagerを使用
import sys
sys.path.insert(0, '../../')  # プロジェクトルートをパスに追加
from data.utils.database_manager import DatabaseManager

db = DatabaseManager()
df = db.get_bond_data(...)
```

## 設計書

`research/gbdt/designs/` に設計書あり。実装前に必ず参照すること。
各設計書が対応するノートブックを明示しているので、ファイル名を確認して実装すること。

| 設計書ファイル | 対応ノートブック | 内容 |
|-------------|--------------|------|
| `designs/spot_rate_7y_base.md` | `notebooks/spot_rate_7y_base.ipynb` | 7Y以下テナー・ベースラインモデル |
| `designs/spot_rate_7y_compare.md` | `notebooks/spot_rate_7y_compare.ipynb` | 特徴量3種（Config A/B/C）比較実験 |
| `designs/spot_rate_bf_base.md` | `notebooks/spot_rate_bf_base.ipynb` | ガイド準拠バタフライ予測モデル（Config A/B比較） |

## 開発方針

- DBへの書き込み禁止（読み取り専用）
- モデルのpickleファイルは `outputs/` に保存
- 実験ごとにNotebookを作成、スクリプトは再利用可能な形で整理
