# research/ - 分析・実験ゾーン

## このゾーンの役割

Webアプリ本番コードとは独立した研究・実験エリア。
分析で良い結果が出たロジックを `app/` に実装する「研究所」。

## ディレクトリ構成

```
research/
├── designs/           # 設計書（Claudeが作成・レビュー）
├── gbdt/              # GBDT分析プロジェクト
├── pca/               # PCA分析プロジェクト
├── market_analysis/   # 市場分析（市中残存額等）
├── finance/           # 共通金融ユーティリティ
└── [将来の分析]/       # 新規分析追加時は同じ構造で
```

## 各分析フォルダの標準構成

```
[analysis_name]/
├── GEMINI.md          # Gemini用コンテキスト（実装指示）
├── notebooks/         # Jupyter Notebook（実験）
├── scripts/           # Pythonスクリプト（再利用可能なロジック）
└── outputs/           # 分析出力（PNG, CSV, PKLなど）
```

## 役割分担

- **Claude**: 設計書（`designs/`）の作成・コードレビュー
- **Gemini**: 各フォルダの `GEMINI.md` を読んで実装・実験

## Webアプリへの昇格フロー

```
research/[analysis]/scripts/ のロジック
    → Claudeがレビュー
    → app/services/ または app/api/endpoints/ に実装
```

## 開発ルール

- `research/` 内のコードはDBへの書き込みを行わない（読み取り専用）
- 本番の `app/` へのインポートは禁止（一方通行）
- 良いロジックは `data/` 共通ライブラリ経由で本番へ
