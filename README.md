# 📊 Market Analytics - 国債金利分析システム

日本国債市場の動向を高度に分析し、金利リスク管理と投資判断を支援する包括的なデータ分析プラットフォーム

## 🎯 主要機能

### ✨ v2.0 新機能
- **📈 拡張イールドカーブ分析**: 最大20日付の同時比較
- **🎯 年限範囲フィルター**: 指定範囲（例：20-30年）での詳細分析
- **⚡ 高速営業日検索**: 最適化されたクイック選択システム
- **🎨 インタラクティブUI**: レスポンシブデザイン + 動的フィルタリング

### 🔧 技術基盤
- **🔍 普遍的API**: 拡張性を重視したデータベースアクセス設計
- **📊 効率的クエリ**: 単一リクエストによる営業日取得最適化
- **🗃️ 大規模データ**: 130万件以上の履歴データ（2007年〜）
- **🛡️ 堅牢性**: 包括的エラーハンドリングと型安全性

## ⚡ クイックスタート

### 1. 必要な環境
- Python 3.9+
- Supabase アカウント

### 2. インストール

```bash
# リポジトリクローン
git clone <your-repository-url>
cd market-analytics-ver1

# 仮想環境作成・有効化
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
# venv\Scripts\activate   # Windows

# 依存関係インストール
pip install -r requirements.txt
```

### 3. 環境設定

```bash
# 環境変数ファイル作成
cp .env.example .env

# .env ファイルを編集（必須）
nano .env
```

`.env` ファイルに以下を設定:
```env
SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_anon_key
```

### 4. アプリケーション起動

```bash
# 最新版アプリ起動（推奨）
python simple_app.py

# または従来版
cd webapp && python main.py

# ブラウザで確認
# → http://127.0.0.1:8001 (simple_app)
# → http://127.0.0.1:8000 (webapp)
```

### 🚀 即座に使える機能
- **イールドカーブ分析**: `/yield-curve`
- **API情報**: `/api/info`
- **システム状況**: `/health`

## 🏗️ プロジェクト構造

```
market-analytics-ver1/
├── 📁 webapp/                  # Webアプリケーション
│   ├── main.py                # FastAPI + Jinja2 メインアプリ
│   ├── templates/             # HTMLテンプレート
│   └── static/                # CSS, JS, assets
├── 📁 data/                    # データ関連
│   ├── collectors/            # データ収集 (JSDA)
│   ├── processors/            # データ処理・変換
│   ├── utils/                 # データベース管理
│   └── validators/            # データ検証
├── 📁 analysis/                # 分析機能
│   ├── yield_curve_analyzer.py # イールドカーブ分析
│   └── pca_analyzer.py        # 主成分分析
├── 📁 scripts/                 # バッチ処理
│   ├── simple_multi_day_collector.py # 複数日収集
│   └── collect_single_day.py  # 単日収集
└── 📁 data_files/              # 設定・データファイル
```

## 📋 主要コマンド

### Webアプリ関連
```bash
# アプリ起動
cd webapp && python main.py

# 開発サーバー起動
uvicorn webapp.main:app --reload --host 0.0.0.0 --port 8000
```

### データ収集（重要：JSDA保護ルール遵守）
```bash
# 🚨 事前確認：既存プロセスチェック
ps aux | grep python

# 利用可能日付取得
python scripts/collect_available_dates.py

# データ収集（30秒間隔、フォアグラウンド実行必須）
python scripts/simple_multi_day_collector.py data_files/target_dates_latest.json
```

## 🔐 セキュリティ

### 環境変数管理
- `.env` ファイルには機密情報を含む（Git除外済み）
- `.env.example` は設定テンプレート（Git管理対象）
- 本番環境では環境変数かセキュアな設定管理を使用

### JSDA アクセス制限
- **必須間隔**: 30秒以上
- **実行方式**: フォアグラウンド必須（バックグラウンド禁止）
- **監視**: プロセス確認とCtrl+C停止
- **エラー時**: タイムアウト5分、通常エラー3分待機

## 🎨 機能詳細

### イールドカーブ分析 v2.0
- **最大20日付同時比較**: プロフェッショナルレベルの分析
- **年限範囲フィルター**: 短期(0-2年)、中期(2-10年)、長期(10年+)の詳細分析
- **高速営業日選択**: 最新、前日、5営業日前、1ヶ月前のワンクリック選択
- **直感的UI**: ドラッグ&ドロップ対応、レスポンシブデザイン
- **データ保持**: エラー時も入力値を保持、ユーザビリティ向上

### 主成分分析
- 金利データの次元削減
- 主要成分の可視化
- 分散寄与率の分析
- スプライン補間による平滑化

### データベース
- Supabase (PostgreSQL) 
- 120,000+ レコード
- 2002年からの履歴データ
- REST API経由でのアクセス

## ⚙️ 技術スタック

- **Backend**: Python 3.9+, FastAPI
- **Frontend**: Jinja2, Bootstrap, Chart.js
- **Database**: Supabase (PostgreSQL)
- **Analysis**: pandas, scikit-learn, numpy, scipy
- **HTTP**: requests, httpx

## 📊 データソース

- **JSDA (日本証券業協会)**: https://market.jsda.or.jp/
- **更新頻度**: 毎日17:30頃
- **データ範囲**: 2002年～現在
- **形式**: CSV（自動取得）

## 🚀 開発ロードマップ

- ✅ **基盤システム**: データ収集・Webアプリ・基本分析
- ✅ **可視化**: 5パネル比較・PCA機能
- 🔄 **進行中**: 高度分析機能の拡張
- 📋 **計画**: 予測モデル・自動化・本番環境

## 🤝 貢献

1. Issue作成またはFeature Request
2. Fork & Branch作成
3. 変更実装・テスト
4. Pull Request作成

## 📄 ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## ⚠️ 免責事項

このシステムは分析・教育目的です。投資判断に使用する場合は、必ず専門家にご相談ください。データの正確性については保証いたしません。