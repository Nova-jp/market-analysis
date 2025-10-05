# 国債金利分析システム (Market Analytics System)

## 🎯 プロジェクト目的・目標

### ビジョン
日本国債市場の動向を高度に分析し、金利リスク管理と投資判断を支援する包括的なデータ分析プラットフォームを構築する。

### 主要目標
1. **データ収集**: JSDAから安全・確実にヒストリカルデータを収集
2. **自動データ更新**: Cloud Run上で毎日18時自動データ収集（完了）
3. **データ分析**: イールドカーブ分析・主成分分析等の高度な分析機能
4. **可視化**: 直感的で操作性の高いWebアプリケーション

## 🚨 絶対遵守ルール（開発制約）

### **JSDA サーバー保護ルール**
> **警告**: 違反は即座にプロジェクト停止の対象

1. **アクセス間隔**: 必ず5秒以上（推奨30秒以上）
2. **バックグラウンド実行禁止**: `&`, `nohup` での実行禁止
3. **プロセス監視必須**: データ収集前に `ps aux | grep python` で確認
4. **明示的停止**: 完了後は必ず Ctrl+C で停止
5. **同時実行禁止**: 複数の収集スクリプト同時実行禁止
6. **エラー時待機**: タイムアウト時5分、一般エラー時3分待機
7. **フォアグラウンド実行**: 必ずターミナルで直接実行・監視

### **安全な実行手順（必須）**
```bash
# 1. 既存プロセス確認
ps aux | grep -E "(simple_multi_day|collect_single)" | grep -v grep

# 2. 既存プロセスがあれば停止
kill [PID]

# 3. フォアグラウンドで実行
python scripts/simple_multi_day_collector.py data_files/target_dates_latest.json

# 4. 完了時は Ctrl+C で停止確認
```

### **開発ルール**
- Webアプリは直接JSDAアクセス禁止（DBアクセスのみ）
- 新規データ収集は明示的指示時のみ
- コード変更時のJSDA制約確認必須

## 🏗️ 技術アーキテクチャ

### **技術スタック**
- **言語**: Python 3.9+
- **データベース**: Supabase (PostgreSQL)
- **Webフレームワーク**: FastAPI + Jinja2
- **可視化**: Chart.js, Bootstrap
- **データ分析**: pandas, scikit-learn, numpy
- **データ収集**: requests, httpx
- **自動化**: Cloud Scheduler, Cloud Run
- **祝日判定**: jpholiday

### **プロジェクト構造**

> **重要**: このディレクトリ構造は現在のアーキテクチャの根幹であり、変更不可

```
market-analytics-ver1/
├── 📁 app/                     # ✅ Webアプリケーション (Production Ready)
│   ├── web/
│   │   └── main.py            # 🔵 統一エントリーポイント (ローカル & Cloud Run共通)
│   ├── api/                   # FastAPI エンドポイント
│   │   ├── endpoints/         # health, dates, yield_data, scheduler
│   │   └── main.py            # API専用エントリーポイント (Legacy)
│   ├── services/              # ビジネスロジック層
│   │   └── scheduler_service.py  # スケジューラーサービス
│   ├── core/                  # 設定・共通機能
│   │   ├── config.py          # 環境設定管理
│   │   ├── database.py        # データベース接続
│   │   └── models.py          # Pydanticモデル
│   └── models/                # データモデル
│
├── 📁 data/                    # ✅ 共通関数ライブラリ (Production Ready)
│   ├── processors/            # データ処理・変換
│   │   └── bond_data_processor.py  # JSDA データ処理
│   ├── utils/                 # ユーティリティ
│   │   ├── database_manager.py     # Supabase DB操作
│   │   └── jsda_parser.py          # JSDA パーサー
│   ├── collectors/            # データ収集機能
│   │   ├── bond_collector.py
│   │   └── historical_bond_collector.py
│   └── validators/            # データ検証
│
├── 📁 scripts/                 # ✅ 実行スクリプト (Production Ready)
│   ├── simple_multi_day_collector.py  # 複数日データ収集
│   ├── collect_single_day.py  # 単日データ収集
│   ├── daily_collector.py     # 日次自動収集
│   ├── run_local.py           # ローカル開発サーバー起動
│   └── run_production.py      # 本番環境サーバー起動
│
├── 📁 analysis/                # ✅ ローカル分析スクリプト (Research Ready)
│   ├── yield_curve_analyzer.py      # イールドカーブ分析
│   ├── principal_component_analysis.py  # PCA分析
│   ├── interactive_pca_analysis.py  # インタラクティブPCA分析
│   ├── simple_pca_demo.py     # PCA デモスクリプト
│   └── ml/                    # 機械学習実験
│
├── 📁 notebooks/               # 📋 分析実験用 (Future)
│   ├── exploration/           # 探索的データ分析
│   ├── modeling/              # モデリング実験
│   └── reports/               # レポート生成
│
├── 📁 templates/               # ✅ Webアプリ HTML テンプレート
│   ├── base.html              # ベーステンプレート
│   ├── dashboard.html         # ダッシュボード
│   ├── yield_curve.html       # イールドカーブ画面
│   └── pca.html               # PCA分析画面
│
├── 📁 static/                  # ✅ Webアプリ静的ファイル
│   ├── css/                   # スタイルシート
│   ├── js/                    # JavaScript
│   └── images/                # 画像アセット
│
├── 📁 data_files/              # ✅ 設定ファイル
│   └── target_dates_latest.json  # データ収集対象日付
│
├── 📁 legacy/                  # 🔄 旧実装 (非推奨)
│   └── frontend/              # Streamlitアプリ
│
└── 📁 tests/                   # 📋 テストスイート (Future)
```

### **アーキテクチャ設計原則**

#### **1. 統一エントリーポイント**
```python
# ローカル開発・本番環境統一 (Production Ready)
app/web/main.py
→ FastAPI + Jinja2 統合アプリケーション
→ Webアプリ + API + スケジューラー機能を統合
→ ローカル: python -m app.web.main
→ Cloud Run: uvicorn app.web.main:app (Dockerfileで起動)

# スケジューラーエンドポイント (app/web/main.py に統合済み)
→ Cloud Scheduler → POST /api/scheduler/daily-collection
→ GET /api/scheduler/status
```

#### **2. 機能分離の3層構造**
```
【プレゼンテーション層】
app/ + templates/ + static/
→ Webアプリケーション（ローカル & Cloud Run共通）
→ DBアクセスのみ (JSDA直接アクセス禁止)
→ FastAPI + Jinja2 統合

【共通関数ライブラリ層】
data/
→ 再利用可能なコア関数群
→ データ収集・処理・DB操作の根本的機能
→ app/, scripts/, analysis/ から呼び出される
→ processors/ : データ処理
→ utils/ : データベース操作
→ collectors/ : データ収集

【実行・分析層】
scripts/ : バッチ処理・運用スクリプト
analysis/ : ローカル分析スクリプト
notebooks/ : Jupyter Notebook実験（将来）
```

#### **3. データ収集の実行方法**
```bash
# ローカル手動収集 (開発・テスト)
python scripts/simple_multi_day_collector.py data_files/target_dates_latest.json

# 本番自動収集 (Cloud Run + Cloud Scheduler)
Cloud Scheduler → app/web/main.py (POST /api/scheduler/daily-collection)
→ app/services/scheduler_service.py を使用
→ 内部で data/processors/bond_data_processor.py を呼び出し
→ data/utils/database_manager.py でDB保存
→ 毎日18:00自動実行
```

## ✅ 完成機能一覧

### **1. データ収集システム (Production Ready)**
- **単日収集**: `scripts/collect_single_day.py`
- **複数日収集**: `scripts/simple_multi_day_collector.py`
- **JSDA保護機能**: タイムアウト検出、自動リトライ、適切な間隔制御
- **データ処理**: J列 (interest_payment_date) MM/DD形式対応
- **エラーハンドリング**: 詳細ログ、プロセス監視、安全停止

### **2. Webアプリケーション (Production Ready)**
```bash
# 推奨: 統合Webアプリ (ローカル & 本番共通)
python -m app.web.main
# → http://127.0.0.1:8000

# または uvicorn経由
uvicorn app.web.main:app --reload --host 0.0.0.0 --port 8000
```

**主要機能:**
- 📊 **ダッシュボード**: インタラクティブなイールドカーブ表示
- 📈 **時系列ナビゲーション**: 日付選択による過去データ閲覧
- 🔄 **リアルタイム更新**: Chart.js による動的グラフ更新
- 📱 **レスポンシブ対応**: Bootstrap ベースのモバイル対応UI

**API エンドポイント:**
- `GET /api/dates` - 利用可能日付一覧
- `GET /api/yield-curve/{date}` - 指定日のイールドカーブデータ
- `GET /api/compare` - 複数日比較データ
- `GET /health` - ヘルスチェック

### **3. データ分析エンジン (Core Complete)**
- **イールドカーブ分析**: `analysis/yield_curve_analyzer.py`
- **期間別分類**: 短期・中期・長期債の自動分類
- **統計分析**: 基本統計量、トレンド分析
- **多項式フィッティング**: 曲線の平滑化と補間

### **4. データベース管理 (Production Ready)**
- **Supabase統合**: PostgreSQL クラウドDB
- **スキーマ管理**: bond_data テーブル (123,000+ レコード)
- **データ整合性**: 重複排除、型検証
- **バッチ処理**: 高効率な一括挿入・更新

### **5. 自動データ収集システム (Production Ready)**
```bash
# Cloud Scheduler → Cloud Run → JSDA → Supabase DB
# 毎日18:00 (JST) 自動実行
```

**主要機能:**
- 🕰️ **スケジュール実行**: Cloud Scheduler による毎日18時自動実行
- 📅 **祝日判定**: jpholiday による日本の祝日・土日自動スキップ
- 🏢 **Cloud Run統合**: サーバーレス環境での安定実行
- 🔒 **セキュリティ**: Cloud Schedulerからのリクエストのみ受付
- ⚡ **エラーハンドリング**: タイムアウト・リトライ・安全チェック完備
- 📊 **ログ管理**: 詳細な実行ログと結果追跡

**API エンドポイント:**
- `POST /api/scheduler/daily-collection` - 日次データ収集実行（Cloud Scheduler専用）
- `GET /api/scheduler/status` - スケジューラー状態確認

**Cloud Scheduler設定:**
- **ジョブ名**: `daily-data-collection`
- **実行時刻**: 毎日18:00 (Asia/Tokyo)
- **対象URL**: `https://market-analytics-646409283435.asia-northeast1.run.app`
- **リトライ**: 自動リトライ機能付き

## 🗓️ 開発ロードマップ

### **Epic 1: データ収集・基盤システム** ✅ **完了**
#### Feature 1.1: JSDA データ収集システム ✅
- Story 1.1.1: 単日データ収集機能 ✅
- Story 1.1.2: 複数日データ収集機能 ✅
- Story 1.1.3: JSDAサーバー保護機能 ✅
- Story 1.1.4: エラーハンドリング・リトライ機能 ✅

#### Feature 1.2: データベース設計・管理 ✅
- Story 1.2.1: Supabase プロジェクト構築 ✅
- Story 1.2.2: テーブルスキーマ設計 ✅
- Story 1.2.3: データ挿入・更新システム ✅
- Story 1.2.4: J列形式変更対応 (MM/DD) ✅

### **Epic 2: Webアプリケーション基盤** ✅ **完了**
#### Feature 2.1: FastAPI + Jinja2 基盤 ✅
- Story 2.1.1: FastAPI アプリケーション構築 ✅
- Story 2.1.2: Jinja2 テンプレートエンジン統合 ✅
- Story 2.1.3: Bootstrap UI フレームワーク ✅
- Story 2.1.4: 静的ファイル配信システム ✅

#### Feature 2.2: API エンドポイント ✅
- Story 2.2.1: 日付一覧取得 API ✅
- Story 2.2.2: イールドカーブデータ API ✅
- Story 2.2.3: データ比較 API ✅
- Story 2.2.4: ヘルスチェック API ✅

### **Epic 3: 可視化・UI システム** ✅ **完了**
#### Feature 3.1: イールドカーブ可視化 ✅
- Story 3.1.1: Chart.js 統合 ✅
- Story 3.1.2: インタラクティブグラフ ✅
- Story 3.1.3: 時系列ナビゲーション ✅
- Story 3.1.4: レスポンシブデザイン ✅

### **Epic 4: 高度分析機能** 🔄 **進行中**
#### Feature 4.1: イールドカーブ分析 ✅
- Story 4.1.1: 基本統計分析 ✅
- Story 4.1.2: 期間別分類システム ✅
- Story 4.1.3: トレンド分析 ✅
- Story 4.1.4: 多項式フィッティング ✅

#### Feature 4.2: 主成分分析 ✅ **ローカル完成** / 📋 **Web統合計画中**
- Story 4.2.1: PCA アルゴリズム実装 ✅
  - Task: sklearn PCA モジュール統合 ✅
  - Task: 次元削減パラメータ調整 ✅
  - Task: 固有ベクトル解釈システム ✅
  - ✅ 実装済み: `analysis/principal_component_analysis.py`
  - ✅ デモ実装: `analysis/simple_pca_demo.py`
  - ✅ インタラクティブ版: `analysis/interactive_pca_analysis.py`
- Story 4.2.2: 主成分可視化 ✅ (ローカル)
  - Task: 主成分寄与率グラフ ✅
  - Task: 固有ベクトル表示 ✅
  - Task: 時系列主成分変化 ✅
- Story 4.2.3: Web UI 統合 📋 (計画中)
  - Task: PCA結果をDBから取得するAPIエンドポイント
  - Task: `templates/pca.html` の実装強化
  - Task: インタラクティブグラフ統合 (Chart.js)
  - Task: レポート出力機能

#### Feature 4.3: 予測・機械学習 📋 **将来計画**
- Story 4.3.1: 時系列予測モデル
- Story 4.3.2: 異常検知システム
- Story 4.3.3: リスク指標計算

### **Epic 5: 運用・自動化** ✅ **完了**
#### Feature 5.1: 自動データ更新 ✅
- Story 5.1.1: 日次バッチ処理 ✅
  - Task: Cloud Scheduler設定 ✅
  - Task: 祝日判定システム ✅
  - Task: エラーハンドリング・リトライ ✅
- Story 5.1.2: 自動品質チェック ✅
  - Task: データ収集前安全確認 ✅
  - Task: プロセス監視機能 ✅
- Story 5.1.3: ログ・監視システム ✅
  - Task: 詳細実行ログ ✅
  - Task: Cloud Logging統合 ✅

#### Feature 5.2: 本番環境デプロイ ✅
- Story 5.2.1: クラウド環境構築 ✅
  - Task: Cloud Run サービス構築 ✅
  - Task: カスタムドメイン設定 ✅
  - Task: SSL証明書設定 ✅
- Story 5.2.2: 運用監視システム ✅
  - Task: ヘルスチェックAPI ✅
  - Task: Cloud Logging統合 ✅

## 🔧 開発コマンド

### **環境構築**
```bash
pip install -r requirements.txt
cp .env.example .env  # 環境変数を設定
```

### **アプリケーション起動**
```bash
# 推奨: 統合Webアプリ (ローカル & 本番共通)
python -m app.web.main
# → http://127.0.0.1:8000

# または uvicorn経由 (開発時)
uvicorn app.web.main:app --reload --host 0.0.0.0 --port 8000

# ローカル分析スクリプト実行
python analysis/simple_pca_demo.py           # PCA分析デモ
python analysis/yield_curve_analyzer.py       # イールドカーブ分析

# Legacy
streamlit run legacy/frontend/streamlit_app.py  # (非推奨)
```

### **データ収集 (要注意: JSDA保護ルール遵守)**
```bash
# 手動データ収集（開発・テスト用）
# 必須: プロセス確認
ps aux | grep python

# 安全な実行
python scripts/simple_multi_day_collector.py data_files/target_dates_latest.json
```

### **自動データ収集管理**
```bash
# Cloud Scheduler ジョブ管理
gcloud scheduler jobs list --location="asia-northeast1"

# 手動実行（テスト用）
gcloud scheduler jobs run daily-data-collection --location="asia-northeast1"

# 実行ログ確認
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=market-analytics"

# スケジューラー状態確認
curl https://market-analytics-646409283435.asia-northeast1.run.app/api/scheduler/status
```

### **開発支援**
```bash
# テスト実行 (Future)
pytest tests/

# コードフォーマット (Future)
black webapp/ data/ analysis/
flake8 webapp/ data/ analysis/
```

## 📊 データソース

### **JSDA (日本証券業協会)**
- **URL**: https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/
- **形式**: CSV/Excel (自動CSV選択)
- **更新**: 毎日17:30頃 (一部18:30)
- **履歴**: 2002年からのデータ利用可能
- **制約**: アクセス間隔5秒以上必須

### **データベース現状**
- **レコード数**: 123,000+ 件
- **対象期間**: 2002年〜現在
- **主要フィールド**: 
  - trade_date (取引日)
  - interest_payment_date (利払日: MM/DD形式)
  - yield_rate (利回り)
  - maturity_years (残存年数)

## ⚙️ 環境変数設定

`.env`ファイルに以下を設定:
```env
# Supabase 設定
SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_anon_key

# アプリケーション設定
DEBUG=True
LOG_LEVEL=INFO
```

## 📈 プロジェクト状況

### **完成度**
- **データ収集**: 100% (Production Ready)
- **自動データ更新**: 100% (Production Ready)
- **Webアプリ基盤**: 100% (Production Ready)
- **基本分析**: 100% (イールドカーブ)
- **高度分析 (ローカル)**: 80% (PCA分析実装済み)
- **高度分析 (Web統合)**: 10% (PCA Web UI計画中)
- **運用・監視**: 100% (Production Ready)

### **次のマイルストーン**
1. **PCA分析Web統合** (Epic 4, Feature 4.2.3)
   - 優先度: 高
   - 期間: 1-2週間
   - 内容: ローカルPCA分析をWebアプリに統合
   - 成果物:
     - `app/api/endpoints/pca.py` - PCA APIエンドポイント
     - `templates/pca.html` の強化
     - Chart.js による主成分可視化

2. **予測モデル検討** (Epic 4, Feature 4.3)
   - 優先度: 中
   - 期間: 4-6週間
   - 内容: 時系列予測・異常検知モデル
   - 担当: ML パイプライン構築

### **技術的負債**
- Frontend Streamlit コード整理 (優先度: 低)
- テストスイート未実装 (優先度: 中)
- エラー処理の標準化 (優先度: 低)

---

## 📝 最終更新
- **更新日**: 2025-10-05
- **バージョン**: v3.0
- **更新者**: Claude Code Assistant
- **変更内容**:
  - `src/`ディレクトリの削除（`data/`に統一）
  - `deployments/`ディレクトリの削除（Dead Code除去）
  - エントリーポイントを`app/web/main.py`に統一
  - プロジェクト構造を3層アーキテクチャに明確化
    - プレゼンテーション層: `app/`
    - 共通関数ライブラリ層: `data/`
    - 実行・分析層: `scripts/`, `analysis/`, `notebooks/`
  - 依存関係の整理と最適化