# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

# 国債金利分析システム (Market Analytics System)

## 🎯 プロジェクト概要

日本国債市場の動向を分析し、イールドカーブ分析・主成分分析・投資リスク管理を支援する包括的なデータ分析プラットフォーム。

**本番環境状況**: Cloud Run上でWebアプリケーションと自動データ収集システムが稼働中。毎日18:00 (JST) に自動データ更新。

## 🚨 絶対遵守ルール（開発制約）

### 開発スタンス & ベストプラクティス（重要）

**ユーザーはWebアプリ開発の専門家ではないことを前提とし、常に「業界標準」や「ベストプラクティス」を提案・採用すること。**

1.  **能動的な提案**: ユーザーの指示が標準的でない場合、そのまま実装せず「一般的には〜という手法が使われますが、いかがなさいましょう？」とより良い代替案を提示する。
2.  **アーキテクチャの標準化**:
    *   **DBアクセス**: Supabase REST API依存を廃止し、**SQLAlchemy (Async)** を使用した標準的なORM/Repositoryパターンを採用する。
    *   **構成**: FastAPI + Pydantic + SQLAlchemy のモダンな構成（Modern Python Stack）に準拠する。
3.  **コード品質**: 型ヒント（Type Hints）、Linter（Ruff/Black）、テスト（Pytest）を重視し、堅牢なコードを書く。

### JSDA サーバー保護ルール（必須）

**警告**: 違反は即座にプロジェクト停止の対象

1. **アクセス間隔**: 必ず5秒以上（推奨30秒以上）
2. **バックグラウンド実行禁止**: `&`, `nohup` での実行は絶対禁止
3. **プロセス監視必須**: データ収集前に `ps aux | grep python` で確認
4. **明示的停止**: 完了後は必ず Ctrl+C で停止
5. **同時実行禁止**: 複数の収集スクリプト同時実行禁止
6. **エラー時待機**: タイムアウト時5分、一般エラー時3分待機
7. **フォアグラウンド実行**: 必ずターミナルで直接実行・監視

**安全な実行手順**:
```bash
# 1. 既存プロセス確認
ps aux | grep -E "(simple_multi_day|collect_single)" | grep -v grep

# 2. 既存プロセスがあれば停止
kill [PID]

# 3. フォアグラウンドで実行（絶対にバックグラウンド実行しない）
source venv/bin/activate && python scripts/simple_multi_day_collector.py data_files/target_dates_latest.json

# 4. 完了時は Ctrl+C で停止確認
```

### 開発ルール

- Webアプリは直接JSDAアクセス禁止（DBアクセスのみ）
- 新規データ収集は明示的指示時のみ
- コード変更時のJSDA制約確認必須

## 🔍 Claude Code 動作最適化ルール

### コンテキスト使用量最適化（必須）

**警告**: このプロジェクトは大規模なため、コンテキスト使用量の最適化が必須

#### 1. ファイル読み込み制限

**必要最小限の原則**: タスクに直接関係するファイルのみ読み込む

- **大容量ファイル警告**: 500行以上のファイルを読み込む前に必要性を再確認
- **段階的探索**: いきなり複数ファイルを読まず、1-2ファイルから開始

**正しいアプローチ:**
```bash
# ❌ 悪い例: 無差別に複数ファイルを読み込む
Read app/api/endpoints/market_amount.py
Read app/api/endpoints/yield_data.py
Read app/api/endpoints/dates.py
Read app/core/models.py
...（10ファイル同時読み込み）

# ✅ 良い例: 段階的に必要なファイルのみ
1. Glob "app/api/endpoints/*.py"  # まず存在確認
2. Read app/api/endpoints/market_amount.py  # 該当ファイルのみ
3. 必要に応じて関連ファイルを追加読み込み
```

#### 2. 探索的タスクでのツール選択

**Task agent優先使用（探索的タスク）:**
- 複数ファイルにまたがる調査が必要な場合
- コードパターンの検索が必要な場合
- 依存関係の追跡が必要な場合

**Glob/Grep優先使用（ファイル読み込み前）:**
- ファイル読み込み前に必ず対象を絞り込む
- パターンマッチングで関連ファイルを特定
- 大量のファイルから特定のコードを検索

**例: データベースアクセスコードを探す場合**
```bash
# ❌ 悪い例: 推測で複数ファイルを読み込む
Read app/core/database.py
Read data/utils/database_manager.py
Read app/services/scheduler_service.py
...

# ✅ 良い例: 検索で絞り込んでから読み込む
1. Grep "execute_query|get_bond_data" output_mode:"files_with_matches"
2. 結果に基づいて必要なファイルのみ Read
```

#### 3. 除外すべきディレクトリ・ファイル

**常に除外（読み込み・探索禁止）:**
- `venv/` - 仮想環境（785 MB）
- `__pycache__/` - Pythonキャッシュファイル
- `*.pyc` - コンパイル済みPythonファイル
- `*.log` - ログファイル
- `.ipynb_checkpoints/` - Jupyter Notebookチェックポイント
- `.git/` - Gitリポジトリデータ

**参照のみ（過度な読み込み禁止）:**
- `legacy/` - 旧実装（非推奨、参照時のみ）
- `static/` - 静的ファイル（必要時のみ）
- `templates/` - HTMLテンプレート（必要時のみ）

**注意: notebooks/ と analysis/ は除外しない:**
- ユーザーがデータ実験で頻繁に使用するため、通常通りアクセス可能

#### 4. 効率的なファイル探索パターン

**パターン1: 新機能追加時**
```bash
# ステップ1: 既存の類似機能を探す
Glob "app/api/endpoints/*.py"  # エンドポイント一覧確認
Grep "class.*Endpoint|@router" output_mode:"files_with_matches" path:"app/api/endpoints"

# ステップ2: 1-2ファイルのみ読み込んで実装パターンを理解
Read app/api/endpoints/yield_data.py  # 参考実装1つのみ

# ステップ3: 必要に応じて関連ファイルを追加読み込み
Read app/core/models.py  # モデル定義が必要な場合のみ
```

**パターン2: バグ修正時**
```bash
# ステップ1: エラーメッセージから対象ファイルを特定
Grep "error_message_keyword" output_mode:"files_with_matches"

# ステップ2: 該当ファイルのみ読み込む
Read /path/to/identified/file.py

# ステップ3: 依存関係を段階的に追跡
LSP goToDefinition  # 関数定義を確認
```

**パターン3: データベーススキーマ確認時**
```bash
# ✅ 良い例: 特定テーブルのみ検索
Glob "scripts/sql/schema/*.sql"
Read scripts/sql/schema/create_bond_table.sql  # 該当テーブルのみ

# ❌ 悪い例: すべてのSQLファイルを読み込む
Read scripts/sql/schema/create_bond_table.sql
Read scripts/sql/schema/create_boj_holdings_table.sql
...
```

#### 5. コンテキスト使用量モニタリング

**自己チェックポイント:**
- ファイル読み込み前: 「このファイルは本当に必要か？」
- 大容量ファイル読み込み時: 「部分読み込み（offset/limit）で十分か？」
- 複数ファイル読み込み時: 「並列読み込みが適切か、段階的に読むべきか？」

**期待されるコンテキスト削減効果:**
- venv/ 除外: 約80% 削減
- 段階的読み込み: 約10-15% 削減
- Glob/Grep活用: 約5-10% 削減

#### 6. 長時間実行コマンドの取り扱い

**警告**: 大量出力を伴う長時間実行コマンドはコンテキストを大幅に消費

**基本方針: バックグラウンド自動実行を避ける**

長時間実行が予想されるコマンド（5分以上、または大量の出力が予想される）は、**バックグラウンドで自動実行せず**、ユーザーに別ターミナルでの実行を提案すること。

**長時間実行コマンドの例:**
- データベース操作: `psql`による大量データインポート/エクスポート
- データ処理: 数年分のbond_dataのバッチ処理
- データ収集: 複数日分のJSDAデータ収集
- ビルド・テスト: 長時間かかるビルドや統合テスト

**正しいアプローチ:**
```
❌ 悪い例: バックグラウンドで自動実行
Bash(command="python scripts/export_bond_data_by_year.py", run_in_background=true)
→ 結果: 大量の出力がコンテキストを消費

✅ 良い例: 別ターミナルでの実行を提案
「このコマンドは大量の出力が予想されるため、以下を別のターミナルで実行することをお勧めします:

```bash
source venv/bin/activate
python scripts/export_bond_data_by_year.py
```

実行が完了したらお知らせください。」
```

**判断基準:**
- 予想実行時間が5分以上
- 出力行数が1000行以上見込まれる
- ループ処理で進捗を逐次出力する
- データベースの大量レコード処理

**例外（バックグラウンド実行可能）:**
- 状態確認コマンド（`git status`, `ps aux`）
- 短時間テスト（`pytest`単一ファイル）
- 軽量ビルド（`tsc`など）

### 自動承認ルール（リスクベース分類）

このセクションでは、確認頻度とコード品質のバランスを取るための自動承認ポリシーを定義します。

#### 基本原則

1. **Plan-First Approach**: 中規模以上のタスクは必ずEnterPlanModeで計画を提示
2. **Read-Heavy, Write-Light**: 読み取り操作は積極的に、書き込みは慎重に
3. **Fail-Safe Design**: 不確実な場合は必ず確認を求める

#### ✅ 自動承認可能（Low Risk）

**読み取り専用操作**:
- ファイル読み込み: `Read`, `Glob`, `Grep`
- データベース読み込み: `SELECT` クエリのみ
- ログ確認: `gcloud logging read`
- 状態確認: `git status`, `ps aux`, `gcloud ... list/describe`
- Web検索・フェッチ: `WebSearch`, `WebFetch`

**非破壊的な開発操作**:
- ブランチ作成: `git checkout -b feature/*`
- 依存関係インストール: `pip install` (venv内)
- ローカルテスト実行: `pytest`, `python -m unittest`
- ローカルサーバー起動: `uvicorn`（ポート8000-9000）

**制限付き書き込み**:
- 新規ファイル作成: `scripts/`, `analysis/`, `notebooks/` 内のみ
- ドキュメント更新: `*.md` ファイル（CLAUDE.md除く）
- テストファイル: `test_*.py`, `*_test.py`

#### ⚠️ 条件付き承認（Medium Risk）

**承認条件: EnterPlanMode で事前計画承認済み**

- 既存コード修正: `app/`, `data/` ディレクトリ内
- データベーススキーマ変更: `scripts/sql/schema/` 内
- 設定ファイル変更: `requirements.txt`, `.env.example`
- Gitコミット: 変更内容を提示後

#### 🛑 必須承認（High Risk）

**常に明示的承認が必要**:

**本番環境操作**:
- Cloud Runデプロイ: `gcloud run deploy`
- Cloud Schedulerジョブ変更: `gcloud scheduler jobs update/create`
- 本番環境変数変更: `gcloud run services update --set-env-vars`
- Git push: `git push origin main`

**データ操作**:
- JSDAデータ収集実行: `python scripts/*_collector.py`
- データベース削除・更新: `DELETE`, `UPDATE`, `DROP`
- データベースRPC変更: `scripts/sql/create_*_rpc.sql` 実行

**破壊的変更**:
- ファイル・ディレクトリ削除: `rm`, `git rm`
- Git履歴変更: `git rebase`, `git reset --hard`
- アーキテクチャ変更: 3層構造に影響する変更
- CLAUDE.md修正: プロジェクト指針の変更

**セキュリティ影響**:
- `.env` ファイル操作
- RLSポリシー変更
- 認証・認可ロジック変更

#### 実践的ワークフロー例

**シナリオA: 新機能追加（中〜大規模）**
```
1. [自動] EnterPlanMode で調査・設計
2. [手動承認] 実装計画を確認
3. [自動] コード実装（複数ファイル編集）
4. [自動] テスト実行
5. [手動承認] Gitコミット内容確認
6. [手動承認] デプロイ実行
```

**シナリオB: バグ修正（小規模）**
```
1. [自動] ファイル検索・読み込み
2. [自動] 原因特定
3. [自動] 修正実装（1-2ファイル）
4. [自動] テスト実行
5. [手動承認] Gitコミット
```

**シナリオC: データ分析実験**
```
1. [自動] データベースからデータ取得
2. [自動] notebooks/ に分析スクリプト作成
3. [自動] 分析実行
4. [自動] 結果の可視化
（承認不要 - 読み取り専用）
```

#### 自動実行フロー

**条件を満たす場合、確認なしで実行可能**:
- 上記「自動承認可能（Low Risk）」に該当
- EnterPlanMode承認済みタスクの実装フェーズ
- 失敗してもrollback可能な操作

**確認を求めるべきタイミング**:
- 本番環境に影響
- データ削除・更新
- アーキテクチャ変更
- セキュリティに影響
- JSDAサーバーアクセス

#### 実装時の自己チェック

各操作前に以下を確認:
1. この操作はリスク分類のどれに該当？
2. EnterPlanModeで承認済み？
3. 失敗時のrollback方法は？
4. 本番環境に影響する？

### 🐍 Python 3.13 開発制約
- **依存関係**: SQLAlchemy AsyncIO を使用するため、`greenlet` ライブラリが必須。
- **SSL**: Neon への非同期接続時は `connect_args={"ssl": "require"}` を明示的に指定。

### 📈 分析ロジック (Yield / Swap / ASW)
- **API分割**: `yield-data` (国債), `swap-data` (OIS), `asw-data` (ASW) は独立したエンドポイントとして提供。
- **ASW算出**: `asw-data` エンドポイントで `scipy.interpolate.CubicSpline` (natural) を用いてスワップレートを動的に補間し、ASW = 国債利回り - 補間スワップレート を算出。
- **閲覧制限**: スワップおよびASWカーブはフロントエンドで簡易パスワード保護。
  - **Password**: `0720` (sessionStorage `swapUnlocked` で状態保持)

## 🏗️ アーキテクチャ

### 技術スタック

- **言語**: Python 3.9+
- **データベース**: Neon (PostgreSQL) - Serverless Postgres
- **Webフレームワーク**: FastAPI + Jinja2
- **可視化**: Chart.js, Bootstrap
- **データ分析**: pandas, scikit-learn, numpy
- **データ収集**: requests, httpx
- **自動化**: Cloud Scheduler, Cloud Run
- **祝日判定**: jpholiday

### 3層アーキテクチャ

**重要**: このディレクトリ構造は現在のアーキテクチャの根幹 - 変更不可

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
→ processors/: データ処理
→ utils/: データベース操作
→ collectors/: データ収集

【実行・分析層】
scripts/: バッチ処理・運用スクリプト
analysis/: ローカル分析スクリプト
notebooks/: Jupyter Notebook実験（将来）
```

### 統一エントリーポイント

**本番環境 & ローカル開発**: `app/web/main.py`
- FastAPI + Jinja2 統合アプリケーション
- Web UI + API + スケジューラー機能を統合
- ローカル: `uvicorn app.web.main:app --reload --host 0.0.0.0 --port 8000`
- Cloud Run: `uvicorn app.web.main:app` (Dockerfileで起動)

### データ収集フロー

**ローカル手動収集**（開発・テスト用）:
```bash
source venv/bin/activate
python scripts/simple_multi_day_collector.py data_files/target_dates_latest.json
```

**本番自動収集**（Cloud Run + Cloud Scheduler）:
```
Cloud Scheduler (毎日18:00 JST)
  ↓
POST /api/scheduler/daily-collection
  ↓
app/services/scheduler_service.py
  ↓
data/processors/bond_data_processor.py
  ↓
data/utils/database_manager.py → Neon DB
```

## 🔧 開発コマンド

### 環境構築

```bash
# 仮想環境の作成と有効化（必須）
python3 -m venv venv
source venv/bin/activate

# 依存関係のインストール
pip install -r requirements.txt

# 環境変数を設定
cp .env.example .env
# .env ファイルを編集してNeon接続情報を入力
```

### 仮想環境ルール（必須）

**重要**: Pythonコードの実行は必ず仮想環境内で行うこと

1. 実行前に有効化: `source venv/bin/activate`
2. グローバル環境へのpip install禁止 - venv内でのみ実行
3. Claude Codeは必ずプレフィックス: `source venv/bin/activate && python3 script.py`

```bash
# ✅ 正しい実行方法
source venv/bin/activate && python3 scripts/collect_single_day.py

# ❌ 間違った実行方法（グローバル環境を汚染する）
python3 scripts/collect_single_day.py
```

### アプリケーション起動

```bash
# Webアプリ起動（統一エントリーポイント）
source venv/bin/activate
uvicorn app.web.main:app --reload --host 0.0.0.0 --port 8000
# → http://127.0.0.1:8000

# ローカル分析スクリプト実行
source venv/bin/activate
python analysis/simple_pca_demo.py              # PCA分析デモ
python analysis/yield_curve_analyzer.py         # イールドカーブ分析
```

### データ収集（JSDA保護ルール適用）

```bash
# 手動データ収集（開発・テスト用のみ）
# 必須: プロセス確認
ps aux | grep python

# 安全な実行（& や nohup は絶対に使用しない）
source venv/bin/activate
python scripts/simple_multi_day_collector.py data_files/target_dates_latest.json
```

### Cloud Scheduler管理

```bash
# スケジューラージョブ一覧
gcloud scheduler jobs list --location="asia-northeast1"

# 手動実行（テスト用）
gcloud scheduler jobs run daily-data-collection --location="asia-northeast1"

# 実行ログ確認
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=market-analytics"

# スケジューラー状態確認
curl https://market-analytics-646409283435.asia-northeast1.run.app/api/scheduler/status
```

## ⚙️ 環境変数設定

**セキュリティ警告**:
- `.env` ファイルには機密データを含む（Git除外済み）
- `.env.example` は設定テンプレート（Git管理対象）
- 本番環境では環境変数またはセキュアな設定管理を使用

必須の `.env` 設定 (Neon/PostgreSQL):
```env
# Database 設定 (Neon)
DB_HOST=ep-xxxx.aws.neon.tech
DB_PORT=5432
DB_NAME=neondb
DB_USER=your_user
DB_PASSWORD=your_password

# アプリケーション設定
DEBUG=True
LOG_LEVEL=INFO
```

*Note: SUPABASE_* および CLOUD_SQL_* 変数は互換性のために残されていますが、新規開発では標準の DB_* 変数を使用してください。*

### データベース接続設計

**v3.6での設計変更（2026-01-04）**:
- **Neonへの完全移行**: Supabase/Cloud SQLの使用終了
- **接続方式**: PostgreSQL標準接続（psycopg2 / asyncpg）
- **構成管理**: `app/core/config.py` で一元管理

**使用箇所**:
- Webアプリ: `app/core/database.py` (Async)
- データ収集スクリプト: `data/utils/database_manager.py` (Sync)

## 📊 データソース

### JSDA（日本証券業協会）

- **URL**: https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/
- **形式**: CSV/Excel（自動CSV選択）
- **更新**: 毎日17:30頃（一部18:30）
- **履歴**: 2002年からのデータ利用可能
- **制約**: アクセス間隔5秒以上必須

### データベース現状

- **レコード数**: 123,000+件
- **対象期間**: 2002年〜現在
- **主要フィールド**:
  - `trade_date` (取引日)
  - `interest_payment_date` (利払日: MM/DD形式)
  - `yield_rate` (利回り)
  - `maturity_years` (残存年数)

## 🔑 主要ファイル & アーキテクチャ

### エントリーポイント

- **Webアプリ**: `app/web/main.py` - ローカル & Cloud Run統一エントリーポイント
- **APIエンドポイント**: `app/api/endpoints/` - FastAPIルートハンドラー
- **スケジューラーサービス**: `app/services/scheduler_service.py` - 自動収集ロジック

### コアライブラリ（再利用可能）

- **データベースマネージャー（非同期）**: `app/core/database.py` - Web API用（FastAPI async）
- **データベースマネージャー（同期）**: `data/utils/database_manager.py` - スクリプト/バッチ用
- **国債データプロセッサー**: `data/processors/bond_data_processor.py` - JSDAデータ処理
- **JSDAパーサー**: `data/utils/jsda_parser.py` - JSDAフォーマットパーサー

### データ収集スクリプト

- **複数日収集**: `scripts/simple_multi_day_collector.py` - 複数日のデータ収集
- **単日収集**: `scripts/collect_single_day.py` - 単日のデータ収集
- **重要**: 両方ともJSDAサーバー保護ルールを遵守

### 設定ファイル

- **アプリ設定**: `app/core/config.py` - pydantic-settingsによる環境設定
- **対象日付**: `data_files/target_dates_latest.json` - 収集対象日付
- **依存関係**: `requirements.txt` - 統一された依存関係（ローカル & Cloud Run）

### デプロイメント

- **Dockerfile**: Cloud Runデプロイメント設定
- **Cloud Scheduler**: 毎日自動実行（18:00 JST）
- **サービス**: Cloud Run（asia-northeast1）の `market-analytics`

## 📈 APIエンドポイント

### Webページ
- `GET /` - ホームページ
- `GET /yield-curve` - イールドカーブ比較画面
- `GET /pca` - PCA分析画面
- `GET /market-amount` - 市中残存額可視化画面

### APIルート
- `GET /health` - ヘルスチェック
- `GET /api/dates` - 利用可能な取引日一覧
- `GET /api/yield-curve/{date}` - 指定日のイールドカーブデータ
- `GET /api/compare` - 複数日比較データ
- `POST /api/scheduler/daily-collection` - データ収集実行（Cloud Schedulerのみ）
- `GET /api/scheduler/status` - スケジューラー状態確認

## 🎯 現在の状況 & ロードマップ

### 完成度

- **データ収集**: 100%（本番稼働中）
- **自動データ更新**: 100%（本番稼働中）
- **Webアプリ基盤**: 100%（本番稼働中）
- **基本分析**: 100%（イールドカーブ）
- **高度分析（ローカル）**: 80%（PCA分析実装済み）
- **高度分析（Web統合）**: 10%（PCA Web UI計画中）
- **運用・監視**: 100%（本番稼働中）

### 次のマイルストーン

**PCA分析Web統合**（優先度: 高）
- バックエンド: `app/api/endpoints/pca.py` - PCA APIエンドポイント
- フロントエンド: `templates/pca.html` の強化
- 可視化: Chart.jsによる主成分の可視化

## 🔒 セキュリティ & ベストプラクティス

### 環境変数
- `.env` ファイルには機密データを含む（Git除外済み）
- `.env.example` は設定テンプレート（Git管理対象）
- 本番環境では環境変数またはセキュアな設定管理を使用

### コード品質
- 2つのデータベースマネージャー: Web API用は非同期（`app/core/`）、スクリプト用は同期（`data/utils/`）
- 非同期/同期のデータベース操作を混在させない
- 常に仮想環境を使用
- JSDAサーバー保護ルールを厳格に遵守

### JSDAアクセス制約
- **必須間隔**: 30秒以上
- **実行モード**: フォアグラウンドのみ（バックグラウンド禁止）
- **監視**: プロセス確認とCtrl+C終了
- **エラー処理**: タイムアウト時5分待機、エラー時3分待機

## 💡 開発のヒント

### 新機能追加時

1. **コアライブラリを再利用**: まず `data/` ディレクトリで既存機能を確認
2. **アーキテクチャに従う**: 3層分離（プレゼンテーション/ライブラリ/実行）を維持
3. **データベースアクセス**: `app/` では非同期版、`scripts/` では同期版を使用
4. **JSDAルール**: データ収集に関わる場合は、JSDA保護制約を確認

### 一般的なパターン

**Web APIエンドポイント**（非同期）:
```python
from app.core.database import db_manager

async def get_data():
    result = await db_manager.execute_query("bond_data", params)
    return result
```

**スクリプト/バッチ**（同期）:
```python
from data.utils.database_manager import DatabaseManager

db = DatabaseManager()
result = db.get_bond_data(params)
```

### Cloud Runをローカルでテスト

```bash
# Dockerイメージをビルド
docker build -t market-analytics .

# コンテナを実行
docker run -p 8080:8080 \
  -e SUPABASE_URL="your_url" \
  -e SUPABASE_KEY="your_key" \
  market-analytics
```

## 📝 バージョン履歴

### v3.5 (2025-12-27)
- **更新者**: Claude Code Assistant
- **変更内容**:
  - **長時間実行コマンドの取り扱いルール追加**
    - バックグラウンド自動実行によるコンテキスト消費を防止
    - 大量出力が予想されるコマンドは別ターミナル実行を提案
    - 判断基準の明確化（実行時間5分以上、出力1000行以上）
    - データベース操作、データ処理、ビルドなどの具体例
  - **期待される効果**:
    - コンテキスト使用量: 大幅削減（長時間タスク時）
    - 会話の継続性: 向上
    - ユーザー体験: 明示的な進捗確認が可能

### v3.4 (2025-12-27)
- **更新者**: Claude Code Assistant
- **変更内容**:
  - **自動承認ルール追加（リスクベース分類）**
    - 3段階リスク分類: Low / Medium / High Risk
    - Plan-First Approach による効率的な開発フロー
    - 実践的ワークフロー例（新機能追加、バグ修正、データ分析）
    - 自動実行可能な操作と必須承認操作の明確化
  - **期待される効果**:
    - 確認回数: 約50-70%削減（読み取り・分析タスク）
    - 安全性: High Risk操作は必ず確認
    - 生産性: EnterPlanModeで一括承認後は高速実装

### v3.3 (2025-12-27)
- **更新者**: Claude Code Assistant
- **変更内容**:
  - **コンテキスト使用量最適化ルール追加**
    - ファイル読み込み制限ガイドライン
    - Task agent / Glob / Grep 活用推奨
    - 除外すべきディレクトリ・ファイルの明示
    - 効率的な探索パターンの具体例
  - **ファイルクリーンアップ**
    - logs/ ディレクトリ削除（1.1 MB）
    - data_files/archive/ ディレクトリ削除（268 KB）
    - プロジェクト内 __pycache__ ディレクトリ削除（約120 KB）
  - **notebooks/ と analysis/ は除外せず**
    - データ実験で頻繁に使用するため通常通りアクセス可能

### v3.2 (2025-12-21)
- `bond_data` と `boj_holdings` テーブルにRow Level Security (RLS)ポリシー設定
- セキュリティスクリプト: `scripts/sql/security/setup_rls_policies.sql`
- スキーマファイルにRLSポリシー追加
- ドキュメント修正

### v3.1 (2025-10-16)
- 単一Service Role Key設計への統一
- データベース接続の簡素化
- Dockerfileとデプロイメントの最適化

### v3.0 (2025-10-05)
- ディレクトリ構造の統一（`src/` → `data/`）
- 3層アーキテクチャの明確化
- エントリーポイントを `app/web/main.py` に統一
