## 🤖 エージェント行動指針 (厳守)

### 1. 提案と実行の完全な分離 (非可逆操作の制限)
- **「調査」と「実装」の区別**:
  - **調査（可逆的操作）**: `grep_search`, `read_file`, `ls`, `glob` 等の読み取り専用ツールは、状況把握のために事前の承認なく自律的に使用して良い。
  - **実装（非可逆的操作）**: `replace`, `write_file`, `run_shell_command`（書き込みや削除を伴うもの）等の操作は、必ず**事前に具体的な修正方針（プラン）を自然言語で提示し、ユーザーの明示的な承認を得てから**行うこと。
- **勝手なコーディングの禁止**:
  解決策がわかっても、方針の合意がないままコードの修正を開始してはならない。
- **承認の定義**:
  ツール実行時のシステム確認画面は「事前の承認」とはみなさない。必ず会話の中で「その内容で進めてください」等の明示的な合意を得ること。

### 2. Python実行環境の制約
- **仮想環境の強制利用**:
  Pythonスクリプトを実行する際は、システムグローバルの `python` や `python3` コマンドを**絶対に使用しない**。
- **実行パスの指定**:
  必ずプロジェクトルートの仮想環境バイナリを直接指定すること。
  - ⭕️ `./venv/bin/python script.py`
  - ❌ `python script.py`
- **理由**:
  必要なライブラリは仮想環境 (`venv`) にのみインストールされており、システムのPythonを使用すると `ModuleNotFoundError` が発生するため。

## 📡 データ収集・外部アクセス方針

### 共通ルール
- **Timezone**: 全ての日付判定は **JST (UTC+9)** を基準とする。
- **DB接続**: バッチ処理・スクリプトでは必ず同期版 `data.utils.database_manager` を使用する。

### ソース別ポリシー

#### 1. JSDA (日本証券業協会)
- **頻度**: 毎日 (平日)
- **制約**: **アクセス間隔 30秒以上厳守**。
- **手法**: `scripts/collectors/daily_collector.py` (simple_multi_day_collectorロジック)

#### 2. MOF (財務省入札)
- **頻度**: 毎日チェック (入札開催日のみデータ取得)
- **手法**: カレンダーページを確認し、当日分の入札結果があれば取得。
- **収集クラス**: `data/collectors/mof/bond_auction_web_collector.py`

#### 3. BOJ (日銀保有残高)
- **頻度**: 毎日チェック (不定期更新)
- **手法**: 年別インデックスページを確認し、DB未登録の新しいファイルがあれば取得。
- **収集クラス**: `data/collectors/boj/holdings_collector.py`

## 🗄️ データベース操作 (Neon / PostgreSQL)

### コンテキスト別マネージャー選択
- **Web API (FastAPI)**:
  - ⭕️ `app.core.database.DatabaseManager` (Async / SQLAlchemy)
  - 理由: 非同期I/Oによる高スループット維持のため。
- **Scripts / Batch**:
  - ⭕️ `data.utils.database_manager.DatabaseManager` (Sync / Psycopg2)
  - 理由: 安定性重視、単純な逐次処理のため。

## 🗄️ テーブル構成 (Database Schema)

### bond_market_amount

- **用途**: 各銘柄・各営業日ごとの市中残存額を保持。

- **主キー**: `(trade_date, bond_code)`

- **計算式**: `累積発行額 (bond_auction.total_amount の合計) - 最新の日銀保有額 (boj_holdings.face_value)`

- **注意点**: `bond_data` テーブルに `market_amount` カラムは存在しない。時系列データが必要な場合はこのテーブルを結合すること。



## 📈 金融計算・データ設計の規約



### 1. データベース設計思想

- **事実データと評価指標の分離**: 

  - `bond_data` は外部からの一次データ（事実）を保持し、変更しない。

  - `ASW_data` 等の計算結果（評価指標）は、モデル依存のため別テーブルで管理し、再計算・洗い替えを許容する。

- **ミニマリスト・スキーマ**: 

  - 派生テーブルには重複情報（銘柄名、償還日等）を持たせず、必要に応じて `trade_date` と `bond_code` で JOIN して取得する。



### 2. ASW計算仕様 (QuantLib)

- **マーケットコンベンション**:

  - デフォルト: Act/365 固定。

  - 支払頻度: `PA` (Annual) と `SA` (Semi-Annual) の両方を保存。

  - スワップ側 (Float Leg): 常に TONA Index (Act/365) を使用し、支払頻度は Fixed Leg に同期させる。

- **精度**: DB保存時の値は小数点以下4桁に丸める（表示用・容量節約のため）。

### 3. 主成分分析 (PCA) 実装仕様

#### 1. 分析アーキテクチャ (Performance Optimized)
- **計算・配信の分離**:
  - `/api/pca/analyze`: PCAモデル（固有ベクトル・スコア）の計算とキャッシュ。レスポンスは最新日の誤差データのみを含む。
  - `/api/pca/reconstruction`: キャッシュされたモデルを用いた特定日の誤差計算。フロントエンドからの要求に応じてオンデマンドで実行。
- **キャッシュ機構**:
  - `pickle` を使用して、PCAモデル、スコア行列、補間済みデータ (`daily_data`) を `.cache/pca/` に保存。
  - キャッシュキー: `pca_cache_{基準日}_{ルックバック日数}.pkl`。
- **キャッシュクリア**:
  - 毎日 **21:30**（ASW計算等の日次バッチ完了時）に全てのPCAキャッシュを自動削除し、翌日の最新データでの再計算を保証する。

#### 2. データ前処理・計算
- **補間**: 各営業日の銘柄構成の差異を吸収するため、共通グリッドに対する 3次スプライン補間を実施。
- **単位系**:
  - 利回りデータは「%」単位 (0.1% = 0.1) で扱う。
  - **BPS換算**: 画面表示および統計量計算における bps 換算は数値を `100` 倍する。

#### 3. 復元誤差 (Reconstruction Error)
- **定義**: `誤差 = 実測利回り - PCAによる復元利回り`。
- **用途**: 銘柄（Maturity）ごとの誤差を散布図でプロット。特定日の市場の「歪み」を検出するために使用。

### 4. フォワードカーブ計算仕様 (QuantLib)

#### 1. 計算ロジック
- **手法**: `QuantLib.OvernightIndexedSwap` を使用し、対象日のディスカウントカーブから `fairRate` を算出。
- **データソース**: `irs_data` テーブルの TONA OIS レートを使用。
- **カーブ構築**: `PiecewiseLogCubicDiscount` を使用して連続的なカーブを生成。
- **コンベンション**: Act/365 Fixed, Annual。

#### 2. 表示モード
- **Fixed Start (n年先スタート)**: $F(n, n+t)$。ユーザーが指定した開始時期 $n$ から、$t$ 年先までのフォワードレートを表示。
- **Fixed Tenor (m年物推移)**: $F(t, t+m)$。ユーザーが指定したスワップ期間 $m$ のレートが、将来の開始時期 $t$ ごとにどう変化するかをプロット（X軸は $t+m$）。

### 5. TONA (OIS) データの取り扱い

- **参照期間の制限**:
  - TONA (OIS) データは **2026年1月1日以降** のもののみを有効なデータとして扱う。
- **データ取得の共通化 (Single Source of Truth)**:
  - 各サービスやスクリプトで独自の SQL クエリを書かず、必ず `DatabaseManager` (同期/非同期) の共通メソッドを経由して取得すること。
  - 共通メソッド内には自動的に 2026年1月1日以降のフィルタが適用される。

### 6. Webアプリケーション構成 & デプロイ

#### 1. アーキテクチャ
- **Frontend**: Next.js (TypeScript) - `frontend/` ディレクトリ.
- **Backend**: FastAPI (Python) - API提供およびフロントエンドの静的ファイル配信.
- **開発環境**: `scripts/dev.py` を使用してバックエンド・フロントエンドを同時起動（Port 8000 & 3000）.
- **配信方式**: 
  - Next.js を静的エクスポート (`output: 'export'`).
  - FastAPI が `static/dist/` ディレクトリから HTML/JS/CSS を配信.
  - ルーティングは `app/web/main.py` で定義.

#### 2. 主要ディレクトリ & ページ構成
- **`frontend/app/`**: App Router によるページ定義
  - `yield-curve/`: 国債利回りカーブ比較（JGB）
  - `asw/`: ASW (Asset Swap Spread) 分析・比較
  - `pca/`: 主成分分析（TONA OIS / JGB）
  - `market-amount/`: 市中残存額分析
- **`frontend/components/`**: 再利用可能なUIコンポーネント
  - `YieldCurveChart.tsx`: 利回りカーブ用チャート
  - `ASWChart.tsx`: ASWスプレッド用チャート

#### 3. ビルド & デプロイ (Cloud Run)
- **Multi-stage Build**:
  - `Stage 1 (node:20)`: フロントエンドのビルド。React 19/Next 16 対応のため Node 20 以上が必須。
  - `Stage 2 (python:3.11-slim)`: バックエンド実行環境。
- **注意点**: 
  - `.dockerignore` に `frontend/` を含める必要がある（ビルドにソースが必要なため）。
  - `next.config.ts` で `eslint` / `typescript` のビルド時エラーを無視する設定を入れている。
- **デプロイコマンド**: `python3 scripts/deploy.py`

### 7. 日次運用プロセス (Cloud Scheduler)

システムは Google Cloud Scheduler を使用して、以下のスケジュールでデータの自動取得および計算を実行する。

| 実行時間 (JST) | ジョブ名 / エンドポイント | 取得・処理内容 |
| :--- | :--- | :--- |
| **06:00** (毎月1日) | `calendar-refresh` | **入札カレンダー更新**: 財務省から当月の入札予定を取得・キャッシュ。 |
| **07:10** | `macro-daily-collection` | **マクロデータ取得**: 米国債利回り(UST)、為替(USD/JPY等)、主要株価指数を Yahoo Finance / FRED から取得。 |
| **08:45-12:15** (毎週木) | `intl-trans-collection` | **対内対外証券投資**: 財務省の週次データをポーリングして取得。 |
| **12:36** (平日) | `auction-collection` | **入札結果収集**: 当日実施された国債入札の結果（落札価格等）を財務省から取得。 |
| **18:05** | `daily-collection` | **JGB利回り取得**: JSDA（日本証券業協会）から翌営業日公表分の売買参考統計値を取得。 |
| **21:00** | `irs-daily-collection` | **IRSデータ取得**: JPX/JSCC から当日の TONA OIS レートを収集。 |
| **21:00** | `evening-combined` | **統合収集**: 18時の取りこぼし確認、BOJ保有データ同期、IRS取得を統合実行。 |
| **21:30** | `asw-calculation` | **ASW計算**: 最新のJGBとOISから全銘柄のASWを算出。完了後に **PCAキャッシュを自動クリア**。 |

- **Timezone**: 全てのスケジュールは日本標準時 (JST) に基づく。
- **リトライ**: ネットワークエラー等に備え、各エンドポイント内部でリトライロジックが実装されている。
- **監視**: 実行ログは Google Cloud Logging で確認可能。
