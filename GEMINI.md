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

### 4. 日次運用プロセス
