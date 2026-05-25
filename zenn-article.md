---
title: "国債トレーダーがPythonで自作した金利分析プラットフォーム ─ イールドカーブPCA・ASW・フォワードカーブをゼロから実装"
emoji: "📈"
type: "tech"
topics: ["Python", "FastAPI", "nextjs", "googlecloud", "fintech"]
published: false
---

## はじめに

証券会社の金利デスクでトレーダーをしています。日々の業務は「データとの戦い」と言っても過言ではありません。

毎朝デスクに座ると、まずブルームバーグで前日の国債利回りを確認し、JSDA（日本証券業協会）のサイトで出来高を調べ、JSCCのページでスワップレートを開き、財務省のサイトで次回の入札スケジュールを確認する……。これらを全部ブラウザで別々に開きながら、頭の中でPCAのファクターがどう動いたかを計算する。慣れれば慣れるほど「自動化したい」と思うものです。

ブルームバーグやリフィニティブにはビルトインの分析機能がありますが、自分が本当に見たい分析軸（国債イールドカーブのPCA、ASWの期間構造、フォワードカーブのOISストリップ）を自由に組み合わせるには、どうしても限界があります。

**「自分で作れば完全に自由だ」** という発想で始まった個人開発プロジェクトが、気づけば本番稼働するWebアプリケーションになっていました。この記事では、金融ドメイン知識とエンジニアリングがどう交差するのかを軸に、設計の意図と実装の詳細を紹介します。

---

## システム全体像

### できること

このプラットフォームは以下の分析機能を提供します。

| 機能 | 説明 |
|------|------|
| **イールドカーブ可視化** | 最大20日付のカーブを重ね合わせて比較 |
| **PCA主成分分析** | Level/Slope/Curvatureのファクター分解と寄与率 |
| **ASW期間構造** | 国債ごとのAsset Swap Spreadをプロット |
| **フォワードカーブ** | Fixed Start / Fixed Tenor のOISフォワード |
| **市中残存額** | 日銀保有額を控除した実質的な市中流通量 |
| **Excelエクスポート** | ASW・TONA・IMM OISのマトリクスを一括ダウンロード |

### アーキテクチャ

4層構造で設計しています。依存の方向を一方向に保つことで、各層の責務を明確に分離しています。

```
【Web層】    web/  ─── Next.js 16 (静的エクスポート)
               │
               ▼
【API層】    api/  ─── FastAPI + SQLAlchemy (Async)
               │
               ▼
【Core層】   core/ ─── 設定・DB・計算ロジック・Pydanticモデル
               ▲
               │
【Pipeline層】 pipeline/ ─── JSDA / MOF / BOJ / JSCC データ収集
```

`pipeline/` は `api/` を import しません。データの流れは常に一方向（外部 → pipeline → DB → api → web）です。

---

## 技術スタック

| レイヤー | 技術 | 選定理由 |
|---------|------|---------|
| バックエンド | FastAPI 0.128 | 非同期IO・型安全・自動ドキュメント生成 |
| ORM | SQLAlchemy 2.0 (Async) | asyncpgとの統合、型安全なクエリ |
| DB | Neon PostgreSQL (Serverless) | スケールゼロ、無料枠で個人開発に最適 |
| フロントエンド | Next.js 16 (静的エクスポート) | Node.jsサーバー不要、FastAPIが一括配信 |
| グラフ | Recharts 3 | Reactネイティブ、ResponsiveContainer対応 |
| データ処理 | pandas + numpy | CSV処理・集計 |
| 金融計算 | QuantLib 1.40 | 債券計算・カーブビルディングの業界標準ライブラリ |
| 主成分分析 | scikit-learn 1.8 | PCA実装 |
| 補間 | scipy 1.16 (CubicSpline) | スワップレートの連続補間 |
| インフラ | Cloud Run + Cloud Scheduler | サーバーレス、mainプッシュで自動デプロイ |

---

## データ収集パイプライン ─ 最大の難関

金融分析ツールの価値は、分析ロジックよりも**データの品質と鮮度**で決まります。ここが最も時間を使った部分です。

### なぜ複数ソースが必要か

国債市場を正確に理解するには、単一ソースでは不十分です。

- **JSDA**: 国債の利回り・単価・出来高。25列のCSVで毎営業日18時頃に公表
- **JSCC/JPX**: 金利スワップ（IRS/OIS）のマーケットデータ。ASWやフォワードカーブ計算に必須
- **財務省**: 入札結果・入札カレンダー。需給分析に使う
- **日本銀行**: 国債保有状況（月次）。「市中残存額 = 発行残高 - 日銀保有」の計算に必要

これらを組み合わせて初めて「市場が何を考えているか」が見えてきます。

### JSDA収集の実装上の苦労

#### サーバー保護ルール

JSDAのサーバーに対しては、**アクセス間隔を5秒以上**（推奨30秒以上）空ける運用ルールがあります。過去データを一括収集する場合、数百日分のデータ取得に数時間かかることもあります。

```python
# pipeline/fetchers/jsda/processor.py

class BondDataProcessor:
    """政府系債券データ処理クラス（25列対応）"""

    def __init__(self):
        self.base_url = "https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/files"
        
        # JSDA CSVは欠損値を数値で埋める（999.999等）
        self.invalid_values = {
            'coupon_rate': [99.999],
            'yields': [999.999],  # 利回り系フィールドの異常値マーク
            'prices': [999.99],   # 価格系フィールドの異常値マーク
        }
```

#### 25列のカラムマッピング

JSDAのCSVは列名がなく、位置で意味が決まります。25列（L・M・N・T列はスキップ）を正確にマッピングする必要がありました。

```python
self.column_mapping = {
    0: 'trade_date',            # A: 日付
    1: 'issue_type',            # B: 銘柄種別 (1=T-Bills, 2=国債)
    2: 'bond_code',             # C: 銘柄コード
    3: 'bond_name',             # D: 銘柄名
    4: 'due_date',              # E: 償還期日
    5: 'coupon_rate',           # F: 利率
    6: 'ave_compound_yield',    # G: 平均値複利
    7: 'ave_price',             # H: 平均値単価
    # ...以下省略（L, M, N, T列はスキップ）
    25: 'median_compound_yield', # Z: 中央値複利
}
```

#### 「公表日」と「取引日」のずれ問題

JSDA CSVのファイル名は「公表日」（翌営業日）が使われています。つまり、2025年12月30日（火）の取引データは「2026年1月5日（月）」というファイル名で公表されます。

年末年始などの連休が挟まると、このずれが大きくなります。単純にファイル名の日付をそのまま取引日として保存すると、データが1〜数日ずれてしまいます。

対策として、JSDAのHTMLページにある日付リストをスクレイピングして、「ファイルの公表日の一つ前の日付」を取引日として特定する処理を実装しました。

```python
def determine_trade_date_from_html(self, target_date: date) -> Optional[date]:
    """
    HTMLの日付リストで、target_dateの一つ前の日付を取引日として特定する

    例: リスト [..., 2026-01-05, 2025-12-30, ...] で
        target_date が 2026-01-05 の場合 → 2025-12-30 を返す
    """
    # HTMLから利用可能な日付リストを取得（キャッシュ利用）
    if self._available_dates_cache is None:
        parser = JSDAParser()
        self._available_dates_cache = parser.get_available_dates()
    
    sorted_dates = sorted(self._available_dates_cache, reverse=True)
    for i, d in enumerate(sorted_dates):
        if d == target_date and i + 1 < len(sorted_dates):
            return sorted_dates[i + 1]  # 一つ前の日付が取引日
    return None
```

### Cloud Schedulerとの統合

毎日のデータ更新はCloud Scheduler → Cloud RunのHTTPエンドポイント経由で実行しています。

```
毎日 18:00 JST → POST /api/scheduler/daily-collection
                 → JSDA + MOF + BOJ データ収集
毎日 21:00 JST → POST /api/scheduler/irs-daily-collection
                 → JSCC IRS/OISデータ収集
毎月 1日 6:00   → POST /api/scheduler/calendar-refresh
                 → 財務省入札カレンダー更新
```

---

## 金融計算の実装 ─ ドメイン知識が活きる部分

ここが記事の核心です。金融の概念から丁寧に解説しながら、Pythonでの実装を見ていきます。

### 1. イールドカーブのPCA分析

#### 金融の背景

イールドカーブは「残存年数（横軸）vs 利回り（縦軸）」のグラフです。毎日動いているように見えますが、実はその変動の**9割以上が3つのパターン**で説明できます。

- **第1主成分（Level）**: カーブ全体が上下する「水準」変化。日銀が利上げ・利下げするとこの動きが大きくなる（寄与率 約92%）
- **第2主成分（Slope）**: 短期と長期が逆方向に動く「傾き」変化。景気後退懸念でスティープ化、利上げ期待でフラット化する
- **第3主成分（Curvature）**: 短期・長期に対して中期が相対的に動く「曲率」変化

トレーダーはこの3成分で市場の動きを語ります（例：「今週はPC1が上昇、PC2がフラットナーで動いた」）。

#### 実装

各日のイールドカーブは銘柄ごとにバラバラな残存年数を持っているため、まず共通グリッドへの補間が必要です。

```python
# core/calculations/pca.py

from sklearn.decomposition import PCA
from scipy.interpolate import CubicSpline

class PCAService:
    
    def interpolate_yield_curves(self, daily_data: Dict[str, pd.DataFrame]):
        """各日のイールドカーブを3次スプライン補間で共通グリッドに揃える"""
        
        # 全日付の残存年数の和集合を共通グリッドとする
        all_maturities = set()
        for df in daily_data.values():
            all_maturities.update(df['maturity'].values)
        common_grid = np.sort(np.array(list(all_maturities)))

        interpolated_data = []
        for date, df in daily_data.items():
            df_sorted = df.sort_values('maturity').drop_duplicates('maturity')
            
            # 3次スプライン補間（extrapolate=Falseで範囲外はNaN）
            cs = CubicSpline(
                df_sorted['maturity'].values,
                df_sorted['yield'].values,
                extrapolate=False
            )
            interpolated = cs(common_grid)
            
            # NaNが50%未満の行のみ採用
            if np.isnan(interpolated).sum() / len(interpolated) < 0.5:
                interpolated_data.append(interpolated)

        return np.array(interpolated_data), common_grid
    
    def perform_pca(self, X: np.ndarray, n_components: int = 3):
        """PCA実行"""
        # NaNを列平均で補完してから実行
        X_filled = X.copy()
        col_means = np.nanmean(X, axis=0)
        for i in range(X.shape[1]):
            mask = np.isnan(X_filled[:, i])
            X_filled[mask, i] = col_means[i]
        
        pca = PCA(n_components=n_components)
        X_pca = pca.fit_transform(X_filled)
        
        # pca.explained_variance_ratio_[0] ≈ 0.92 (92%が第1主成分で説明可能)
        return pca, X_pca
```

計算結果はjoblibでキャッシュし、同じパラメータで再度リクエストされた場合の再計算を回避しています。

```python
def run_pca_analysis(self, lookback_days: int = 100, n_components: int = 3, end_date: Optional[str] = None):
    """PCA分析実行（キャッシュ対応）"""
    
    # キャッシュヒット時はDBアクセス不要
    cache_data = self.load_cache(actual_end_date, lookback_days)
    if cache_data and cache_data['pca'].n_components == n_components:
        # ... キャッシュから返す
    
    # キャッシュミス時: DB取得 → 補間 → PCA → キャッシュ保存
    X, common_grid, valid_dates = self.interpolate_yield_curves(daily_data)
    pca, X_pca = self.perform_pca(X, n_components)
    self.save_cache(actual_end_date, lookback_days, {'pca': pca, 'X_pca': X_pca, ...})
```

---

### 2. ASW（Asset Swap Spread）

#### 金融の背景

```
ASW = 国債利回り - 同年限スワップレート（MMS）
```

「国債とスワップを交換したら損か得か」を測る指標です。

国債は「元本保証のリスクフリー資産」ですが、日本では超低金利政策の影響で国債が需給ひっ迫し、同年限スワップより**利回りが低い（ASWがマイナス）**状況が続いてきました。ASWのタイト/ワイドを追うことで、市場の国債需給感を定量的に把握できます。

#### 実装のポイント：単純なCubicSplineではなくQuantLibを使う理由

スワップレートは「1Y・2Y・3Y…10Y」といった整数年限のデータしかありません。対して国債の残存年数は銘柄ごとにバラバラです（例：残存2.37年）。

単純なCubicSpline補間でも近似はできますが、国債入札スケジュールや利払日を考慮した**Matched Maturity Swap（MMS）**を正確に計算するには、QuantLibによるカーブビルディングが必要です。

```python
# core/calculations/bond_math.py

import QuantLib as ql

class QuantLibHelper:
    def __init__(self, date_str: str):
        ql.Settings.instance().evaluationDate = self._parse_date(date_str)
        self.calendar = ql.Japan()
        # TONA（無担保翌日物金利）インデックス
        self.tona_index = ql.OvernightIndex(
            "TONA", 0, ql.JPYCurrency(), ql.Japan(), ql.Actual365Fixed()
        )

    def build_ois_curve(self, ois_data: list):
        """OIS（翌日物金利スワップ）カーブを構築"""
        rate_helpers = []
        for item in ois_data:
            tenor = self._parse_tenor(item['tenor'])
            rate = item['rate'] / 100.0
            helper = ql.OISRateHelper(2, tenor, ql.QuoteHandle(ql.SimpleQuote(rate)), self.tona_index)
            rate_helpers.append(helper)
        
        # ピースワイズ対数三次ディスカウントカーブ
        self.yield_curve = ql.PiecewiseLogCubicDiscount(
            2, self.calendar, rate_helpers, ql.Actual365Fixed()
        )
        self.yield_curve.enableExtrapolation()

    def calculate_mms(self, maturity_date_str: str, fixed_freq_str: str = 'Annual') -> float:
        """
        指定満期日のMatchedMaturitySwapレートを計算
        国債と同じ日付構造のスワップのフェアレート（ASW計算の基準）
        """
        maturity_date = self._parse_date(maturity_date_str)
        
        # OvernightIndexedSwapのフェアレート = 国債と同年限のスワップレート
        swap = ql.OvernightIndexedSwap(
            ql.OvernightIndexedSwap.Payer,
            10000.0,
            fixed_schedule,
            0.01,  # ダミーレート（fairRate()で上書きされる）
            ql.Actual365Fixed(),
            self.tona_index
        )
        engine = ql.DiscountingSwapEngine(self.yield_curve_handle)
        swap.setPricingEngine(engine)
        
        return swap.fairRate() * 100.0  # % 表示で返す
```

```python
# api/services/asw.py での利用

ql_helper = QuantLibHelper(date_str)
ql_helper.build_ois_curve(ois_data)

for bond in bond_data:
    bond_yield = float(bond['ave_compound_yield'])
    maturity_str = bond['due_date'].strftime("%Y-%m-%d")
    
    # 年払い（PA）と半年払い（SA）の両方を計算
    mms_pa = ql_helper.calculate_mms(maturity_str, 'Annual')
    mms_sa = ql_helper.calculate_mms(maturity_str, 'Semiannual')
    
    asw_pa = bond_yield - mms_pa  # ASW（年払い基準）
    asw_sa = bond_yield - mms_sa  # ASW（半年払い基準）
```

なお、QuantLibはCPU集約的な処理のため、FastAPIの非同期コンテキストでは `run_in_threadpool` でラップして呼び出しています。

---

### 3. フォワードカーブ（OISフォワード）

#### 金融の背景

フォワードレートとは「n年後からm年間の金利」を指します。OISカーブから計算し、「市場が将来の金利をどう予測しているか」を可視化します。

例えば「1年後スタートの1年スワップレート（1y1y forward）が現在のスポット1年金利より高い」なら、市場は「1年後に金利が上昇する」と予想していることになります。

このプラットフォームでは2種類のフォワードカーブを実装しています：
- **Fixed Start**: 開始日を固定して、テナー（1M・3M・6M・1Y…）を変えていく
- **Fixed Tenor**: テナーを固定して、開始日（1Y→2Y→3Y…）をずらしていく

#### 実装

```python
# core/calculations/bond_math.py

def calculate_forward_swap_rate(self, start_tenor_str: str, swap_tenor_str: str) -> Optional[float]:
    """
    フォワードスワップレートを計算
    
    Args:
        start_tenor_str: 開始テナー ('1Y', '2Y'...)
        swap_tenor_str:  スワップ長さ ('1Y', '3M'...)
    
    Returns:
        フォワードパースワップレート [%]
    """
    start_tenor = self._parse_tenor(start_tenor_str)
    swap_tenor = self._parse_tenor(swap_tenor_str)
    
    # スポット日 = 評価日 + 2営業日（JPYスワップ標準）
    spot_date = self.calendar.advance(self.eval_date, 2, ql.Days)
    forward_start_date = self.calendar.advance(spot_date, start_tenor)
    maturity_date = self.calendar.advance(forward_start_date, swap_tenor)
    
    schedule = ql.Schedule(
        forward_start_date, maturity_date,
        ql.Period(ql.Annual), self.calendar,
        ql.ModifiedFollowing, ql.ModifiedFollowing,
        ql.DateGeneration.Backward, False
    )
    
    swap = ql.OvernightIndexedSwap(
        ql.OvernightIndexedSwap.Payer, 10000.0, schedule,
        0.01,  # ダミーレート
        ql.Actual365Fixed(), self.tona_index
    )
    engine = ql.DiscountingSwapEngine(self.yield_curve_handle)
    swap.setPricingEngine(engine)
    
    return swap.fairRate() * 100.0
```

IMMオプション（3月・6月・9月・12月の第3水曜日からのフォワード）も実装しており、「ZH26（2026年3月IMM開始）からの3Mスワップ」といった形式での計算に対応しています。

---

## インフラ・CI/CD

### マルチステージDockerビルド

フロントエンド（Next.js）とバックエンド（Python）を1コンテナにまとめています。Cloud Runのサービスを1つに抑えることで、管理コストと費用を最小化するためです。

```dockerfile
# infra/Dockerfile

# ステージ1: Next.jsをビルド
FROM node:20 AS frontend-builder
WORKDIR /build
COPY web/package*.json ./
RUN npm ci
COPY web/ .
RUN npm run build  # → out/ に静的ファイルを生成

# ステージ2: Python本番イメージ
FROM python:3.11-slim

# QuantLib等のC++拡張のためgccが必要
RUN apt-get install -y gcc

# キャッシュ最適化: 依存インストールをコード変更より先に
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコード
COPY core/ core/
COPY api/ api/
COPY pipeline/ pipeline/

# フロントエンドの静的ファイルをコピー
# FastAPIがこのディレクトリをStaticFilesとしてマウント
COPY --from=frontend-builder /build/out/ static/dist/

# 非rootユーザーで実行（セキュリティベストプラクティス）
RUN useradd -r -g appuser appuser
USER appuser

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
```

Next.jsの静的エクスポート（`next export`相当）は `api/main.py` でマウントしています。

```python
# api/main.py

# Dockerビルド後は static/dist/、ローカル開発は web/out/ を優先
dist_path = os.path.join(project_root, "static", "dist")
if not os.path.exists(dist_path):
    dist_path = os.path.join(project_root, "web", "out")

if os.path.exists(dist_path):
    app.mount("/", StaticFiles(directory=dist_path, html=True), name="static")
```

### GitHub Actions × Workload Identity Federation

`main` ブランチへのプッシュで自動デプロイが走ります。

```yaml
# .github/workflows/deploy.yml

jobs:
  deploy:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write  # WIF用OIDCトークン発行に必要

    steps:
      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.WIF_PROVIDER }}
          service_account: ${{ secrets.WIF_SERVICE_ACCOUNT }}
          # ← サービスアカウントJSONキーをシークレットに持たない
          # GitHubのOIDCトークンで一時的な権限を取得する

      - name: Build and push Docker image
        run: |
          docker build -f infra/Dockerfile -t $IMAGE:${{ github.sha }} .
          docker push $IMAGE:${{ github.sha }}
      
      - name: Deploy to Cloud Run
        run: |
          gcloud run deploy market-analytics \
            --image $IMAGE:${{ github.sha }} \
            --region asia-northeast1
```

**Workload Identity Federationを採用した理由**: サービスアカウントJSONキーを使う場合、キーのローテーション管理が必要で、漏洩リスクも高くなります。WIFならGitHubのOIDCトークンを使って一時的な認証ができるため、長期認証情報が不要です。

---

## DBの設計判断：非同期と同期の使い分け

このプロジェクトでは、同じNeon PostgreSQLに接続するクライアントを2種類用意しています。

```
core/db/async_client.py  ← FastAPI（Webリクエスト処理）用
core/db/sync_client.py   ← pipeline/scripts（バッチ処理）用
```

**FastAPIは非同期I/O**で複数リクエストを同時に捌く必要があるため、`asyncpg`ドライバを使った非同期クライアントを使います。一方、データ収集スクリプトは単一プロセスで順次実行するため、シンプルな同期クライアント（psycopg2）で十分です。

Neonはサーバーレスなので、長時間アクセスがないとコネクションが切断されます。`pool_recycle=1800`（30分）で接続を定期的に再作成し、ゾンビコネクションを防いでいます。

```python
# core/db/engine.py

engine = create_async_engine(
    settings.async_database_url,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800,  # Neonのアイドル切断対策
    connect_args={"ssl": "require"}
)
```

---

## 開発を通じて学んだこと

### 金融ドメイン知識がエンジニアリングを変えた

「なぜASWにQuantLibが必要か」を理解しているからこそ、単純なCubicSpline補間に留まらない実装ができました。年払い・半年払いの違いがスワップレートに影響することを知っていれば、`annual`と`semiannual`の両方を計算するAPIを設計する必然性がわかります。

逆に言えば、金融ドメインの知識がない状態でこの要件を受け取っても、「どこが重要な設計上の決断なのか」が判断できません。

### 実務データの汚さ

JSDA CSVの「欠損値を999.999で埋める」仕様は、実際にデータを触るまでわかりませんでした。「公表日と取引日がずれる」問題も同様です。金融データは見た目はきれいでも、実務レベルで使おうとすると必ずこういった「暗黙の仕様」が出てきます。

### 個人開発だからこそ本番品質を追求できた

「自分が毎朝使うツール」だという前提があるので、CI/CDを整備してデプロイを自動化し、Cloud Schedulerでデータを毎日更新する仕組みを作りました。個人プロジェクトでもプロダクションレベルの品質を追求する経験は、ソフトウェアエンジニアリングへの理解を実践的に深めてくれました。

---

## おわりに

「金融×エンジニアリング」を両方できる人材は、まだ希少です。このプロジェクトを通じて、トレーダーとしての相場感がコード設計に影響を与え、逆にコードを書くことで金融の仕組みを改めて言語化できることを実感しました。

フィンテック領域では、金融の仕組みを深く理解した上でシステムを設計・実装できるエンジニアが求められています。このプロジェクトは、その橋渡しができる人材であることを示す取り組みの一つです。

コードはGitHubで公開していますので、興味のある方はご覧ください。

---

*この記事で紹介したコードは一部省略・簡略化しています。*
