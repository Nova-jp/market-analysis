# 設計書: OIS スポットレート予測モデル（7Y以下・ベースライン）

> **対象読者**: Gemini（実装担当）
> **目的**: 7Y以下の OIS テナーを対象に、LightGBM でh日後の変化量を予測するベースラインノートブックを実装する。
> **出力ファイル**: `research/gbdt/notebooks/spot_rate_7y_base.ipynb`

---

## 1. データ仕様

### 1-1. 分析対象テナー

`irs_data` テーブルから以下のテナーのみを使用する（7Y以下に絞る）：

```python
TARGET_TENORS = ['3M', '6M', '9M', '1Y', '2Y', '3Y', '5Y', '7Y']
```

### 1-2. データ取得

```python
import sys
sys.path.insert(0, '../../')
from data.utils.database_manager import DatabaseManager

db = DatabaseManager()

# OIS データ（product_type='OIS'）
rows = db.get_ois_data(product_type='OIS')
df_swap_raw = pd.DataFrame(rows)
df_swap_raw['trade_date'] = pd.to_datetime(df_swap_raw['trade_date'])

# ワイド形式に変換（index=trade_date, columns=tenor）
df_ois = df_swap_raw.pivot(index='trade_date', columns='tenor', values='rate')

# 7Y以下のテナーのみ残す
df_ois = df_ois[[t for t in TARGET_TENORS if t in df_ois.columns]]
df_ois = df_ois.sort_index()
```

### 1-3. マクロデータ取得

```python
# FX
fx_q = "SELECT trade_date, currency_pair, close_price FROM exchange_rates WHERE currency_pair IN ('USDJPY', 'DXY')"
df_fx = pd.DataFrame(db.select_as_dict(fx_q))
df_fx['trade_date'] = pd.to_datetime(df_fx['trade_date'])
df_fx = df_fx.pivot(index='trade_date', columns='currency_pair', values='close_price')

# 日経
nk_q = "SELECT trade_date, close_price as nikkei FROM stock_prices WHERE ticker = '^N225'"
df_nk = pd.DataFrame(db.select_as_dict(nk_q))
df_nk['trade_date'] = pd.to_datetime(df_nk['trade_date'])
df_nk = df_nk.set_index('trade_date')[['nikkei']]

# 米国10年金利
us_q = "SELECT trade_date, yield_value as ust10y FROM foreign_yields WHERE region = 'US' AND tenor = '10Y'"
df_us = pd.DataFrame(db.select_as_dict(us_q))
df_us['trade_date'] = pd.to_datetime(df_us['trade_date'])
df_us = df_us.set_index('trade_date')[['ust10y']]

# 日銀会合データ
boj_q = "SELECT meeting_date, policy_rate FROM boj_meetings ORDER BY meeting_date"
df_boj = pd.DataFrame(db.select_as_dict(boj_q))
df_boj['meeting_date'] = pd.to_datetime(df_boj['meeting_date'])
```

---

## 2. 前処理

### 2-1. データ結合とMICE補完

```python
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.linear_model import BayesianRidge
from sklearn.preprocessing import StandardScaler

# OIS + マクロを outer join で結合
df_all = df_ois.join([df_fx, df_nk, df_us], how='outer')
df_all = df_all.sort_index()

# 全行NaNは除去
df_all = df_all.dropna(how='all')

# 数値型へ
for col in df_all.columns:
    df_all[col] = pd.to_numeric(df_all[col], errors='coerce')

# 補完フラグを先に立てる（補完前に必ず実行）
numeric_cols = df_all.columns.tolist()
for col in numeric_cols:
    df_all[f'{col}_is_imputed'] = df_all[col].isna().astype(int)

# MICE補完（スケーリング → 補完 → 逆スケーリング）
scaler = StandardScaler()
df_scaled = pd.DataFrame(
    scaler.fit_transform(df_all[numeric_cols]),
    columns=numeric_cols, index=df_all.index
)
imputer = IterativeImputer(estimator=BayesianRidge(), max_iter=10, random_state=42)
df_imputed_scaled = imputer.fit_transform(df_scaled)
df_all[numeric_cols] = scaler.inverse_transform(df_imputed_scaled)
```

### 2-2. 日銀カレンダー特徴量

`boj_meetings` テーブルから `policy_rate` と `days_to_next_mpm` を生成する。

```python
# 全取引日のインデックスに policy_rate を forward fill でマージ
df_boj_daily = df_boj.set_index('meeting_date')[['policy_rate']]
df_boj_daily = df_boj_daily.reindex(df_all.index, method='ffill')

# days_to_next_mpm: 次回会合日までの営業日数（カレンダー日数でも可）
meeting_dates = df_boj['meeting_date'].sort_values().values
def days_to_next(date, meeting_dates):
    future = meeting_dates[meeting_dates > date]
    if len(future) == 0:
        return np.nan
    return (future[0] - date).days

df_all['policy_rate'] = df_boj_daily['policy_rate']
df_all['days_to_next_mpm'] = [days_to_next(d, meeting_dates) for d in df_all.index]
```

### 2-3. MPM直後フラグ（学習・評価除外用）

会合日から5営業日以内のデータは目的変数が不安定なため学習・評価から除外する。

```python
df_all['is_meeting_day'] = df_all.index.isin(df_boj['meeting_date']).astype(int)
df_all['is_post_mpm'] = (
    df_all['is_meeting_day'].rolling(window=6, min_periods=1).max()
).astype(int)
```

---

## 3. 特徴量エンジニアリング

### 3-1. 分数階差の実装

```python
def frac_diff(series: pd.Series, d: float = 0.4, window: int = 50) -> pd.Series:
    """
    固定ウィンドウ法による分数階差。
    先頭 window-1 行は NaN（ウォームアップ期間）。
    """
    weights = [1.0]
    for k in range(1, window):
        weights.append(-weights[-1] * (d - k + 1) / k)
    weights = np.array(weights[::-1])  # 古い順に並べる

    arr = series.to_numpy(dtype=float)
    n = len(arr)
    result = np.full(n, np.nan)
    if n < window:
        return pd.Series(result, index=series.index)

    from numpy.lib.stride_tricks import sliding_window_view
    windows_view = sliding_window_view(arr, window_shape=window)
    dots = windows_view @ weights
    dots[np.isnan(windows_view).any(axis=1)] = np.nan
    result[window - 1:] = dots
    return pd.Series(result, index=series.index)
```

### 3-2. テナーを数値化

```python
def tenor_to_float(t: str) -> float:
    num = float(''.join(filter(str.isdigit, t)))
    return num / 12.0 if 'M' in t else num
```

### 3-3. プーリング（ワイド→ロング）＋ 特徴量生成

予測ホライズン `h` ごとに以下を実行する（h = 3, 5 を別々に実行）。

```python
MACRO_COLS = ['USDJPY', 'DXY', 'nikkei', 'ust10y']
OIS_COLS   = [t for t in TARGET_TENORS if t in df_all.columns]
D, WINDOW  = 0.4, 50

def build_pooled_df(df_all: pd.DataFrame, ois_cols: list, macro_cols: list, h: int) -> pd.DataFrame:
    """
    全テナーのサンプルを縦積みにしたロング形式のDataFrameを返す。
    特徴量:
      - ois_level      : 絶対水準（補完済み）
      - ois_spread_h   : h日前との差分（OIS_t - OIS_{t-h}）
      - ois_fd         : 絶対水準の分数階差（d=0.4）
      - tenor_val      : テナーを数値化
      - policy_rate    : 政策金利水準
      - days_to_next_mpm
      - {macro}_fd     : マクロ変数の分数階差
      - *_is_imputed   : 補完フラグ
    目的変数:
      - target         : OIS_{t+h} - OIS_t（h日後変化量）
    """
    # マクロの分数階差を事前計算
    macro_fd = {}
    for m in macro_cols:
        if m in df_all.columns:
            macro_fd[f'{m}_fd'] = frac_diff(df_all[m], D, WINDOW)

    all_rows = []
    for tenor in ois_cols:
        s = df_all[tenor]

        # 特徴量
        ois_level    = s                              # 絶対水準
        ois_spread_h = s - s.shift(h)                 # h日前とのスプレッド
        ois_fd       = frac_diff(s, D, WINDOW)        # 分数階差

        # 目的変数
        target = s.shift(-h) - s

        tmp = pd.DataFrame({
            'tenor':             tenor,
            'tenor_val':         tenor_to_float(tenor),
            'ois_level':         ois_level,
            'ois_spread_h':      ois_spread_h,
            'ois_fd':            ois_fd,
            'policy_rate':       df_all.get('policy_rate', pd.Series(np.nan, index=df_all.index)),
            'days_to_next_mpm':  df_all.get('days_to_next_mpm', pd.Series(np.nan, index=df_all.index)),
            'is_post_mpm':       df_all['is_post_mpm'],
            'target':            target,
        }, index=df_all.index)

        # マクロ FD を追加
        for col, series in macro_fd.items():
            tmp[col] = series

        # 補完フラグを追加（OIS + マクロ）
        for col in [tenor] + macro_cols:
            flag_col = f'{col}_is_imputed'
            if flag_col in df_all.columns:
                tmp[flag_col] = df_all[flag_col]

        all_rows.append(tmp)

    df_long = pd.concat(all_rows).sort_index()

    # NaN が残る行（ウォームアップ期間など）を除去
    feat_cols = [c for c in df_long.columns if c not in ('tenor', 'is_post_mpm', 'target')]
    df_long = df_long.dropna(subset=feat_cols + ['target'])

    # MPM直後除外
    df_long = df_long[df_long['is_post_mpm'] == 0].drop(columns='is_post_mpm')

    return df_long
```

### 3-4. 目的変数の正規化

```python
# テナー別 std で正規化（異分散補正）
df_long['target_std'] = df_long.groupby('tenor')['target'].transform('std')
df_long['target_norm'] = df_long['target'] / df_long['target_std']
```

---

## 4. ウォークフォワード検証

```python
import bisect
from scipy.stats import spearmanr
import lightgbm as lgb

PARAMS = {
    'objective':       'regression',
    'metric':          'rmse',
    'verbosity':       -1,
    'boosting_type':   'gbdt',
    'random_state':    42,
    'learning_rate':   0.05,
    'num_leaves':      31,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq':    5,
    'min_data_in_leaf': 30,
    'lambda_l2':       1.0,
}
NUM_BOOST_ROUND = 100
TEST_WINDOW_DAYS = 90
PURGE_DAYS = 5

NON_FEATURE_COLS = {'tenor', 'target', 'target_std', 'target_norm'}

def walk_forward(df_long: pd.DataFrame, oos_start: str) -> pd.DataFrame:
    """
    拡張窓ウォークフォワード検証。
    target_norm を予測し、IC で評価する。
    """
    target_col  = 'target_norm'
    feature_cols = [c for c in df_long.columns if c not in NON_FEATURE_COLS]

    unique_dates = sorted(df_long.index.unique())
    oos_start_dt = pd.to_datetime(oos_start)
    start_idx = bisect.bisect_left(unique_dates, oos_start_dt)

    results = []
    current_idx = start_idx
    fold = 0

    while current_idx < len(unique_dates):
        train_end  = unique_dates[current_idx - 1]
        purge_end  = train_end - pd.Timedelta(days=PURGE_DAYS)
        test_start = unique_dates[current_idx]
        test_end   = test_start + pd.Timedelta(days=TEST_WINDOW_DAYS)

        train_mask = df_long.index <= purge_end
        test_mask  = (df_long.index >= test_start) & (df_long.index < test_end)

        if train_mask.sum() < 100 or test_mask.sum() < 10:
            break

        X_train = df_long.loc[train_mask, feature_cols]
        y_train = df_long.loc[train_mask, target_col]
        X_test  = df_long.loc[test_mask,  feature_cols]
        y_test  = df_long.loc[test_mask,  target_col]

        dtrain = lgb.Dataset(X_train, label=y_train,
                             categorical_feature=['tenor_val'])  # tenor_val をカテゴリとして扱う場合は変更
        model  = lgb.train(PARAMS, dtrain, num_boost_round=NUM_BOOST_ROUND)

        preds      = model.predict(X_test)
        train_ic, _ = spearmanr(y_train, model.predict(X_train))

        fold_df = df_long.loc[test_mask, ['tenor', 'target', 'target_std']].copy()
        fold_df['pred_norm'] = preds
        fold_df['pred']      = preds * fold_df['target_std']  # 逆正規化
        fold_df['actual']    = fold_df['target']
        fold_df['fold']      = fold
        fold_df['train_ic']  = train_ic

        results.append(fold_df)

        fold += 1
        next_date  = test_end
        current_idx = bisect.bisect_left(unique_dates, next_date)

    return pd.concat(results).reset_index(names='date') if results else pd.DataFrame()
```

---

## 5. IC 集計

```python
def summarize_ic(results_df: pd.DataFrame, recent_n_folds: int = 3) -> dict:
    actual = results_df['actual']
    pred   = results_df['pred_norm']

    # Global IC
    ic_all, _ = spearmanr(actual, pred)

    # Cross-Sectional IC（各日付でテナーをランク付け）
    cs_ics = []
    for date, grp in results_df.groupby('date'):
        if len(grp) >= 3:
            ic, _ = spearmanr(grp['actual'], grp['pred_norm'])
            if not np.isnan(ic):
                cs_ics.append(ic)
    cs_ic = np.mean(cs_ics) if cs_ics else np.nan

    # Time-Series IC（テナー別時系列IC）
    ts_ics = []
    for tenor, grp in results_df.groupby('tenor'):
        if len(grp) >= 5:
            ic, _ = spearmanr(grp['actual'], grp['pred_norm'])
            if not np.isnan(ic):
                ts_ics.append(ic)
    ts_ic = np.mean(ts_ics) if ts_ics else np.nan

    # 直近Nフォールドの Global IC
    recent_folds = sorted(results_df['fold'].unique())[-recent_n_folds:]
    recent_df    = results_df[results_df['fold'].isin(recent_folds)]
    ic_recent, _ = spearmanr(recent_df['actual'], recent_df['pred_norm'])

    # Train IC と Gap
    train_ic = results_df.groupby('fold')['train_ic'].first().mean()
    gap      = train_ic - ic_all

    return dict(
        ic_all=ic_all, ic_recent=ic_recent,
        cs_ic=cs_ic, ts_ic=ts_ic,
        train_ic=train_ic, gap=gap,
    )
```

---

## 6. ノートブック構成

以下のセル構成で `spot_rate_7y_base.ipynb` を作成すること。

| # | セルの内容 |
|---|-----------|
| 1 | ライブラリ import、定数定義（TARGET_TENORS, D, WINDOW 等） |
| 2 | データ取得（OIS, FX, 日経, 米金利, 日銀会合） |
| 3 | OIS 概観プロット（全テナー時系列、欠損率確認） |
| 4 | MICE補完実行＋補完フラグ生成 |
| 5 | 日銀カレンダー特徴量・is_post_mpm 生成 |
| 6 | `build_pooled_df` でロング形式に変換（h=3 と h=5 を個別に作成） |
| 7 | 特徴量の基本統計量・相関確認（データリーク・NaN 残留チェック） |
| 8 | OOS開始日を設定して `walk_forward` 実行（h=3） |
| 9 | `walk_forward` 実行（h=5） |
| 10 | `summarize_ic` で結果集計。以下の形式で出力：`Global IC=X.XXX, CS IC=X.XXX, TS IC=X.XXX, Gap=X.XXX` |
| 11 | `ic_by_fold` プロット（フォールド別 OOS IC の推移） |
| 12 | `ic_by_tenor` プロット（テナー別 TS IC） |
| 13 | Feature Importance プロット（上位20特徴量） |
| 14 | 結果サマリーをテキストセルに記録（後でレビューするため） |

---

## 7. 出力ファイル

以下を `research/gbdt/outputs/` に保存すること：

- `spot_7y_base_ic_by_fold_3d.png`
- `spot_7y_base_ic_by_fold_5d.png`
- `spot_7y_base_ic_by_tenor_3d.png`
- `spot_7y_base_ic_by_tenor_5d.png`
- `spot_7y_base_feature_importance_3d.png`
- `spot_7y_base_feature_importance_5d.png`

---

## 8. 結果記録フォーマット（ノートブック末尾のテキストセルに記入）

```
モデル名: spot_rate_7y_base
対象テナー: 3M, 6M, 9M, 1Y, 2Y, 3Y, 5Y, 7Y
特徴量: ois_level, ois_spread_h, ois_fd, macro_fd, boj_calendar
OOS開始日: YYYY-MM-DD
rounds: 100

3d: Global IC=0.XXX, CS IC=0.XXX, TS IC=0.XXX, Gap=0.XXX, ic_recent=0.XXX
5d: Global IC=0.XXX, CS IC=0.XXX, TS IC=0.XXX, Gap=0.XXX, ic_recent=0.XXX

特記事項:
- （特徴量の効き・問題点など気づいたことを記録）
```

---

## 9. 注意事項・よくある罠

| 罠 | 対策 |
|----|------|
| `target_norm` の std が系列ごとに大きくばらつく | プーリング後にテナー別全期間 std で計算する |
| MICE補完のフラグ生成を補完後にやってしまう | **必ず補完前**に `_is_imputed` フラグを生成する |
| MPM直後データが混入する | `is_post_mpm==1` を学習・評価から除外する |
| 最終フォールドのみの IC を報告する | 全OOSフォールド合算の Global IC を必ず確認する |
| `ois_spread_h` に未来情報が混入する | `s - s.shift(h)` は過去を参照しているので問題なし |
| `target` に未来情報が混入 | `s.shift(-h) - s` の `dropna` で末尾 h 行は自動除去される |
| early stopping で `best_iteration=1` | early stopping は使わず固定100ラウンドで統一 |
