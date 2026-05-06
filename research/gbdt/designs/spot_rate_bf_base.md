# 設計書: OIS バタフライ予測モデル（ガイド準拠ベースライン）

> **対象読者**: Gemini（実装担当）
> **目的**: ガイド（`gbdt_feature_engineering_guide.md`）の設計に忠実に従い、OISバタフライ変化量を予測するモデルを実装する。`bf_level` を予測対象テナー1本のみにした場合（Config A）と全内側テナー分追加した場合（Config B）を比較する。
> **出力ファイル**: `research/gbdt/notebooks/spot_rate_bf_base.ipynb`
> **参照**: `research/designs/gbdt_feature_engineering_guide.md`

---

## 1. テナー設計

### 全テナー（11本）

```python
ALL_TENORS = ['3M', '6M', '9M', '1Y', '18M', '2Y', '3Y', '4Y', '5Y', '6Y', '7Y']
```

### プーリング対象テナー（内側9本・端点除外）

```python
INNER_TENORS = ['6M', '9M', '1Y', '18M', '2Y', '3Y', '4Y', '5Y', '6Y']
```

端点（`3M`, `7Y`）はバタフライが成立しないため目的変数から除外する。
ただし `OIS_3M`, `OIS_7Y` は特徴量（front_spread, slope）として使用する。

---

## 2. データ取得

```python
import sys
sys.path.insert(0, '/Users/nishiharahiroto/Documents/programs/market-analytics-ver1')
from data.utils.database_manager import DatabaseManager

db = DatabaseManager()

# OIS データ
rows = db.get_ois_data(product_type='OIS')
df_swap_raw = pd.DataFrame(rows)
df_swap_raw['trade_date'] = pd.to_datetime(df_swap_raw['trade_date'])
df_ois = df_swap_raw.pivot(index='trade_date', columns='tenor', values='rate')
df_ois = df_ois[[t for t in ALL_TENORS if t in df_ois.columns]].sort_index()

# 取得できたテナーを確認（18M, 4Y, 6Y が存在するか）
print("Available tenors:", df_ois.columns.tolist())
print("Missing tenors:", [t for t in ALL_TENORS if t not in df_ois.columns])
```

> **注意**: `18M`, `4Y`, `6Y` が `irs_data` に存在しない場合はセルに表示して処理を止めること。存在しない場合はそのテナーを `ALL_TENORS` / `INNER_TENORS` から除外して続行する。

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
boj_q = "SELECT meeting_date, policy_rate_after as policy_rate FROM boj_meetings ORDER BY meeting_date"
df_boj = pd.DataFrame(db.select_as_dict(boj_q))
df_boj['meeting_date'] = pd.to_datetime(df_boj['meeting_date'])
df_boj['policy_rate'] = df_boj['policy_rate'].astype(float)
```

---

## 3. 前処理

### 3-1. 結合・MICE補完

```python
# outer join で結合
df_all = df_ois.join([df_fx, df_nk, df_us], how='outer').sort_index()
df_all = df_all.dropna(how='all')
for col in df_all.columns:
    df_all[col] = pd.to_numeric(df_all[col], errors='coerce')

# 補完フラグ（補完前に必ず生成）
numeric_cols = [c for c in df_all.columns if not c.endswith('_is_imputed')]
for col in numeric_cols:
    df_all[f'{col}_is_imputed'] = df_all[col].isna().astype(int)

# MICE補完
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.linear_model import BayesianRidge
from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
df_scaled = pd.DataFrame(scaler.fit_transform(df_all[numeric_cols]),
                         columns=numeric_cols, index=df_all.index)
imputer = IterativeImputer(estimator=BayesianRidge(), max_iter=10, random_state=42)
df_all[numeric_cols] = scaler.inverse_transform(imputer.fit_transform(df_scaled))
```

### 3-2. 日銀カレンダー特徴量

```python
# policy_rate を forward fill
df_boj_daily = df_boj.set_index('meeting_date')[['policy_rate']]
df_all['policy_rate'] = df_boj_daily.reindex(df_all.index, method='ffill')['policy_rate']

# days_to_next_mpm（線形のまま使用。sin/cos変換は不要）
meeting_dates = df_boj['meeting_date'].sort_values().values
def days_to_next(date):
    future = meeting_dates[meeting_dates > date]
    return int((future[0] - date).days) if len(future) > 0 else np.nan
df_all['days_to_next_mpm'] = [days_to_next(d) for d in df_all.index]

# MPM直後フラグ
df_all['is_meeting_day'] = df_all.index.isin(df_boj['meeting_date']).astype(int)
df_all['is_post_mpm'] = df_all['is_meeting_day'].rolling(window=6, min_periods=1).max().astype(int)
```

---

## 4. バタフライ計算

```python
def compute_butterfly(df_ois: pd.DataFrame, all_tenors: list, inner_tenors: list) -> pd.DataFrame:
    """
    inner_tenors に対してのみバタフライを計算する（端点除外）。
    各テナーの両隣を all_tenors のリストから決定する。
    """
    bf = pd.DataFrame(index=df_ois.index)
    for t in inner_tenors:
        idx = all_tenors.index(t)
        prev_t = all_tenors[idx - 1]
        next_t = all_tenors[idx + 1]
        bf[t] = 2 * df_ois[t] - df_ois[prev_t] - df_ois[next_t]
    return bf

# 補完済みOISからバタフライを計算
ois_imputed = df_all[[t for t in ALL_TENORS if t in df_all.columns]]
available_inner = [t for t in INNER_TENORS if t in ois_imputed.columns]
df_bf = compute_butterfly(ois_imputed, ALL_TENORS, available_inner)

# バタフライの概観プロット
plt.figure(figsize=(12, 6))
for t in df_bf.columns:
    plt.plot(df_bf.index, df_bf[t], label=t, alpha=0.7)
plt.title("OIS Butterfly Spreads (Inner Tenors)")
plt.legend()
plt.show()
```

---

## 5. ユーティリティ関数

```python
def frac_diff(series: pd.Series, d: float = 0.4, window: int = 50) -> pd.Series:
    weights = [1.0]
    for k in range(1, window):
        weights.append(-weights[-1] * (d - k + 1) / k)
    weights = np.array(weights[::-1])
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

---

## 6. 特徴量生成・プーリング

### 6-1. 共通特徴量（全行で同じ値）

```python
MACRO_COLS = ['USDJPY', 'DXY', 'nikkei', 'ust10y']
D, WINDOW = 0.4, 50

# マクロ: 分数階差のみ（水準は非定常のため使わない）
macro_fd = {f'{m}_fd': frac_diff(df_all[m], D, WINDOW)
            for m in MACRO_COLS if m in df_all.columns}

# 共通コンテキスト（水準のまま）
df_all['front_spread'] = df_all['3M'] - df_all['policy_rate']   # M1_spread 相当
df_all['slope_3m7y']   = df_all['7Y'] - df_all['3M']            # Slope_M1M8 相当
```

### 6-2. バタフライの分数階差（内側テナー分）

```python
bf_fd = {t: frac_diff(df_bf[t], D, WINDOW) for t in df_bf.columns}
```

### 6-3. プーリング関数

`config` 引数で2通りの特徴量セットを切り替える。

```python
def build_pooled_df(
    df_all: pd.DataFrame,
    df_bf: pd.DataFrame,
    bf_fd: dict,
    macro_fd: dict,
    inner_tenors: list,
    h: int,
    config: str,   # 'A': bf_level 1本のみ,  'B': 全内側テナーのbf_levelを追加
) -> pd.DataFrame:
    """
    目的変数: バタフライ変化量 bf_{tenor}_{t+h} - bf_{tenor}_t

    特徴量（Config A）:
      - bf_level       : 予測対象テナーのバタフライ水準（1本）        ← Fly_Level
      - bf_fd          : 予測対象テナーのバタフライ分数階差（1本）     ← B{n}_frac_diff
      - front_spread   : OIS_3M - policy_rate（水準）                ← M1_spread
      - slope_3m7y     : OIS_7Y - OIS_3M（水準）                     ← Slope_M1M8
      - policy_rate    : 政策金利水準（水準）                         ← Actual_Policy_Rate
      - days_to_next_mpm                                             ← Days_to_MPM
      - USDJPY_fd, DXY_fd, nikkei_fd, ust10y_fd                     ← 外部指標FD
      - *_is_imputed
      - tenor_index    : カテゴリ変数                                ← Meeting_Index

    特徴量（Config B）:
      - Config A の全特徴量
      - bf_level_{tenor} × len(inner_tenors)本: 全内側テナーのバタフライ水準
      - bf_fd_{tenor}   × len(inner_tenors)本: 全内側テナーのバタフライ分数階差
    """
    all_rows = []

    for tenor in inner_tenors:
        if tenor not in df_bf.columns:
            continue

        bf_s = df_bf[tenor]
        target = bf_s.shift(-h) - bf_s   # バタフライ変化量（目的変数）

        row_data = {
            'bf_level':         bf_s,                          # 予測対象の水準
            'bf_fd':            bf_fd[tenor],                  # 予測対象のFD
            'front_spread':     df_all['front_spread'],        # 水準のまま
            'slope_3m7y':       df_all['slope_3m7y'],          # 水準のまま
            'policy_rate':      df_all['policy_rate'],         # 水準のまま
            'days_to_next_mpm': df_all['days_to_next_mpm'],
        }

        # Config B: 全内側テナーのバタフライ水準とFDを追加
        if config == 'B':
            for t in inner_tenors:
                if t in df_bf.columns:
                    row_data[f'bf_level_{t}'] = df_bf[t]
                    row_data[f'bf_fd_{t}']    = bf_fd[t]

        # マクロFD（分数階差のみ）
        for col, series in macro_fd.items():
            row_data[col] = series

        # 補完フラグ
        for flag_col in [c for c in df_all.columns if c.endswith('_is_imputed')]:
            row_data[flag_col] = df_all[flag_col]

        tmp = pd.DataFrame({'tenor_index': tenor, **row_data, 'target': target},
                           index=df_all.index)
        all_rows.append(tmp)

    df_long = pd.concat(all_rows).sort_index()

    # NaN除去（FDウォームアップ期間など）
    feat_cols = [c for c in df_long.columns if c not in ('tenor_index', 'target')]
    df_long = df_long.dropna(subset=feat_cols + ['target'])

    # MPM直後除外
    df_long = df_long.merge(
        df_all[['is_post_mpm']], left_index=True, right_index=True, how='left'
    )
    df_long = df_long[df_long['is_post_mpm'] == 0].drop(columns='is_post_mpm')

    # 目的変数の正規化（テナー別全期間std）
    df_long['target_std']  = df_long.groupby('tenor_index')['target'].transform('std')
    df_long['target_norm'] = df_long['target'] / df_long['target_std']

    return df_long
```

---

## 7. Walk-Forward 検証

```python
import lightgbm as lgb
import bisect
from scipy.stats import spearmanr

PARAMS = {
    'objective':        'regression',
    'metric':           'rmse',
    'verbosity':        -1,
    'boosting_type':    'gbdt',
    'random_state':     42,
    'learning_rate':    0.05,
    'num_leaves':       31,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq':     5,
    'min_data_in_leaf': 30,
    'lambda_l2':        1.0,
}
NUM_BOOST_ROUND  = 100
TEST_WINDOW_DAYS = 90
PURGE_DAYS       = 5
OOS_START        = '2024-01-01'

NON_FEATURE_COLS = {'tenor_index', 'target', 'target_std', 'target_norm'}

def walk_forward(df_long: pd.DataFrame, oos_start: str) -> pd.DataFrame:
    target_col   = 'target_norm'
    feature_cols = [c for c in df_long.columns if c not in NON_FEATURE_COLS]

    # tenor_index をカテゴリ型に変換（LightGBM に明示）
    df_long = df_long.copy()
    df_long['tenor_index'] = df_long['tenor_index'].astype('category')
    cat_features = ['tenor_index']

    unique_dates = sorted(df_long.index.unique())
    start_idx    = bisect.bisect_left(unique_dates, pd.to_datetime(oos_start))

    results, fold, current_idx = [], 0, start_idx

    while current_idx < len(unique_dates):
        train_end  = unique_dates[current_idx - 1]
        purge_end  = train_end - pd.Timedelta(days=PURGE_DAYS)
        test_start = unique_dates[current_idx]
        test_end   = test_start + pd.Timedelta(days=TEST_WINDOW_DAYS)

        train_mask = df_long.index <= purge_end
        test_mask  = (df_long.index >= test_start) & (df_long.index < test_end)

        if train_mask.sum() < 100 or test_mask.sum() < 10:
            current_idx = bisect.bisect_left(unique_dates, test_end)
            continue

        X_train, y_train = df_long.loc[train_mask, feature_cols], df_long.loc[train_mask, target_col]
        X_test,  y_test  = df_long.loc[test_mask,  feature_cols], df_long.loc[test_mask,  target_col]

        dtrain   = lgb.Dataset(X_train, label=y_train, categorical_feature=cat_features)
        model    = lgb.train(PARAMS, dtrain, num_boost_round=NUM_BOOST_ROUND)
        preds    = model.predict(X_test)
        train_ic = spearmanr(y_train, model.predict(X_train))[0]

        fold_df = df_long.loc[test_mask, ['tenor_index', 'target', 'target_std']].copy()
        fold_df['pred_norm'] = preds
        fold_df['pred']      = preds * fold_df['target_std']
        fold_df['actual']    = fold_df['target']
        fold_df['fold']      = fold
        fold_df['train_ic']  = train_ic
        results.append(fold_df)

        fold += 1
        current_idx = bisect.bisect_left(unique_dates, test_end)

    return pd.concat(results).reset_index(names='date') if results else pd.DataFrame()
```

---

## 8. IC集計

```python
def summarize_ic(results_df: pd.DataFrame, recent_n_folds: int = 3) -> dict:
    if results_df.empty:
        return {}
    actual, pred = results_df['actual'], results_df['pred_norm']

    ic_all, _ = spearmanr(actual, pred)

    # CS IC（同日に複数テナーをランク付け）← 主評価指標
    cs_ics = [spearmanr(g['actual'], g['pred_norm'])[0]
              for _, g in results_df.groupby('date') if len(g) >= 3]
    cs_ic = np.nanmean(cs_ics) if cs_ics else np.nan

    # TS IC（テナー別時系列IC）
    ts_ics = [spearmanr(g['actual'], g['pred_norm'])[0]
              for _, g in results_df.groupby('tenor_index') if len(g) >= 5]
    ts_ic = np.nanmean(ts_ics) if ts_ics else np.nan

    recent_folds = sorted(results_df['fold'].unique())[-recent_n_folds:]
    recent_df    = results_df[results_df['fold'].isin(recent_folds)]
    ic_recent, _ = spearmanr(recent_df['actual'], recent_df['pred_norm'])

    train_ic = results_df.groupby('fold')['train_ic'].first().mean()

    return dict(ic_all=ic_all, cs_ic=cs_ic, ts_ic=ts_ic,
                ic_recent=ic_recent, train_ic=train_ic, gap=train_ic - ic_all)
```

---

## 9. 実行・比較

```python
results, walk_results = {}, {}

for config in ['A', 'B']:
    print(f"Running Config {config}...")
    for h in [3, 5]:
        df_long = build_pooled_df(df_all, df_bf, bf_fd, macro_fd,
                                  available_inner, h=h, config=config)
        res = walk_forward(df_long, OOS_START)
        walk_results[(config, h)] = res
        results[(config, h)]      = summarize_ic(res)
        print(f"  h={h}: Global IC={results[(config,h)].get('ic_all',0):.3f}, "
              f"CS IC={results[(config,h)].get('cs_ic',0):.3f}, "
              f"Gap={results[(config,h)].get('gap',0):.3f}")

# 比較テーブル
rows = [{'Config': c, 'h': h,
         'Global IC': round(s.get('ic_all',    np.nan), 3),
         'CS IC':     round(s.get('cs_ic',     np.nan), 3),
         'TS IC':     round(s.get('ts_ic',     np.nan), 3),
         'Gap':       round(s.get('gap',       np.nan), 3),
         'ic_recent': round(s.get('ic_recent', np.nan), 3)}
        for (c, h), s in results.items()]
df_cmp = pd.DataFrame(rows).set_index(['Config', 'h'])
display(df_cmp)
```

---

## 10. 可視化

```python
# IC by fold（A/B × h=3/5 の4枚）
for h in [3, 5]:
    fig, axes = plt.subplots(1, 2, figsize=(14, 4), sharey=True)
    for ax, config in zip(axes, ['A', 'B']):
        res = walk_results[(config, h)]
        if res.empty: continue
        fold_ics = [spearmanr(g['actual'], g['pred_norm'])[0]
                    for _, g in res.groupby('fold')]
        ax.bar(range(len(fold_ics)), fold_ics)
        ax.axhline(0, color='red', linewidth=0.8, linestyle='--')
        ax.set_title(f"Config {config} (h={h}) IC by Fold")
        ax.set_xlabel("Fold")
    axes[0].set_ylabel("Spearman IC")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}bf_base_ic_by_fold_{h}d.png", dpi=150)
    plt.show()

# IC by tenor（A/B × h=3/5）
for h in [3, 5]:
    fig, axes = plt.subplots(1, 2, figsize=(14, 4), sharey=True)
    for ax, config in zip(axes, ['A', 'B']):
        res = walk_results[(config, h)]
        if res.empty: continue
        tenor_ics = res.groupby('tenor_index').apply(
            lambda x: spearmanr(x['actual'], x['pred_norm'])[0], include_groups=False)
        tenor_ics.plot(kind='bar', ax=ax)
        ax.axhline(0, color='red', linewidth=0.8, linestyle='--')
        ax.set_title(f"Config {config} (h={h}) IC by Tenor")
    axes[0].set_ylabel("Spearman IC")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}bf_base_ic_by_tenor_{h}d.png", dpi=150)
    plt.show()

# Feature Importance（Config A・B を全データで再学習）
for config in ['A', 'B']:
    df_long = build_pooled_df(df_all, df_bf, bf_fd, macro_fd,
                              available_inner, h=3, config=config)
    feat_cols = [c for c in df_long.columns if c not in NON_FEATURE_COLS]
    df_long_copy = df_long.copy()
    df_long_copy['tenor_index'] = df_long_copy['tenor_index'].astype('category')
    dtrain = lgb.Dataset(df_long_copy[feat_cols], label=df_long_copy['target_norm'],
                         categorical_feature=['tenor_index'])
    model  = lgb.train(PARAMS, dtrain, num_boost_round=NUM_BOOST_ROUND)
    imp = pd.DataFrame({'feature': feat_cols,
                        'importance': model.feature_importance(importance_type='gain')}
                       ).sort_values('importance', ascending=False)
    plt.figure(figsize=(10, 6))
    sns.barplot(data=imp.head(20), x='importance', y='feature')
    plt.title(f"Feature Importance - Config {config} (h=3)")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}bf_base_importance_{config}.png", dpi=150)
    plt.show()
```

---

## 11. ノートブック構成

| # | セルの内容 |
|---|-----------|
| 1 | import、定数定義（ALL_TENORS, INNER_TENORS, D, WINDOW, OOS_START） |
| 2 | データ取得（OIS, FX, Nikkei, UST10Y, BOJ） + 利用可能テナー確認 |
| 3 | outer join + MICE補完 + 補完フラグ生成 |
| 4 | 日銀カレンダー特徴量（policy_rate forward fill, days_to_next_mpm, is_post_mpm） |
| 5 | `compute_butterfly` 定義・実行 + バタフライ概観プロット |
| 6 | `frac_diff` 定義、共通特徴量（front_spread, slope_3m7y, macro_fd, bf_fd）計算 |
| 7 | `build_pooled_df` 定義 |
| 8 | `walk_forward`, `summarize_ic` 定義 |
| 9 | Config A / B × h=3 / 5 を実行して比較テーブル出力 |
| 10 | IC by fold プロット（h=3, h=5） |
| 11 | IC by tenor プロット（h=3, h=5） |
| 12 | Feature Importance プロット（Config A・B） |
| 13 | 結果サマリーテキストセル（下記フォーマットで記入） |

---

## 12. 出力ファイル

`research/gbdt/outputs/` に保存：

- `bf_base_ic_by_fold_3d.png`
- `bf_base_ic_by_fold_5d.png`
- `bf_base_ic_by_tenor_3d.png`
- `bf_base_ic_by_tenor_5d.png`
- `bf_base_importance_A.png`
- `bf_base_importance_B.png`

---

## 13. 結果記録フォーマット（ノートブック末尾テキストセルに記入）

```
モデル名: spot_rate_bf_base
対象テナー（内側）: 6M, 9M, 1Y, 18M, 2Y, 3Y, 4Y, 5Y, 6Y
目的変数: バタフライ変化量（h日後）
OOS開始日: 2024-01-01
rounds: 100

--- h=3 ---
Config A (bf_level 1本): Global IC=0.XXX, CS IC=0.XXX, TS IC=0.XXX, Gap=0.XXX
Config B (bf_level 全本): Global IC=0.XXX, CS IC=0.XXX, TS IC=0.XXX, Gap=0.XXX

--- h=5 ---
Config A (bf_level 1本): Global IC=0.XXX, CS IC=0.XXX, TS IC=0.XXX, Gap=0.XXX
Config B (bf_level 全本): Global IC=0.XXX, CS IC=0.XXX, TS IC=0.XXX, Gap=0.XXX

採用: （CS ICで最も改善したConfigを記載）
特記事項:
- 18M, 4Y, 6Y の欠損状況
- テナー別IC の傾向
```

---

## 14. 注意事項

| 罠 | 対策 |
|----|------|
| `18M`, `4Y`, `6Y` が DB に存在しないと butterfly が計算できない | セル2で確認し、存在しないテナーを INNER_TENORS から除外して続行 |
| `tenor_index` をカテゴリ変数として LightGBM に渡し忘れる | `lgb.Dataset` の `categorical_feature=['tenor_index']` を必ず指定 |
| バタフライのFDを生OISから計算してしまう | FDは必ず `df_bf`（バタフライ後の系列）に対して適用する |
| 目的変数が生OIS変化量のままになっている | `target = bf_s.shift(-h) - bf_s` を確認（`df_all[tenor]` ではなく `df_bf[tenor]`） |
| Config B でガイドの「カーブ形状追加→IC低下」が再現した場合 | Gap が拡大しているか確認。Config A を採用する根拠になる |
| `is_post_mpm` の merge で index がずれる | `left_index=True, right_index=True` で merge する |
