# 設計書: 特徴量比較実験（Config A / B / C）

> **対象読者**: Gemini（実装担当）
> **目的**: 3種類の特徴量セットを同一条件で比較し、CS IC 改善の鍵を探る。
> **前提**: `spot_rate_7y_base.ipynb` のデータ取得・前処理・walk-forward の実装を再利用すること。
> **出力ファイル**: `research/gbdt/notebooks/spot_rate_7y_compare.ipynb`

---

## 1. 比較する3つの特徴量セット

### Config A（ベースライン・再実行）

`spot_rate_7y_base.ipynb` と同じ特徴量。比較の基準として同一ノートブック内で再実行する。

| 特徴量 | 内容 |
|-------|------|
| `ois_level` | OIS絶対水準（補完済み） |
| `ois_spread_h` | `OIS_t - OIS_{t-h}`（h日前との差分） |
| `ois_fd` | OIS絶対水準の分数階差（d=0.4） |
| `tenor_val` | テナーを数値化 |
| `policy_rate`, `days_to_next_mpm` | 日銀カレンダー |
| `USDJPY_fd`, `DXY_fd`, `nikkei_fd`, `ust10y_fd` | マクロ変数の分数階差 |
| `*_is_imputed` | 補完フラグ |

---

### Config B（Config A ＋ 全テナーのバタフライスプレッドを追加）

Config A の全特徴量に加えて、**全テナーのバタフライスプレッド水準と分数階差**（各8本）を追加する。
OISの特徴量は削除しない。

| 特徴量 | 内容 |
|-------|------|
| Config A の全特徴量 | （上記 A と同じ） |
| `bf_all_{tenor}` × 8本 | 全テナーのバタフライスプレッド水準（`bf_all_3M` 〜 `bf_all_7Y`） |
| `bf_all_{tenor}_fd` × 8本 | 全テナーのバタフライスプレッドの分数階差 |

---

### Config C（Config A ＋ 目的テナーのバタフライのみ追加）

Config A の全特徴量に加えて、**予測対象テナー自身のバタフライスプレッドの水準と分数階差**を2本追加する。

| 特徴量 | 内容 |
|-------|------|
| Config A の全特徴量 | （上記 A と同じ） |
| `bf_target_level` | 予測対象テナーのバタフライスプレッド水準 |
| `bf_target_fd` | 予測対象テナーのバタフライスプレッドの分数階差 |

---

## 2. バタフライスプレッドの定義

テナー順序: `['3M', '6M', '9M', '1Y', '2Y', '3Y', '5Y', '7Y']`

中間テナー（両隣が存在する場合）は通常定義、端点テナーは片側定義を使う：

```python
TENOR_ORDER = ['3M', '6M', '9M', '1Y', '2Y', '3Y', '5Y', '7Y']

def compute_butterfly(df_ois: pd.DataFrame, tenor_order: list) -> pd.DataFrame:
    """
    各テナーのバタフライスプレッドを計算して返す。
    中間テナー: BF_m = OIS_m - 0.5 * (OIS_prev + OIS_next)
    端点テナー: 片側差分（短端: OIS_3M - OIS_6M、長端: OIS_7Y - OIS_5Y）
    """
    bf = pd.DataFrame(index=df_ois.index)
    n = len(tenor_order)
    for i, t in enumerate(tenor_order):
        if t not in df_ois.columns:
            continue
        if i == 0:
            # 短端（3M）: 片側
            next_t = tenor_order[i + 1]
            bf[t] = df_ois[t] - df_ois[next_t]
        elif i == n - 1:
            # 長端（7Y）: 片側
            prev_t = tenor_order[i - 1]
            bf[t] = df_ois[t] - df_ois[prev_t]
        else:
            # 中間テナー: 両側平均
            prev_t = tenor_order[i - 1]
            next_t = tenor_order[i + 1]
            bf[t] = df_ois[t] - 0.5 * (df_ois[prev_t] + df_ois[next_t])
    return bf
```

**注意**: `compute_butterfly` は MICE 補完後の `df_ois`（欠損なし）に対して実行すること。

---

## 3. 特徴量生成関数

Config A の `build_pooled_df` をベースに、config 引数で切り替える形で実装する。

```python
def build_pooled_df(
    df_all: pd.DataFrame,
    df_bf: pd.DataFrame,       # バタフライスプレッドのワイド形式DataFrame（compute_butterfly の出力）
    ois_cols: list,
    macro_cols: list,
    h: int,
    config: str,               # 'A', 'B', 'C' のいずれか
) -> pd.DataFrame:
    """
    config に応じて特徴量を切り替えてロング形式DataFrameを返す。
    目的変数は常に OIS_{t+h} - OIS_t（OIS の変化量）。
    """
    macro_fd = {f'{m}_fd': frac_diff(df_all[m], D, WINDOW)
                for m in macro_cols if m in df_all.columns}

    # バタフライの分数階差を事前計算（Config B, C で使用）
    bf_fd_dict = {}
    for t in ois_cols:
        if t in df_bf.columns:
            bf_fd_dict[t] = frac_diff(df_bf[t], D, WINDOW)

    all_rows = []
    for tenor in ois_cols:
        s    = df_all[tenor]           # OIS 絶対水準
        bf_s = df_bf.get(tenor, None)  # バタフライスプレッド水準

        # --- 目的変数（共通） ---
        target = s.shift(-h) - s

        # --- Config 別 特徴量 ---
        if config == 'A':
            row_data = {
                'ois_level':    s,
                'ois_spread_h': s - s.shift(h),
                'ois_fd':       frac_diff(s, D, WINDOW),
            }
        elif config == 'B':
            # Config A の全特徴量をベースに、全テナーのBFを追加
            row_data = {
                'ois_level':    s,
                'ois_spread_h': s - s.shift(h),
                'ois_fd':       frac_diff(s, D, WINDOW),
            }
            # 全テナーのBF水準とFDを追加（カラム名に tenor 名を含める）
            for t in ois_cols:
                if t in df_bf.columns:
                    row_data[f'bf_all_{t}']    = df_bf[t]
                    row_data[f'bf_all_{t}_fd'] = bf_fd_dict.get(t, pd.Series(np.nan, index=df_all.index))
        elif config == 'C':
            row_data = {
                'ois_level':        s,
                'ois_spread_h':     s - s.shift(h),
                'ois_fd':           frac_diff(s, D, WINDOW),
                'bf_target_level':  bf_s if bf_s is not None else np.nan,
                'bf_target_fd':     bf_fd_dict.get(tenor, pd.Series(np.nan, index=df_all.index)),
            }

        tmp = pd.DataFrame({
            'tenor':            tenor,
            'tenor_val':        tenor_to_float(tenor),
            **row_data,
            'policy_rate':      df_all.get('policy_rate',       pd.Series(np.nan, index=df_all.index)),
            'days_to_next_mpm': df_all.get('days_to_next_mpm',  pd.Series(np.nan, index=df_all.index)),
            'is_post_mpm':      df_all['is_post_mpm'],
            'target':           target,
        }, index=df_all.index)

        for col, fd_series in macro_fd.items():
            tmp[col] = fd_series

        for flag_col in [c for c in df_all.columns if c.endswith('_is_imputed')]:
            tmp[flag_col] = df_all[flag_col]

        all_rows.append(tmp)

    df_long = pd.concat(all_rows).sort_index()
    feat_cols = [c for c in df_long.columns if c not in ('tenor', 'is_post_mpm', 'target')]
    df_long = df_long.dropna(subset=feat_cols + ['target'])
    df_long = df_long[df_long['is_post_mpm'] == 0].drop(columns='is_post_mpm')

    df_long['target_std']  = df_long.groupby('tenor')['target'].transform('std')
    df_long['target_norm'] = df_long['target'] / df_long['target_std']

    return df_long
```

---

## 4. walk_forward と summarize_ic

`spot_rate_7y_base.ipynb` の実装をそのままコピーして使用すること（変更不要）。

```python
# OOS 開始日・ウォークフォワードパラメータは base と統一する
OOS_START        = '2024-01-01'
TEST_WINDOW_DAYS = 90
PURGE_DAYS       = 5
NUM_BOOST_ROUND  = 100
```

---

## 5. 実行と比較

全6パターン（Config A/B/C × h=3/5）を順番に実行する。

```python
results = {}

for config in ['A', 'B', 'C']:
    df_long_3d = build_pooled_df(df_all, df_bf, OIS_COLS, MACRO_COLS, h=3, config=config)
    df_long_5d = build_pooled_df(df_all, df_bf, OIS_COLS, MACRO_COLS, h=5, config=config)

    res_3d = walk_forward(df_long_3d, OOS_START)
    res_5d = walk_forward(df_long_5d, OOS_START)

    results[(config, 3)] = summarize_ic(res_3d)
    results[(config, 5)] = summarize_ic(res_5d)
    print(f"Config {config} done.")
```

---

## 6. 比較テーブルの出力

```python
rows = []
for (config, h), stats in results.items():
    rows.append({
        'Config': config,
        'h': h,
        'Global IC': round(stats.get('ic_all',    np.nan), 3),
        'CS IC':     round(stats.get('cs_ic',     np.nan), 3),
        'TS IC':     round(stats.get('ts_ic',     np.nan), 3),
        'Gap':       round(stats.get('gap',       np.nan), 3),
        'ic_recent': round(stats.get('ic_recent', np.nan), 3),
    })

df_comparison = pd.DataFrame(rows).set_index(['Config', 'h'])
display(df_comparison)
```

期待する出力形式：

```
               Global IC  CS IC  TS IC   Gap  ic_recent
Config h
A      3         0.193   0.010  0.200  0.093      0.162
       5         0.152  -0.022  0.165  0.191      0.158
B      3           ...     ...    ...    ...        ...
       5           ...     ...    ...    ...        ...
C      3           ...     ...    ...    ...        ...
       5           ...     ...    ...    ...        ...
```

---

## 7. 可視化

各Configの ic_by_fold と ic_by_tenor を並べて比較する。

```python
# IC by fold の比較（h=3, Config A/B/C を1枚に並べる）
fig, axes = plt.subplots(1, 3, figsize=(18, 4), sharey=True)
for ax, config in zip(axes, ['A', 'B', 'C']):
    res = walk_results[(config, 3)]  # 実行済みDataFrame
    fold_ics = [spearmanr(g['actual'], g['pred_norm'])[0]
                for _, g in res.groupby('fold')]
    ax.bar(range(len(fold_ics)), fold_ics)
    ax.set_title(f"Config {config} (h=3) IC by Fold")
    ax.set_xlabel("Fold")
axes[0].set_ylabel("Spearman IC")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}compare_ic_by_fold_3d.png", dpi=150)
plt.show()
```

同様に h=5 と ic_by_tenor も出力すること。

---

## 8. ノートブック構成

| # | セルの内容 |
|---|-----------|
| 1 | ライブラリ import、定数定義（`spot_rate_7y_base.ipynb` から流用） |
| 2 | データ取得・MICE補完・日銀カレンダー生成（同上、再実行） |
| 3 | `compute_butterfly` でバタフライスプレッドを計算・概観プロット |
| 4 | `frac_diff`, `tenor_to_float`, `build_pooled_df` の定義 |
| 5 | `walk_forward`, `summarize_ic` の定義（base から流用） |
| 6 | Config A（h=3, h=5）実行 |
| 7 | Config B（h=3, h=5）実行 |
| 8 | Config C（h=3, h=5）実行 |
| 9 | 比較テーブル出力 |
| 10 | IC by fold 比較プロット（h=3 と h=5 それぞれ） |
| 11 | IC by tenor 比較プロット（h=3 と h=5 それぞれ） |
| 12 | Feature Importance プロット（Config A/B/C それぞれ全データで再学習） |
| 13 | 結果サマリーテキストセル（下記フォーマットで記入） |

---

## 9. 出力ファイル

以下を `research/gbdt/outputs/` に保存すること：

- `compare_ic_by_fold_3d.png`
- `compare_ic_by_fold_5d.png`
- `compare_ic_by_tenor_3d.png`
- `compare_ic_by_tenor_5d.png`
- `compare_feature_importance_A.png`
- `compare_feature_importance_B.png`
- `compare_feature_importance_C.png`

---

## 10. 結果記録フォーマット（ノートブック末尾テキストセルに記入）

```
実験名: spot_rate_7y_compare
対象テナー: 3M, 6M, 9M, 1Y, 2Y, 3Y, 5Y, 7Y
OOS開始日: 2024-01-01
rounds: 100

--- h=3 ---
Config A: Global IC=0.XXX, CS IC=0.XXX, TS IC=0.XXX, Gap=0.XXX
Config B: Global IC=0.XXX, CS IC=0.XXX, TS IC=0.XXX, Gap=0.XXX
Config C: Global IC=0.XXX, CS IC=0.XXX, TS IC=0.XXX, Gap=0.XXX

--- h=5 ---
Config A: Global IC=0.XXX, CS IC=0.XXX, TS IC=0.XXX, Gap=0.XXX
Config B: Global IC=0.XXX, CS IC=0.XXX, TS IC=0.XXX, Gap=0.XXX
Config C: Global IC=0.XXX, CS IC=0.XXX, TS IC=0.XXX, Gap=0.XXX

採用: （CS ICで最も改善したConfigを記載）
特記事項:
- （バタフライの効き、テナー別の差など気づいたことを記録）
```

---

## 11. 注意事項

| 注意点 | 対策 |
|-------|------|
| Config A の結果が base と完全一致するはず | 一致しない場合はデータ取得やシードの差を確認 |
| バタフライの分数階差は補完後の OIS から計算する | MICE 補完 → butterfly → frac_diff の順序を守る |
| Config B で `bf_s` が None になる場合 | `ois_cols` と `df_bf.columns` の不一致を確認 |
| 6つのパターンで実行時間が長くなる場合 | h=3 の3 Config を先に実行して傾向を確認してから h=5 に進む |
