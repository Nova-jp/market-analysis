# スポットレート予測モデル 引き継ぎ文書

> **対象読者**: 別プロジェクト内の Claude。BOJ_analysis プロジェクトで確立したモデリング手法を、スポット金利予測モデルに移植するための引き継ぎ。

---

## 1. プロジェクト概要（移植元: BOJ_analysis）

BOJ_analysis では、日銀 OIS スワップレート（M1〜M8: 第1〜第8会合先）の MPM（金融政策決定会合）前後の変動を LightGBM（GBDT）で予測している。

今回の新プロジェクトでは、同じ手法でスポット金利（T12/T18/T24 等のテナー OIS）を予測する。評価指標・ウォークフォワード・ターゲット生成の仕組みをそのまま横流しする。

---

## 2. 移植するコアコンポーネント

### 2-1. 目的変数（ターゲット）

**3日後・5日後の変化量（bp）を予測する**。

```python
# h = 3 または 5
Target_{h}d = レート_{t+h} - レート_{t}
```

ターゲットは系列ごとに全期間 std で正規化してモデルに渡す（異分散補正）。
逆正規化用に std も保持する。

```python
for h in [3, 5]:
    instr_std = df.groupby('Rate_Label')[f'Target_{h}d'].transform('std')
    df[f'Target_{h}d_std']  = instr_std
    df[f'Target_{h}d_norm'] = df[f'Target_{h}d'] / instr_std
```

- モデルの学習・評価は `Target_{h}d_norm`（正規化済み）で行う
- IC 評価は Spearman ランク相関なので定数倍に不変。正規化の有無で IC は変わらない
- 逆正規化して bp 単位に戻すには `pred * Target_{h}d_std`

### 2-2. 評価指標（IC）

**IC = Spearman ランク相関係数**（方向一致度を評価）。

3種類の IC を使い分ける：

| IC 種別 | 計算方法 | 用途 |
|---------|---------|------|
| **Global IC** | 全（日付 × 系列）を一括 Spearman | 全体的な予測力 |
| **CS IC** (Cross-Sectional) | 各日付で系列をランク付けし日次平均 | 相対価値戦略向け。市場トレンドの影響を除去 |
| **TS IC** (Time-Series) | 各系列の時系列 IC を系列平均 | 診断用。トレンド相場で水増しされやすい |

**CS IC と Global IC の乖離が大きい場合**、アルファの大部分が市場方向のベータである可能性を示す（CS IC が真の実力に近い）。

```python
from scipy.stats import spearmanr

# Global IC
ic_all, _ = spearmanr(results['Actual'], results['Pred'])

# CS IC（各日付で系列をランク付け）
cs_ics = []
for date, grp in results.groupby('Date'):
    if len(grp) >= 3:  # 最低3系列必要
        ic, _ = spearmanr(grp['Actual'], grp['Pred'])
        if not np.isnan(ic):
            cs_ics.append(ic)
cs_ic = np.mean(cs_ics)

# TS IC（各系列の時系列 IC）
ts_ics = []
for label, grp in results.groupby('Rate_Label'):
    if len(grp) >= 5:
        ic, _ = spearmanr(grp['Actual'], grp['Pred'])
        if not np.isnan(ic):
            ts_ics.append(ic)
ts_ic = np.mean(ts_ics)
```

**Gap = Train IC - Global IC**：過学習の指標。0.10 程度なら許容範囲。0.20 超は要注意。

### 2-3. ウォークフォワード検証

**拡張窓（expanding window）+ パージ付きウォークフォワード**を採用する。

```
全期間: ──────────────────────────────────────────▶
        [  Train (拡張)  ][ purge ][ Test 90d ]
                                    [  Train (拡張) ][ purge ][ Test 90d ]
                                                               ...
```

パラメータ（BOJ_analysis 確定値）：
- `test_window_days = 90`（3ヶ月）
- `purge_days = 5`（テストウィンドウ直前5日を学習から除外。ターゲットのリーク防止）
- `start_date`：OOS 開始日（例: `2024-01-01`）
- `num_boost_round = 100`（固定。early stopping なし）

**early stopping を使わない理由**：金融時系列の非定常性（例: ゼロ金利期 → 利上げ期）により、バリデーション窓との分布ずれで `best_iteration=1` で止まるフォールドが多発し IC が不安定になるため。固定ラウンド数 + 正則化で汎化を担保する。

実装の骨格：
```python
unique_dates = sorted(df['Date'].unique())
# start_date 以降を OOS にする
test_start_idx = next(i for i, d in enumerate(unique_dates) if d >= pd.to_datetime(start_date))

current_idx = test_start_idx
results = []
fold = 0

while current_idx < len(unique_dates):
    train_end   = unique_dates[current_idx - 1]
    test_start  = unique_dates[current_idx]
    purge_end   = train_end - pd.Timedelta(days=purge_days)

    train_mask  = dates <= purge_end
    test_mask   = (dates >= test_start) & (dates < test_start + pd.Timedelta(days=test_window_days))

    # 学習・予測
    model = train_lgbm(X[train_mask], y[train_mask])
    preds = model.predict(X[test_mask])

    results.append(pd.DataFrame({
        'Fold': fold, 'Date': dates[test_mask],
        'Rate_Label': labels[test_mask],
        'Actual': y[test_mask], 'Pred': preds,
        'Train_IC': spearmanr(y[train_mask], model.predict(X[train_mask]))[0],
    }))

    fold += 1
    next_date = test_start + pd.Timedelta(days=test_window_days)
    current_idx = bisect.bisect_left(unique_dates, next_date)
    if current_idx >= len(unique_dates):
        break

results_df = pd.concat(results).reset_index(drop=True)
```

### 2-4. LightGBM パラメータ

```python
params = {
    'objective': 'regression',
    'metric': 'rmse',
    'verbosity': -1,
    'boosting_type': 'gbdt',
    'random_state': 42,
    'learning_rate': 0.05,
    'num_leaves': 31,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'min_data_in_leaf': 30,
    'lambda_l2': 1.0,
}
```

系列識別子（`Rate_Label` やインデックス）はカテゴリ変数として LightGBM に明示する。

---

## 3. 特徴量エンジニアリングの設計方針

### 3-1. 基本方針

- **スプレッドベース**：絶対水準でなく政策金利差（`レート - 政策金利`）を特徴量の基底にする
  - 政策金利変動の影響を除去し、モデルが「カーブ形状の相対変化」を学習できる
- **分数階差（Fractional Differencing）**：定常化しつつ過去情報を保持
  - `d=0.4, window=50` が BOJ_analysis での確定値
  - `d=1.0`（通常差分）は情報を捨てすぎ、`d=0.0`（原系列）は非定常性を持ち込む

```python
def frac_diff(series, d=0.4, window=50):
    """
    分数階差: w_k = (-1)^k * C(d, k)
    先頭 window-1 行は NaN（ウォームアップ期間）
    """
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

### 3-2. 欠損補完

- **MICE（Multiple Imputation by Chained Equations）**で BayesianRidge を使用
- 補完した行には `{col}_is_imputed` フラグを付け、特徴量として渡す（補完箇所をモデルに伝える）
- MICE は全期間で `fit_transform`（フォールドをまたいでも許容）：金利間の相関構造は長期安定のため

```python
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.linear_model import BayesianRidge

for col in rate_cols:
    df[f'{col}_is_imputed'] = df[col].isnull().astype(int)  # 補完前にフラグ

imputer = IterativeImputer(estimator=BayesianRidge(), max_iter=20, random_state=42)
df[rate_cols] = imputer.fit_transform(df[rate_cols])
```

### 3-3. MPM 直後除外フラグ

会合翌日から5日間は目的変数が不安定（MPMジャンプの影響）なので学習から除外する。

```python
# Is_Meeting_Day = 会合日フラグ（0/1）
df['is_post_mpm'] = (
    df['Is_Meeting_Day'].rolling(window=6, min_periods=1).max() == 1
).astype(int)
# 学習・評価時: df = df[df['is_post_mpm'] == 0]
```

---

## 4. プーリング設計

複数の系列を「縦積み（ロング形式）」にして1モデルで学習することで、系列間の共通パターンを学習し、学習データ量を増やす。

```python
# ワイド形式 → ロング形式
pooled = df.melt(
    id_vars=id_cols,        # Date, 外部指標, 政策金利等
    value_vars=rate_cols,   # 予測対象の系列名
    var_name='Rate_Label',
    value_name='Rate_Value',
)
# Rate_Label を数値インデックスに変換してカテゴリ特徴量として使う
pooled['Series_Index'] = pooled['Rate_Label'].map(label_to_index)
```

系列インデックス（`Series_Index`）はカテゴリ変数として渡すことで、LightGBM がカテゴリ別に分岐ルールを学習できる。

---

## 5. 特徴量除外リスト方式

モデルに渡す特徴量は「除外リスト」で管理する（ホワイトリストではなくブラックリスト）。新しい特徴量を追加したとき、除外リストに追加し忘れるサイレントバグを防ぐ。

```python
NON_FEATURE_COLS = {
    'Date', 'Rate_Label', 'Rate_Value', 'is_post_mpm',
    'Target_1d',
    'Target_3d', 'Target_3d_norm', 'Target_3d_std',
    'Target_5d', 'Target_5d_norm', 'Target_5d_std',
}

feature_cols = [c for c in df.columns if c not in NON_FEATURE_COLS and c != target_col]
```

---

## 6. 結果の整理（summarize_ic）

ウォークフォワードの結果を以下の形式でまとめる：

```python
def summarize_ic(results_df, recent_n_folds=3):
    """
    Returns:
        ic_all:      Global OOS IC（全フォールド合算 Spearman）
        ic_recent:   直近 N フォールドの Global IC
        cs_ic:       Cross-Sectional IC
        ts_ic:       Time-Series IC
        train_ic:    フォールド平均 Train IC
        gap:         train_ic - ic_all（過学習度）
        ic_by_fold:  フォールド番号 → IC の dict
    """
```

実験間の比較は必ず同一の `start_date` と `test_window_days` で行う。

---

## 7. 実験のベースライン記録方法

各実験ノートブックは以下のフォーマットで結果を記録する：

```
モデル名: xxx
start_date: 2024-01-01
rounds: 100

3d: Global IC=0.xxx, CS IC=0.xxx, TS IC=0.xxx, Gap=0.xxx, ic_recent=0.xxx
5d: Global IC=0.xxx, CS IC=0.xxx, TS IC=0.xxx, Gap=0.xxx, ic_recent=0.xxx
```

採用基準: OOS IC（または CS IC）が ベースラインより **+0.01 以上改善**かつ全フォールドで安定。

---

## 8. よくある設計上の罠

| 罠 | 対策 |
|----|------|
| 「最終フォールドのみの IC」を報告する | 必ず全 OOS フォールドを合算した Global IC を使う |
| early stopping で `best_iteration=1` になる | early stopping を使わず固定ラウンド（100）にする |
| Global IC が高いが CS IC が低い | トレンドのベータを見ている。CS IC を優先指標にする |
| MICE の `is_imputed` フラグを渡し忘れる | 補完前にフラグを生成し、特徴量リストに含める |
| MPM 直後の不安定期を学習に混入 | `is_post_mpm == 0` のフィルタを必ず適用する |
| ターゲットの std を計算するタイミング | プーリング後（系列別）に全期間 std で計算する |
| 分数階差のウォームアップ期間 | window=50 の先頭49行は NaN → 学習時に dropna で除外される |

---

## 9. 移植時の適合作業

BOJ_analysis はデータを Excel から読み込んでいるが、新プロジェクトでは DB から取得する。以下の部分を DB のスキーマに合わせて書き直す：

1. **データ読み込み部分**（`processing.py` 相当）：DB クエリに置き換える
2. **カラム名のリネーム**：DB のカラム名に合わせる
3. **MICE 補完の対象列**：DB の欠損パターンに応じて調整する
4. **政策金利の取得**：DB から取得する（現在は `BOJ_meeting_history.csv`）

上記以外（ウォークフォワード・IC 計算・LightGBM パラメータ・分数階差）はそのままコピーして使える。

---

## 10. 参照コード（BOJ_analysis プロジェクト）

| ファイル | 移植対象の内容 |
|---------|-------------|
| `src/modeling.py` | walk_forward_validation, summarize_ic, train_model, calculate_metrics |
| `src/features.py` | frac_diff（分数階差の実装） |
| `src/features_rv.py` | generate_rv_features（スプレッド・C/B 系列の生成例） |
| `src/pooling_butterfly.py` | pool_butterfly_data（プーリング・ターゲット生成の例） |
| `src/processing.py` | MICE 補完・is_post_mpm フラグ・Days_to_MPM 計算 |
