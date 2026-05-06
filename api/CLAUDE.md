# CLAUDE.md — api/ Web API 層

FastAPI アプリケーション。`core/` に依存し、`pipeline/` からは import しない。
エントリーポイント: `api/main.py` → `uvicorn api.main:app`

---

## ディレクトリ構成

```
api/
├── main.py              # FastAPI アプリ・ルーター統合・CORS・静的ファイル配信
├── dependencies.py      # 認証 dependency (HTTP Basic Auth)
├── routes/              # ルートハンドラー（1 ファイル = 1 機能）
│   ├── health.py
│   ├── dates.py
│   ├── yield_data.py
│   ├── scheduler.py
│   ├── pca.py
│   ├── market_amount.py
│   ├── private_analytics.py
│   └── export.py
└── services/            # ビジネスロジック（ルートから呼び出し）
    ├── scheduler.py     # Cloud Scheduler トリガー処理
    ├── asw.py           # ASW 計算サービス
    ├── jsda_volume.py   # JSDA 出来高サービス
    ├── irs.py           # IRS データサービス
    ├── macro.py         # マクロ指標サービス
    └── private_analysis.py
```

---

## エンドポイント追加手順

1. `api/routes/` に新ファイルを作成
2. `api/main.py` の `include_router` に追加
3. `core/models/schemas.py` にレスポンス Pydantic モデルを追加
4. ビジネスロジックが複雑なら `api/services/` に分離

```python
router = APIRouter(prefix="/api/feature-name", tags=["feature-name"])
```

---

## 認証パターン

プライベートエンドポイント（swap / forward-curve / private-analytics）は HTTP Basic Auth。

```python
from api.dependencies import get_current_username
from fastapi import Depends

@router.get("/private")
async def private_endpoint(username: str = Depends(get_current_username)):
    ...
```

- `PRIVATE_USERNAME` / `PRIVATE_PASSWORD` 環境変数が必須。未設定時は 503 を返す。
- `secrets.compare_digest` でタイミング攻撃対策済み。

---

## 分析ロジックの在処

| 機能 | 実装場所 |
|------|---------|
| ASW 算出 | `api/services/asw.py`（scipy CubicSpline 補間） |
| PCA | `core/calculations/pca.py` |
| QuantLib 計算 | `core/calculations/bond_math.py`（同期 → `run_in_threadpool` でラップ） |
| 市中残存額 | `core/calculations/market_amount.py` |

---

## セキュリティ制約

- `ALLOW_SCHEDULER_DEBUG=true` は本番環境（`environment=production`）で完全ブロック。
- CORS: 本番は Cloud Run ドメインのみ許可。開発は `localhost:3000` を許可。
- フロントエンドのスワップ・ASW カーブは簡易パスワード保護（`sessionStorage`、パスワード: `0720`）。
