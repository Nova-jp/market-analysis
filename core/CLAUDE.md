# CLAUDE.md — core/ 共通コア層

全層から参照される唯一の真実の源（single source of truth）。
`api/`・`pipeline/` はここからのみ import する。`core/` は他の層を import しない。

---

## ディレクトリ構成

```
core/
├── config.py              # pydantic-settings による設定管理（環境変数）
├── db/
│   ├── engine.py          # SQLAlchemy async engine / session factory
│   ├── async_client.py    # 非同期 DB クライアント（FastAPI 専用）
│   ├── sync_client.py     # 同期 DB クライアント（pipeline/scripts 専用）
│   └── migrations/        # DDL SQL ファイル
├── models/
│   ├── schemas.py         # Pydantic レスポンスモデル（API 入出力）
│   └── orm.py             # SQLAlchemy ORM モデル（テーブル定義）
├── calculations/
│   ├── bond_math.py       # QuantLib 債券計算ヘルパー
│   ├── market_amount.py   # 市中残存額計算
│   └── pca.py             # PCA 主成分分析サービス
└── utils/
    ├── date_utils.py      # 営業日・祝日判定
    ├── jsda_parser.py     # JSDA CSV フォーマットパーサー
    └── column_mapping.py  # カラム名マッピング定数
```

---

## DB アクセスパターン

### FastAPI エンドポイント（非同期）

```python
from core.db.async_client import db_manager

@router.get("/example")
async def get_example():
    return await db_manager.execute_query("bond_data", {"limit": 100})
```

### パイプライン・スクリプト（同期）

```python
from core.db.sync_client import DatabaseManager

db = DatabaseManager()
result = db.get_bond_data(start_date="2024-01-01", end_date="2024-12-31")
```

- 接続プール: `pool_size=5`, `max_overflow=10`, `pool_recycle=1800`（Neon アイドル切断対策）
- SSL: Neon 接続時は `connect_args={"ssl": "require"}` を明示（`core/db/engine.py` 参照）

---

## 設定管理

```python
from core.config import settings

# 利用可能な設定値
settings.database_url        # Neon PostgreSQL URL
settings.private_username    # HTTP Basic Auth ユーザー名
settings.private_password    # HTTP Basic Auth パスワード
settings.environment         # "production" / "development"
```

---

## ALLOWED_TABLES

`sync_client.py` / `async_client.py` 共通のホワイトリスト。新テーブル追加時は両ファイルに追記し、`core/db/migrations/` に DDL を作成する。

```python
{'bond_data', 'bond_market_amount', 'bond_auction',
 'irs_data', 'ASW_data', 'economic_indicators',
 'boj_holdings', 'rate_predictions'}
```
