# CLAUDE.md — pipeline/ データパイプライン層

外部ソースからのデータ収集・変換・DB 投入を担う。
`core/` に依存するが、`api/` からは import しない（一方通行）。

---

## ディレクトリ構成

```
pipeline/
├── fetchers/            # データ収集（外部ソース別）
│   ├── jsda/            # 国債店頭売買高・利回り（JSDA）
│   ├── jscc/            # IRS/OIS（JSCC/JPX）
│   ├── mof/             # 財務省（入札結果・流動性供給）
│   ├── boj/             # 日本銀行（保有状況）
│   └── macro/           # マクロ経済指標（Yahoo Finance / FRED）
└── jobs/                # バッチジョブエントリーポイント
    ├── collect_bonds.py        # JSDA 複数日収集
    ├── collect_bond_single.py  # JSDA 単日収集
    ├── collect_daily.py        # 日次一括収集
    ├── daily_irs.py            # IRS 日次
    ├── daily_macro.py          # マクロ指標日次
    └── ...
```

---

## DB アクセスパターン

`core/db/sync_client.py` の `DatabaseManager` のみ使用。

```python
from core.db.sync_client import DatabaseManager

db = DatabaseManager()
result = db.batch_insert_data(records, table_name='bond_data')
```

---

## コレクター規約

```python
class XxxCollector:
    def collect(self, date: str) -> List[Dict]:
        ...  # エラー時は logger.error → 空リストを返す（例外は上位へ伝播）
```

---

## 🚨 JSDA サーバー保護（詳細は scripts/CLAUDE.md）

- アクセス間隔: **5 秒以上**（推奨 30 秒以上）
- `&` / `nohup` によるバックグラウンド実行は絶対禁止
- 実行前に `ps aux | grep python` でプロセス重複確認

---

## データソース一覧

| ソース | 収集内容 | 更新タイミング |
|--------|----------|---------------|
| JSDA | 国債店頭売買高・利回りデータ | 毎日 17:30〜18:30 |
| JSCC/JPX | IRS（金利スワップ）データ | 毎日 21:00 頃 |
| MOF | 入札結果・流動性供給入札 | 入札日 |
| BOJ | 国債保有状況 | 月次 |
| Yahoo Finance | 株価・為替（USD/JPY 等） | 日次 |
| FRED | 米国経済統計（FFR 等） | 日次 |

---

## Cloud Scheduler との連携

現行: `Cloud Scheduler → POST /api/scheduler/daily-collection → api/services/scheduler.py → pipeline`

`pipeline/jobs/*.py` は `api/services/scheduler.py` から呼び出される。
スタンドアロン実行（`./venv/bin/python pipeline/jobs/collect_bonds.py`）も可能。
