# CLAUDE.md — scripts/ 実行・運用層

Cloud Scheduler から呼ばれるバッチ処理と、手動運用スクリプト群。
JSDA サーバーへのアクセスを伴う場合は以下のルールを**厳守**する。

---

## ディレクトリ構成

```
scripts/
├── collectors/          # データ収集スクリプト
│   ├── simple_multi_day_collector.py  # 複数日収集（推奨）
│   └── collect_single_day.py          # 単日収集
├── runners/             # Cloud Scheduler 実行ターゲット
│   ├── daily_asw_runner.py
│   ├── daily_macro_data_update.py
│   └── calculate_market_amount_*.py
├── analysis/            # 運用集計・分析スクリプト
├── setup/               # テーブル作成・初期構築
├── maintenance/         # データ修正・メンテナンス
├── sql/                 # DDL・RLS ポリシー定義
└── export/              # データエクスポート
```

---

## 🚨 JSDA サーバー保護ルール（詳細）

**違反はプロジェクト停止対象。必ず以下を守る。**

| ルール | 内容 |
|--------|------|
| アクセス間隔 | 5 秒以上（推奨 30 秒以上） |
| 実行モード | フォアグラウンドのみ |
| バックグラウンド | `&` / `nohup` は**絶対禁止** |
| 同時実行 | 複数の収集スクリプトの同時実行禁止 |
| エラー時待機 | タイムアウト → 5 分待機 / 一般エラー → 3 分待機 |

### 安全な実行手順

```bash
# 1. 既存プロセスを確認（必須）
ps aux | grep -E "(simple_multi_day|collect_single)" | grep -v grep

# 2. 既存プロセスがあれば停止
kill [PID]

# 3. フォアグラウンドで実行（& や nohup は絶対使わない）
./venv/bin/python scripts/collectors/simple_multi_day_collector.py data_files/target_dates_latest.json

# 4. 完了を確認してから次の操作へ（Ctrl+C で明示的に停止）
```

---

## Cloud Scheduler 連携スクリプト

`scripts/runners/` 配下のスクリプトは Cloud Scheduler から HTTP エンドポイント経由で呼ばれる。

| スケジュール | エンドポイント | 担当サービス |
|-------------|--------------|-------------|
| 毎日 18:00 JST | `POST /api/scheduler/daily-collection` | `app/services/scheduler_service.py` |
| 毎日 21:00 JST | `POST /api/scheduler/irs-collection` | `app/services/irs_scheduler_service.py` |
| 毎日（ASW 計算） | `POST /api/scheduler/asw-calculation` | `app/services/asw_scheduler_service.py` |

**本番スケジューラー確認コマンド:**
```bash
gcloud scheduler jobs list --location="asia-northeast1"
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=market-analytics" --limit=20
```

---

## SQL DDL 変更手順

1. `scripts/sql/schema/` に新しい DDL ファイルを作成
2. `data/utils/database_manager.py` の `ALLOWED_TABLES` に新テーブルを追加
3. RLS ポリシーが必要な場合は `scripts/sql/security/` に追加
4. **本番 DB への DDL 実行は必ず手動確認後に実施（自動実行禁止）**

---

## スクリプト実行前チェックリスト

- [ ] `ps aux | grep python` で既存プロセスなし
- [ ] 仮想環境が有効（`./venv/bin/python` または `source venv/activate`）
- [ ] `.env` が最新の DB 接続情報を持っている
- [ ] JSDA アクセスを伴う場合: 前回実行から 30 秒以上経過
