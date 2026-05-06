# CLAUDE.md — 国債金利分析システム

日本国債市場の分析プラットフォーム。Cloud Run 上で稼働し、毎日 18:00 JST に自動データ更新。
詳細なプロジェクト仕様・セットアップ手順は **README.md** を参照。

---

## 開発スタンス（必読）

1. **能動的な提案**: 指示が非標準な場合、そのまま実装せず「一般的には〜ですが、いかがでしょう？」と代替案を提示する。
2. **DB アクセス標準化**: Supabase REST API は廃止済み。**SQLAlchemy (Async)** + Neon PostgreSQL を使用。
3. **コード品質**: 型ヒント・Ruff/Black・Pytest を重視。コメントは WHY が非自明な場合のみ。

---

## 🚨 Database Safety Rules（必須）

`DELETE` / `DROP` / `TRUNCATE` / `WHERE` 句なし `UPDATE` は **必ず事前確認**。

1. 影響範囲を明示（例：「テーブル X から日付 Y〜Z の 2400 件を削除します」）
2. バックアップの有無を確認
3. 本番実行前に `SELECT COUNT(*)` で件数を確認（Dry Run）

---

## 🚨 JSDA サーバー保護ルール（必須）

**詳細手順は `scripts/CLAUDE.md` を参照。**

- アクセス間隔: **5 秒以上**（推奨 30 秒以上）
- バックグラウンド実行禁止: `&` / `nohup` は絶対禁止
- 実行前に `ps aux | grep python` でプロセス確認
- フォアグラウンドのみ・完了後は Ctrl+C で明示的に停止

---

## コンテキスト最適化ルール（必須）

**除外ディレクトリ（読み込み禁止）:**
- `venv/` (785 MB) / `__pycache__/` / `*.pyc` / `*.log` / `.git/`

**段階的探索の原則:**
```
❌ 推測で複数ファイルを一度に読み込む
✅ Glob/Grep で対象を絞り込んでから Read（1-2 ファイルずつ）
```

**長時間コマンド（5 分以上 or 出力 1000 行以上）:** 自動実行せず、ユーザーに別ターミナルでの実行を提案する。

---

## 自動承認ルール

| リスク | 操作 | 対応 |
|--------|------|------|
| ✅ Low | `Read` / `Glob` / `Grep` / `SELECT` / `git status` / `pytest` | 自動実行 |
| ⚠️ Medium | `core/` `api/` `pipeline/` 既存コード修正・Gitコミット | EnterPlanMode 承認後に実行 |
| 🛑 High | Cloud Run デプロイ・`DELETE`/`UPDATE`・`git push`・JSDA収集 | 毎回明示的承認 |

---

## Python 仮想環境ルール（必須）

```bash
# ✅ 推奨（確実）
./venv/bin/python script.py

# ✅ 代替
source venv/bin/activate && python script.py

# ❌ 禁止（グローバル環境を汚染）
python3 script.py
pip install <package>  # venv 外での実行
```

---

## 主要ファイル一覧

| 役割 | パス |
|------|------|
| Web アプリ エントリーポイント | `api/main.py` |
| 非同期 DB（Web API 用） | `core/db/async_client.py` |
| 同期 DB（スクリプト用） | `core/db/sync_client.py` |
| 設定管理 | `core/config.py` |
| Pydantic モデル | `core/models/schemas.py` |
| 認証 dependency | `api/dependencies.py` |
| データ収集（JSDA 複数日） | `pipeline/jobs/collect_bonds.py` |
| 収集対象日付 | `data_files/target_dates_latest.json` |
| Docker ビルド | `infra/Dockerfile` |

---

## 4 層アーキテクチャ

```
【フロントエンド層】    web/
  └─ Next.js (App Router)。静的エクスポート → FastAPI が配信

【API 層】             api/
  └─ FastAPI ルーター・サービス・認証 dependency

【コア層】             core/
  └─ 設定・DB クライアント・Pydantic モデル・計算ロジック
     ※ 全層から import される唯一の真実の源

【パイプライン層】     pipeline/
  └─ 外部データ収集（JSDA/MOF/BOJ/JSCC）・バッチジョブ
     ※ api/ からは import しない（一方通行）
```

依存の方向: `web → api → core ← pipeline`

**各層の固有ルールはサブディレクトリの `CLAUDE.md` を参照。**
