# 国債市場分析プラットフォーム

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.128-009688?logo=fastapi)
![Next.js](https://img.shields.io/badge/Next.js-16-black?logo=next.js)
![Cloud Run](https://img.shields.io/badge/GCP-Cloud_Run-4285F4?logo=google-cloud)

日本国債市場の分析・可視化プラットフォーム。  
イールドカーブ・PCA・ASW・フォワードカーブ等の分析機能と、複数官公庁からの自動データ収集パイプラインを統合。  
Cloud Run 上で本番稼働中。毎日 18:00 JST に自動データ更新。

---

## 機能一覧

| 機能 | 説明 |
|------|------|
| **Yield Curve Analysis** | 最大20日付を同時比較。年限フィルター・クイック選択対応 |
| **PCA Analysis** | Level / Slope / Curvature への分解、時系列スコア・再構成誤差 |
| **ASW Analysis** | 三次スプライン補間。ASW = 国債利回り − スワップレート |
| **Forward Curve** | Fixed Start / Fixed Tenor の2モード。インスタントフォワード計算 |
| **Market Amount** | 残存年数別・銘柄別の国債残高推移グラフ |
| **Export** | IMM 日付ベースの分析データを Excel でダウンロード |

---

## アーキテクチャ

```
web/          Next.js (静的エクスポート) → FastAPI が配信
api/          FastAPI ルーター・サービス・認証
core/         設定・DB クライアント・計算ロジック（単一の真実の源）
pipeline/     JSDA / MOF / BOJ / JPX / Yahoo / FRED からのデータ収集
infra/        Dockerfile・docker-compose.yml

依存方向: web → api → core ← pipeline
```

---

## 技術スタック

| レイヤー | 使用技術 |
|----------|---------|
| バックエンド | FastAPI 0.128、SQLAlchemy 2.0 (Async)、Pydantic v2 |
| フロントエンド | Next.js 16、React 19、Tailwind CSS 4、Recharts 3 |
| データ処理 | pandas、scikit-learn、scipy、QuantLib |
| DB | Neon (Serverless PostgreSQL) |
| インフラ | GCP Cloud Run、Cloud Scheduler、Docker |

---

## ローカル開発

```bash
# 1. 依存インストール
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. 環境変数
cp .env.example .env   # DB接続情報・認証情報を入力

# 3. フロントエンドビルド（初回・変更時のみ）
cd web && npm install && npm run build && cd ..

# 4. サーバー起動
./venv/bin/uvicorn api.main:app --reload --port 8000
# → http://localhost:8000

# フロントエンドのみ開発する場合
cd web && npm run dev  # → http://localhost:3000
```

### 環境変数（`.env`）

```env
DATABASE_URL=postgresql+asyncpg://user:password@host/db
PRIVATE_USERNAME=your_username
PRIVATE_PASSWORD=your_password
ENVIRONMENT=development
```

---

## データ収集（手動）

```bash
# 既存プロセス確認（必須）
ps aux | grep python | grep -v grep

# フォアグラウンドのみ・& や nohup は禁止
./venv/bin/python pipeline/jobs/collect_bonds.py data_files/target_dates_latest.json
```

JSDA サーバー保護のため、アクセス間隔 5 秒以上を厳守。

---

## デプロイ（Cloud Run）

```bash
# Docker ビルド確認
docker build -f infra/Dockerfile -t market-analytics .
docker run -p 8080:8080 --env-file .env market-analytics

# Cloud Run デプロイ
gcloud run deploy market-analytics \
  --source . \
  --region asia-northeast1 \
  --allow-unauthenticated
```

---

## 免責事項

本システムは分析・学習目的です。投資判断への利用は専門家にご相談ください。
