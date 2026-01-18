# --- Frontend Build Stage ---
FROM node:18-slim AS frontend-builder
WORKDIR /build
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# --- Backend Stage ---
FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Python依存関係のインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコードのコピー
COPY app/ app/
COPY data/ data/
COPY analysis/ analysis/
COPY data_files/ data_files/

# ビルドされたフロントエンドをコピー (FastAPIが配信する場所へ)
COPY --from=frontend-builder /build/out/ static/dist/

# 環境変数
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV PORT=8080

# 起動
CMD exec uvicorn app.web.main:app --host 0.0.0.0 --port ${PORT}