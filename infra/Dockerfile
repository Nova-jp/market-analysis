# --- Frontend Build Stage ---
FROM node:20 AS frontend-builder
WORKDIR /build
COPY web/package*.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

# --- Backend Stage ---
FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 非rootユーザーを作成（セキュリティベストプラクティス）
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Python依存関係のインストール（コードより先にコピーしてDockerキャッシュを有効活用）
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# アプリケーションコードのコピー
COPY core/ core/
COPY api/ api/
COPY pipeline/ pipeline/
COPY data_files/ data_files/

# ビルドされたフロントエンドをコピー (FastAPIが配信する場所へ)
COPY --from=frontend-builder /build/out/ static/dist/

# キャッシュディレクトリを作成し非rootユーザーに権限付与
RUN mkdir -p .cache/pca && chown -R appuser:appuser /app

# 環境変数
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV PORT=8080

# ヘルスチェック
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:${PORT}/health')" || exit 1

# 非rootユーザーで実行
USER appuser

# 起動
CMD exec uvicorn api.main:app --host 0.0.0.0 --port ${PORT}