# Cloud Run用Dockerfile
# ローカル開発とCloud Run本番環境で同じコードベースを使用
FROM python:3.9-slim

# 作業ディレクトリ
WORKDIR /app

# システムパッケージの更新
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 依存関係ファイルを先にコピー（キャッシュ最適化）
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコードをコピー
COPY app/ app/              # Webアプリ + API + サービス
COPY src/ src/              # 共通データ処理関数
COPY templates/ templates/  # HTMLテンプレート
COPY static/ static/        # CSS, JS, 画像
COPY data_files/ data_files/  # 設定ファイル

# 環境変数（Cloud Runで上書き可能）
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV PORT=8080

# ポート公開
EXPOSE 8080

# アプリケーション起動
# app/web/main.py を実行（ローカルと同じコード）
CMD exec uvicorn app.web.main:app --host 0.0.0.0 --port ${PORT}
