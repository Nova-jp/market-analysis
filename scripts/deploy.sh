#!/bin/bash
# Cloud Run 簡単デプロイスクリプト

set -e  # エラー時に停止

echo "🚀 Cloud Run デプロイスクリプト"
echo "================================"

# 環境変数の確認
if [ ! -f .env ]; then
    echo "❌ エラー: .env ファイルが見つかりません"
    echo "   データベース設定ファイルを作成してください"
    exit 1
fi

# .envから環境変数を読み込み
source .env

if [ -z "$SUPABASE_URL" ] || [ -z "$SUPABASE_KEY" ]; then
    echo "❌ エラー: SUPABASE_URL または SUPABASE_KEY が設定されていません"
    echo "   .env ファイルを確認してください"
    exit 1
fi

echo "✅ 環境変数チェック完了"

# プロジェクト設定
PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"turnkey-diode-472203-q6"}
SERVICE_NAME="market-analytics"
REGION="asia-northeast1"

echo "📋 デプロイ設定:"
echo "   プロジェクト: $PROJECT_ID"
echo "   サービス: $SERVICE_NAME"
echo "   リージョン: $REGION"

# GCPプロジェクト設定確認
echo "🔧 GCPプロジェクト設定中..."
gcloud config set project $PROJECT_ID

# デプロイ実行
echo "🌍 Cloud Run にデプロイ中..."
gcloud run deploy $SERVICE_NAME \
  --source . \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated \
  --port 8080 \
  --set-env-vars "SUPABASE_URL=$SUPABASE_URL,SUPABASE_KEY=$SUPABASE_KEY"

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 デプロイ完了！"
    echo "🌐 URL: https://$SERVICE_NAME-646409283435.$REGION.run.app"
    echo ""
    echo "💡 ヒント:"
    echo "   - ローカル開発: python scripts/run_local.py"
    echo "   - 再デプロイ: ./scripts/deploy.sh"
else
    echo "❌ デプロイに失敗しました"
    exit 1
fi