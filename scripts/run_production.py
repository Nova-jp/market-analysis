#!/usr/bin/env python3
"""
本番環境サーバー起動スクリプト
Cloud Run等での本番デプロイ用
"""
import os
import sys
import uvicorn
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 環境変数設定
os.environ.setdefault("ENVIRONMENT", "production")

def main():
    """本番サーバーを起動"""
    print("🚀 Starting production server...")
    print("📁 Project root:", project_root)
    print("🔗 Environment: production")

    # 環境変数の確認
    required_env_vars = ["SUPABASE_URL", "SUPABASE_KEY"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_vars:
        print(f"❌ Missing required environment variables: {missing_vars}")
        sys.exit(1)

    print("✅ All required environment variables are set")

    # ポート設定（Cloud Run用）
    port = int(os.getenv("PORT", 8080))
    host = "0.0.0.0"

    try:
        uvicorn.run(
            "app.web.main:app",
            host=host,
            port=port,
            log_level="info",
            access_log=True
        )
    except KeyboardInterrupt:
        print("\n👋 Production server stopped")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()