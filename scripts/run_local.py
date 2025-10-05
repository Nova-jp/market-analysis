#!/usr/bin/env python3
"""
ローカル開発サーバー起動スクリプト
ホットリロード対応の開発環境用
"""
import os
import sys
import uvicorn
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 環境変数設定
os.environ.setdefault("ENVIRONMENT", "local")

def main():
    """ローカル開発サーバーを起動"""
    print("🚀 Starting local development server...")
    print("📁 Project root:", project_root)
    print("🔗 Environment: local")

    # 環境ファイルの確認
    env_file = project_root / ".env"
    if env_file.exists():
        print(f"✅ Environment file found: {env_file}")
    else:
        print(f"⚠️  Environment file not found: {env_file}")
        print("Please create .env file with database configuration")

    try:
        uvicorn.run(
            "app.web.main:app",
            host="127.0.0.1",
            port=8001,
            reload=True,
            reload_dirs=[str(project_root / "app")],
            log_level="info"
        )
    except KeyboardInterrupt:
        print("\n👋 Local development server stopped")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()