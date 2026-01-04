#!/usr/bin/env python3
"""
Cloud Run デプロイスクリプト (Python版)
.envの内容を安全に読み込み、gcloud run deployを実行します。
"""
import os
import subprocess
import sys
from dotenv import load_dotenv

def deploy():
    # .envを読み込み
    load_dotenv()
    
    # 必須変数の確認
    required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD"]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        print(f"❌ Error: Missing required environment variables: {', '.join(missing)}")
        print("Please check your .env file.")
        sys.exit(1)
        
    # 設定値
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "turnkey-diode-472203-q6")
    service_name = "market-analytics"
    region = "asia-northeast1"
    
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "neondb")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    
    print("🚀 Starting Cloud Run deployment...")
    print(f"Project: {project_id}")
    print(f"Service: {service_name}")
    print(f"Region:  {region}")
    
    # gcloud config set project
    subprocess.run(["gcloud", "config", "set", "project", project_id], check=True)
    
    # Deploy command
    cmd = [
        "gcloud", "run", "deploy", service_name,
        "--source", ".",
        "--platform", "managed",
        "--region", region,
        "--allow-unauthenticated",
        "--port", "8080",
        "--set-env-vars", f"DB_HOST={db_host},DB_PORT={db_port},DB_NAME={db_name},DB_USER={db_user},DB_PASSWORD={db_password}"
    ]
    
    print("\n🌍 Deploying to Cloud Run (this may take a few minutes)...")
    try:
        subprocess.run(cmd, check=True)
        print("\n🎉 Deployment successful!")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Deployment failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    deploy()
