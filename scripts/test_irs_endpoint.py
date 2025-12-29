#!/usr/bin/env python3
"""
IRSエンドポイントテストスクリプト
ローカルでエンドポイントをテスト
"""

import requests
import json
from datetime import datetime

# ローカルサーバーURL
BASE_URL = "http://127.0.0.1:8000"

def test_irs_collection():
    """IRSデータ収集エンドポイントをテスト"""

    url = f"{BASE_URL}/api/scheduler/irs-daily-collection"

    # デバッグモード用ヘッダー
    # 本番環境ではALLOW_SCHEDULER_DEBUG=trueを設定する
    headers = {
        "User-Agent": "Google-Cloud-Scheduler"
    }

    print("=" * 80)
    print("IRSデータ収集エンドポイントテスト")
    print("=" * 80)
    print(f"URL: {url}")
    print(f"Time: {datetime.now().isoformat()}")
    print()

    try:
        print("リクエスト送信中...")
        response = requests.post(url, headers=headers, timeout=120)

        print(f"ステータスコード: {response.status_code}")
        print()

        if response.status_code == 200:
            result = response.json()
            print("✅ 成功!")
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            print("❌ エラー")
            print(f"レスポンス: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"❌ リクエストエラー: {e}")

    print()
    print("=" * 80)


if __name__ == "__main__":
    print("\n注意: このスクリプトを実行する前に、以下を確認してください:")
    print("1. Webアプリが起動していること (uvicorn app.web.main:app)")
    print("2. .envでALLOW_SCHEDULER_DEBUG=true を設定していること")
    print()

    input("Enterキーを押してテストを開始...")

    test_irs_collection()
