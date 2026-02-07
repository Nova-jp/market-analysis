#!/usr/bin/env python3
"""
ローカル開発サーバー起動スクリプト
ホットリロード対応の開発環境用
"""
import os
import sys
import subprocess
import time
import signal
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 環境変数設定
os.environ.setdefault("ENVIRONMENT", "local")

def main():
    """バックエンドとフロントエンドを同時に起動"""
    
    # 仮想環境のPythonパス
    venv_python = project_root / "venv" / "bin" / "python"
    if not venv_python.exists():
        venv_python = Path(sys.executable)

    print("🚀 Starting local development environment...")
    print(f"📁 Project root: {project_root}")

    processes = []

    try:
        # 1. バックエンドの起動 (FastAPI)
        print("📡 Starting Backend (FastAPI) on http://localhost:8000...")
        backend_proc = subprocess.Popen(
            [
                str(venv_python), "-m", "uvicorn", 
                "app.web.main:app", 
                "--host", "127.0.0.1", 
                "--port", "8000", 
                "--reload"
            ],
            cwd=str(project_root)
        )
        processes.append(backend_proc)

        # 少し待ってからフロントエンドを起動
        time.sleep(2)

        # 2. フロントエンドの起動 (Next.js)
        print("🎨 Starting Frontend (Next.js) on http://localhost:3000...")
        frontend_dir = project_root / "frontend"
        
        # npm が利用可能か確認
        frontend_proc = subprocess.Popen(
            ["npm", "run", "dev"],
            cwd=str(frontend_dir)
        )
        processes.append(frontend_proc)

        print("\n✅ Both servers are running!")
        print("🔗 Frontend: http://localhost:3000")
        print("🔗 Backend API: http://localhost:8000")
        print("💡 Press Ctrl+C to stop both servers\n")

        # プロセスの監視
        while True:
            for p in processes:
                if p.poll() is not None:
                    # いずれかのプロセスが終了した場合は終了
                    return
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n👋 Stopping servers...")
    finally:
        # 全プロセスを確実に終了させる
        for p in processes:
            if p.poll() is None:
                p.terminate()
        
        # 完全に終了するのを待機
        for p in processes:
            p.wait()
        print("✨ Done")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()