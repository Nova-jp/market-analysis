#!/usr/bin/env python3
"""
統合開発環境起動スクリプト (dev.py)
バックエンド(FastAPI)とフロントエンド(Next.js)を並列に起動します。
"""
import os
import sys
import subprocess
import time
from pathlib import Path

# プロジェクトルートの設定
project_root = Path(__file__).parent.parent.absolute()
sys.path.insert(0, str(project_root))

def check_dependencies():
    """必要な依存関係のチェック"""
    print("🔍 Checking dependencies...")
    
    # 1. venv の確認
    venv_dir = project_root / "venv"
    if not venv_dir.exists():
        print("❌ Virtual environment (venv) not found. Please create it first.")
        return False
        
    # 2. node_modules の確認
    node_modules = project_root / "frontend" / "node_modules"
    if not node_modules.exists():
        print("📦 Installing frontend dependencies...")
        try:
            subprocess.run(["npm", "install"], cwd=str(project_root / "frontend"), check=True)
        except subprocess.CalledProcessError:
            print("❌ Failed to install npm dependencies.")
            return False
            
    return True

def main():
    if not check_dependencies():
        sys.exit(1)

    # 仮想環境のPythonを使用
    python_bin = str(project_root / "venv" / "bin" / "python")
    
    print("🚀 Starting Market Analytics Development Environment...")
    
    processes = []
    try:
        # 1. バックエンド (FastAPI) の起動
        print("📡 Starting Backend (FastAPI) on http://localhost:8000")
        backend_proc = subprocess.Popen(
            [python_bin, "-m", "uvicorn", "app.web.main:app", "--host", "127.0.0.1", "--port", "8000", "--reload"],
            cwd=str(project_root)
        )
        processes.append(backend_proc)

        # 起動待ち
        time.sleep(2)

        # 2. フロントエンド (Next.js) の起動
        print("🎨 Starting Frontend (Next.js) on http://localhost:3000")
        frontend_proc = subprocess.Popen(
            ["npm", "run", "dev"],
            cwd=str(project_root / "frontend")
        )
        processes.append(frontend_proc)

        print("\n" + "="*50)
        print("✅ Servers are running!")
        print(f"🔗 Frontend:    http://localhost:3000")
        print(f"🔗 Backend API: http://localhost:8000")
        print(f"🔗 Forward Curve: http://localhost:3000/forward-curve")
        print("💡 Press Ctrl+C to stop all processes")
        print("="*50 + "\n")

        # プロセス監視
        while True:
            for p in processes:
                if p.poll() is not None:
                    print(f"\n⚠️ Process {p.pid} stopped unexpectedly.")
                    return
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n👋 Terminating development servers...")
    finally:
        for p in processes:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()
        print("✨ Environment cleaned up.")

if __name__ == "__main__":
    main()
