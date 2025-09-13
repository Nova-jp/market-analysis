# 国債金利分析システム (Market Analytics System)

## 🎯 プロジェクト目的・目標

### ビジョン
日本国債市場の動向を高度に分析し、金利リスク管理と投資判断を支援する包括的なデータ分析プラットフォームを構築する。

### 主要目標
1. **データ収集**: JSDAから安全・確実にヒストリカルデータを収集
2. **データ分析**: イールドカーブ分析・主成分分析等の高度な分析機能
3. **可視化**: 直感的で操作性の高いWebアプリケーション
4. **自動化**: 毎日のデータ更新とレポート生成（将来実装）

## 🚨 絶対遵守ルール（開発制約）

### **JSDA サーバー保護ルール**
> **警告**: 違反は即座にプロジェクト停止の対象

1. **アクセス間隔**: 必ず5秒以上（推奨30秒以上）
2. **バックグラウンド実行禁止**: `&`, `nohup` での実行禁止
3. **プロセス監視必須**: データ収集前に `ps aux | grep python` で確認
4. **明示的停止**: 完了後は必ず Ctrl+C で停止
5. **同時実行禁止**: 複数の収集スクリプト同時実行禁止
6. **エラー時待機**: タイムアウト時5分、一般エラー時3分待機
7. **フォアグラウンド実行**: 必ずターミナルで直接実行・監視

### **安全な実行手順（必須）**
```bash
# 1. 既存プロセス確認
ps aux | grep -E "(simple_multi_day|collect_single)" | grep -v grep

# 2. 既存プロセスがあれば停止
kill [PID]

# 3. フォアグラウンドで実行
python scripts/simple_multi_day_collector.py data_files/target_dates_latest.json

# 4. 完了時は Ctrl+C で停止確認
```

### **開発ルール**
- Webアプリは直接JSDAアクセス禁止（DBアクセスのみ）
- 新規データ収集は明示的指示時のみ
- コード変更時のJSDA制約確認必須

## 🏗️ 技術アーキテクチャ

### **技術スタック**
- **言語**: Python 3.9+
- **データベース**: Supabase (PostgreSQL)
- **Webフレームワーク**: FastAPI + Jinja2
- **可視化**: Chart.js, Bootstrap
- **データ分析**: pandas, scikit-learn, numpy
- **データ収集**: requests, httpx

### **プロジェクト構造**
```
market-analytics-ver1/
├── 📁 app/                     # Future: アプリケーションコア
│   ├── api/                   # API endpoints
│   ├── models/                # データモデル (Pydantic)
│   ├── services/              # ビジネスロジック
│   └── utils/                 # ユーティリティ
├── 📁 data/                    # ✅ データ関連 (完成)
│   ├── collectors/            # データ収集 (JSDA)
│   ├── processors/            # データ処理・変換
│   ├── utils/                 # データベース管理
│   └── validators/            # データ検証
├── 📁 analysis/                # ✅ 分析機能 (基盤完成)
│   ├── yield_curve_analyzer.py  # イールドカーブ分析
│   ├── pca/                   # Future: 主成分分析
│   └── ml/                    # Future: 機械学習
├── 📁 webapp/                  # ✅ Webアプリ (本格運用可能)
│   ├── main.py               # FastAPI + Jinja2
│   ├── templates/            # HTML テンプレート
│   └── static/               # CSS, JS, assets
├── 📁 scripts/                 # ✅ バッチ処理 (完成)
│   ├── simple_multi_day_collector.py  # 複数日収集
│   └── collect_single_day.py  # 単日収集
├── 📁 data_files/              # ✅ 設定ファイル
├── 📁 frontend/               # 🔄 Legacy: Streamlit (移行済み)
├── 📁 tests/                  # Future: テスト
└── 📁 docs/                   # Future: ドキュメント
```

## ✅ 完成機能一覧

### **1. データ収集システム (Production Ready)**
- **単日収集**: `scripts/collect_single_day.py`
- **複数日収集**: `scripts/simple_multi_day_collector.py`
- **JSDA保護機能**: タイムアウト検出、自動リトライ、適切な間隔制御
- **データ処理**: J列 (interest_payment_date) MM/DD形式対応
- **エラーハンドリング**: 詳細ログ、プロセス監視、安全停止

### **2. Webアプリケーション (Production Ready)**
```bash
# 起動コマンド
cd webapp && python main.py
# → http://127.0.0.1:8000
```

**主要機能:**
- 📊 **ダッシュボード**: インタラクティブなイールドカーブ表示
- 📈 **時系列ナビゲーション**: 日付選択による過去データ閲覧
- 🔄 **リアルタイム更新**: Chart.js による動的グラフ更新
- 📱 **レスポンシブ対応**: Bootstrap ベースのモバイル対応UI

**API エンドポイント:**
- `GET /api/dates` - 利用可能日付一覧
- `GET /api/yield-curve/{date}` - 指定日のイールドカーブデータ
- `GET /api/compare` - 複数日比較データ
- `GET /health` - ヘルスチェック

### **3. データ分析エンジン (Core Complete)**
- **イールドカーブ分析**: `analysis/yield_curve_analyzer.py`
- **期間別分類**: 短期・中期・長期債の自動分類
- **統計分析**: 基本統計量、トレンド分析
- **多項式フィッティング**: 曲線の平滑化と補間

### **4. データベース管理 (Production Ready)**
- **Supabase統合**: PostgreSQL クラウドDB
- **スキーマ管理**: bond_data テーブル (123,000+ レコード)
- **データ整合性**: 重複排除、型検証
- **バッチ処理**: 高効率な一括挿入・更新

## 🗓️ 開発ロードマップ

### **Epic 1: データ収集・基盤システム** ✅ **完了**
#### Feature 1.1: JSDA データ収集システム ✅
- Story 1.1.1: 単日データ収集機能 ✅
- Story 1.1.2: 複数日データ収集機能 ✅
- Story 1.1.3: JSDAサーバー保護機能 ✅
- Story 1.1.4: エラーハンドリング・リトライ機能 ✅

#### Feature 1.2: データベース設計・管理 ✅
- Story 1.2.1: Supabase プロジェクト構築 ✅
- Story 1.2.2: テーブルスキーマ設計 ✅
- Story 1.2.3: データ挿入・更新システム ✅
- Story 1.2.4: J列形式変更対応 (MM/DD) ✅

### **Epic 2: Webアプリケーション基盤** ✅ **完了**
#### Feature 2.1: FastAPI + Jinja2 基盤 ✅
- Story 2.1.1: FastAPI アプリケーション構築 ✅
- Story 2.1.2: Jinja2 テンプレートエンジン統合 ✅
- Story 2.1.3: Bootstrap UI フレームワーク ✅
- Story 2.1.4: 静的ファイル配信システム ✅

#### Feature 2.2: API エンドポイント ✅
- Story 2.2.1: 日付一覧取得 API ✅
- Story 2.2.2: イールドカーブデータ API ✅
- Story 2.2.3: データ比較 API ✅
- Story 2.2.4: ヘルスチェック API ✅

### **Epic 3: 可視化・UI システム** ✅ **完了**
#### Feature 3.1: イールドカーブ可視化 ✅
- Story 3.1.1: Chart.js 統合 ✅
- Story 3.1.2: インタラクティブグラフ ✅
- Story 3.1.3: 時系列ナビゲーション ✅
- Story 3.1.4: レスポンシブデザイン ✅

### **Epic 4: 高度分析機能** 🔄 **進行中**
#### Feature 4.1: イールドカーブ分析 ✅
- Story 4.1.1: 基本統計分析 ✅
- Story 4.1.2: 期間別分類システム ✅
- Story 4.1.3: トレンド分析 ✅
- Story 4.1.4: 多項式フィッティング ✅

#### Feature 4.2: 主成分分析 📋 **計画中**
- Story 4.2.1: PCA アルゴリズム実装
  - Task: sklearn PCA モジュール統合
  - Task: 次元削減パラメータ調整
  - Task: 固有ベクトル解釈システム
- Story 4.2.2: 主成分可視化
  - Task: 主成分寄与率グラフ
  - Task: 固有ベクトル表示
  - Task: 時系列主成分変化
- Story 4.2.3: Web UI 統合
  - Task: PCA 結果表示ページ
  - Task: インタラクティブ分析
  - Task: レポート出力機能

#### Feature 4.3: 予測・機械学習 📋 **将来計画**
- Story 4.3.1: 時系列予測モデル
- Story 4.3.2: 異常検知システム
- Story 4.3.3: リスク指標計算

### **Epic 5: 運用・自動化** 📋 **将来計画**
#### Feature 5.1: 自動データ更新
- Story 5.1.1: 日次バッチ処理
- Story 5.1.2: 自動品質チェック
- Story 5.1.3: 異常時通知システム

#### Feature 5.2: 本番環境デプロイ
- Story 5.2.1: クラウド環境構築
- Story 5.2.2: CI/CD パイプライン
- Story 5.2.3: 監視・ログシステム

## 🔧 開発コマンド

### **環境構築**
```bash
pip install -r requirements.txt
cp .env.example .env  # 環境変数を設定
```

### **アプリケーション起動**
```bash
# Webアプリ起動 (推奨)
cd webapp && python main.py
# → http://127.0.0.1:8000

# Legacy: Streamlitアプリ
streamlit run frontend/streamlit_app.py

# Future: FastAPI 開発サーバー
uvicorn app.api.main:app --reload --host 0.0.0.0 --port 8000
```

### **データ収集 (要注意: JSDA保護ルール遵守)**
```bash
# 必須: プロセス確認
ps aux | grep python

# 安全な実行
python scripts/simple_multi_day_collector.py data_files/target_dates_latest.json
```

### **開発支援**
```bash
# テスト実行 (Future)
pytest tests/

# コードフォーマット (Future)
black webapp/ data/ analysis/
flake8 webapp/ data/ analysis/
```

## 📊 データソース

### **JSDA (日本証券業協会)**
- **URL**: https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/
- **形式**: CSV/Excel (自動CSV選択)
- **更新**: 毎日17:30頃 (一部18:30)
- **履歴**: 2002年からのデータ利用可能
- **制約**: アクセス間隔5秒以上必須

### **データベース現状**
- **レコード数**: 123,000+ 件
- **対象期間**: 2002年〜現在
- **主要フィールド**: 
  - trade_date (取引日)
  - interest_payment_date (利払日: MM/DD形式)
  - yield_rate (利回り)
  - maturity_years (残存年数)

## ⚙️ 環境変数設定

`.env`ファイルに以下を設定:
```env
# Supabase 設定
SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_anon_key

# アプリケーション設定
DEBUG=True
LOG_LEVEL=INFO
```

## 📈 プロジェクト状況

### **完成度**
- **データ収集**: 100% (Production Ready)
- **Webアプリ**: 100% (Production Ready)  
- **基本分析**: 100% (イールドカーブ)
- **高度分析**: 20% (PCA計画中)
- **自動化**: 0% (将来計画)

### **次のマイルストーン**
1. **主成分分析実装** (Epic 4, Feature 4.2)
   - 優先度: 高
   - 期間: 2-3週間
   - 担当: 分析エンジン拡張

2. **予測モデル検討** (Epic 4, Feature 4.3)
   - 優先度: 中
   - 期間: 4-6週間
   - 担当: ML パイプライン構築

3. **本番環境準備** (Epic 5)
   - 優先度: 中
   - 期間: 3-4週間
   - 担当: インフラ・DevOps

### **技術的負債**
- Frontend Streamlit コード整理 (優先度: 低)
- テストスイート未実装 (優先度: 中)
- エラー処理の標準化 (優先度: 低)

---

## 📝 最終更新
- **更新日**: 2025-09-11
- **バージョン**: v2.0
- **更新者**: Claude Code Assistant
- **変更内容**: プロジェクト全体構造を Epic/Feature/Story/Task レベルで再編成