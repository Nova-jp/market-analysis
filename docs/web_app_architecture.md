# 📈 Market Analytics Webアプリアーキテクチャ設計

## 🎯 概要

日本国債市場分析システムの段階的Webアプリ化計画。現在のStreamlitベースから本格的なWebアプリケーションへの移行戦略。

## 🗂️ 現在の構成（Phase 1）

### Streamlitアプリ
```
frontend/
├── yield_curve_app.py                 # 基本的なイールドカーブ表示
└── yield_curve_time_series_app.py     # 時系列ナビゲーション機能付き
```

**特徴:**
- ✅ 簡単な展開・操作
- ✅ インタラクティブなPlotlyチャート
- ✅ 時系列データのナビゲーション
- ✅ 比較機能
- ❌ カスタマイズ性に限界
- ❌ 外部統合が困難

### 起動方法
```bash
# 時系列ナビゲーション版（推奨）
streamlit run frontend/yield_curve_time_series_app.py

# 基本版
streamlit run frontend/yield_curve_app.py
```

## 🔗 API層（Phase 2 - 完了）

### FastAPI REST API
```
app/api/
├── main.py                 # メインアプリケーション
└── yield_curve.py          # イールドカーブ API
```

### エンドポイント設計

| エンドポイント | メソッド | 説明 | レスポンス |
|---------------|---------|------|-----------|
| `/` | GET | API情報 | API基本情報 |
| `/health` | GET | ヘルスチェック | システム状態 |
| `/api/yield-curve/dates` | GET | 利用可能日付一覧 | 日付リスト |
| `/api/yield-curve/{date}` | GET | 指定日のイールドカーブ | 債券データ + 統計 |
| `/api/yield-curve/compare/dates` | GET | 複数日比較 | 比較データ |
| `/api/yield-curve/stats/{date}` | GET | 統計情報のみ | 軽量統計データ |

### API起動方法
```bash
# 開発サーバー
python app/api/main.py

# または
uvicorn app.api.main:app --reload --host 0.0.0.0 --port 8000
```

### APIドキュメント
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## 🌐 本格Webアプリ（Phase 3 - 将来）

### アーキテクチャ概要
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   Backend API   │    │   Database      │
│                 │    │                 │    │                 │
│ React/Vue.js    │◄──►│ FastAPI         │◄──►│ Supabase        │
│ TypeScript      │    │ Python          │    │ PostgreSQL      │
│ Tailwind CSS    │    │ Pydantic        │    │ 120,000+ records│
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │
         │              ┌─────────────────┐
         │              │   Analytics     │
         └──────────────►│   Engine        │
                        │ NumPy/Pandas    │
                        │ Scikit-learn    │
                        └─────────────────┘
```

### 推奨技術スタック

#### フロントエンド
- **Framework**: React + TypeScript または Vue.js 3 + TypeScript
- **UI Library**: Tailwind CSS + HeadlessUI または Ant Design
- **Charts**: D3.js または Chart.js/Recharts
- **State Management**: Redux Toolkit または Pinia (Vue)
- **Build Tool**: Vite
- **Testing**: Jest + React Testing Library

#### バックエンド（既存）
- **API Framework**: FastAPI
- **Data Processing**: Pandas, NumPy
- **Database ORM**: なし（直接REST API使用）
- **Caching**: Redis（オプション）
- **Documentation**: OpenAPI/Swagger

#### インフラ（将来）
- **Deployment**: Vercel/Netlify (Frontend) + Railway/Render (Backend)
- **Database**: Supabase（現状維持）
- **CDN**: Cloudflare
- **Domain**: カスタムドメイン

### フォルダ構造案（Phase 3）
```
market-analytics-web/
├── frontend/                   # フロントエンドアプリ
│   ├── src/
│   │   ├── components/        # UIコンポーネント
│   │   │   ├── charts/       # チャート関連
│   │   │   ├── navigation/   # ナビゲーション
│   │   │   └── common/       # 共通コンポーネント
│   │   ├── pages/            # ページコンポーネント
│   │   ├── hooks/            # カスタムフック
│   │   ├── services/         # API通信
│   │   ├── types/            # TypeScript型定義
│   │   └── utils/            # ユーティリティ
│   ├── public/               # 静的ファイル
│   ├── package.json
│   └── vite.config.ts
├── backend/                    # バックエンド（既存app/移行）
│   ├── api/
│   ├── analysis/
│   └── data/
└── docs/                      # ドキュメント
```

## 📋 機能要件（Phase 3）

### 🎨 UI/UX機能
- **レスポンシブデザイン**: モバイル・タブレット対応
- **ダークモード**: ライト・ダークテーマ切り替え
- **アニメーション**: スムーズなトランジション
- **多言語対応**: 日本語・英語

### 📊 分析機能
- **インタラクティブチャート**: ズーム・パン・フィルタリング
- **リアルタイム更新**: WebSocket接続でのライブデータ
- **カスタムダッシュボード**: ユーザー設定可能なレイアウト
- **エクスポート機能**: PNG・PDF・Excel出力
- **アラート機能**: 閾値ベースの通知

### 🔒 セキュリティ・認証
- **認証システム**: JWT ベース認証
- **API制限**: レート制限・使用量制限
- **データ暗号化**: HTTPS・データベース暗号化

## 🚀 移行計画

### Phase 1 ✅ 完了
- [x] Streamlitプロトタイプ
- [x] 基本的なイールドカーブ表示
- [x] 時系列ナビゲーション

### Phase 2 ✅ 完了
- [x] FastAPI REST API
- [x] エンドポイント設計
- [x] API ドキュメント生成
- [x] CORS対応

### Phase 3 📋 計画中
- [ ] フロントエンド技術選択
- [ ] UI/UXデザイン
- [ ] 認証システム設計
- [ ] 本格開発開始

### Phase 4 🚀 将来
- [ ] 本番環境デプロイ
- [ ] パフォーマンス最適化
- [ ] 追加分析機能
- [ ] モバイルアプリ（PWA）

## 🛠️ 開発環境セットアップ

### 現在の環境（Phase 1-2）
```bash
# プロジェクトクローン
cd market-analytics-ver1

# 仮想環境作成・有効化
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 依存関係インストール
pip install -r requirements.txt

# 環境変数設定
cp .env.example .env
# .envを編集してSupabase認証情報を設定

# Streamlitアプリ起動
streamlit run frontend/yield_curve_time_series_app.py

# API サーバー起動（別ターミナル）
python app/api/main.py
```

### 将来の環境（Phase 3）
```bash
# フロントエンド
cd frontend
npm install
npm run dev

# バックエンド
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload

# データベース
# Supabase（現状維持）
```

## 📈 パフォーマンス最適化案

### データベース
- **インデックス最適化**: trade_date, bond_code等
- **クエリ最適化**: 複雑なJOINの回避
- **キャッシング**: Redis導入検討

### API
- **レスポンス圧縮**: gzip圧縮
- **データページネーション**: 大量データの分割読み込み
- **非同期処理**: 重い分析処理の背景実行

### フロントエンド
- **コード分割**: 動的インポート
- **画像最適化**: WebP形式、遅延読み込み
- **キャッシング**: Service Worker、CDN

## 🔍 監視・分析

### メトリクス
- **パフォーマンス**: レスポンス時間、スループット
- **利用状況**: ページビュー、機能利用状況
- **エラー**: エラー率、例外情報

### ツール候補
- **分析**: Google Analytics, Mixpanel
- **エラー監視**: Sentry
- **パフォーマンス**: Lighthouse, PageSpeed Insights

## 📞 まとめ

現在のStreamlitベースから本格的なWebアプリケーションへの移行は段階的に実行可能です。API層は既に準備完了しており、フロントエンド技術選択とUI/UX設計が次のステップです。

**推奨アクション:**
1. **短期**: Streamlitアプリの機能拡張継続
2. **中期**: フロントエンド技術選択・プロトタイプ開発
3. **長期**: 本格的なWebアプリケーション開発・デプロイ