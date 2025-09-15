# 🚀 Vercel デプロイメントガイド

## 📋 デプロイ前チェックリスト

### ✅ 必要ファイル確認
- [x] `vercel.json` - Vercel設定ファイル
- [x] `api/index.py` - エントリーポイント
- [x] `requirements-vercel.txt` - 依存関係（軽量版）
- [x] `simple_app.py` - メインアプリケーション
- [x] `templates/` - HTMLテンプレート
- [x] `static/` - 静的ファイル

## 🔧 1. Vercelアカウント設定

```bash
# Vercel CLIインストール
npm install -g vercel

# アカウントログイン
vercel login
```

## 🌐 2. GitHubリポジトリとの連携

1. **GitHubにコード確認**
   ```bash
   git status
   git add .
   git commit -m "Vercelデプロイメント設定完了"
   git push origin main
   ```

2. **Vercel Dashboard**
   - https://vercel.com/dashboard
   - "Import Project" → GitHub連携
   - `market-analytics-ver1` リポジトリ選択

## 🔐 3. 環境変数設定

Vercel Dashboard → Settings → Environment Variables で以下を設定:

```
SUPABASE_URL = your_supabase_project_url
SUPABASE_KEY = your_supabase_anon_key
```

## 🚀 4. デプロイ実行

### 方法A: CLIデプロイ（推奨）
```bash
cd /path/to/market-analytics-ver1
vercel
```

### 方法B: GitHub自動デプロイ
- GitHubにpush → 自動デプロイ開始

## ✅ 5. デプロイ後確認

1. **基本動作確認**
   - ホームページ: `https://your-app.vercel.app`
   - イールドカーブ: `https://your-app.vercel.app/yield-curve`
   - API: `https://your-app.vercel.app/api/info`

2. **機能テスト**
   - [ ] 日付選択機能
   - [ ] クイック日付ボタン
   - [ ] グラフ表示
   - [ ] 年限フィルター

## 🔧 6. トラブルシューティング

### よくある問題

1. **Import Error: data.utils.database_manager**
   ```bash
   # 解決策: パス設定確認
   sys.path.append(os.path.dirname(os.path.abspath(__file__)))
   ```

2. **環境変数が読み込まれない**
   ```bash
   # Vercel Dashboard → Settings → Environment Variables で再確認
   ```

3. **静的ファイルが読み込まれない**
   ```bash
   # templates/ と static/ がプロジェクトルートにあることを確認
   ```

## 📊 使用量監視

Vercel Dashboard → Analytics で確認:
- Function Invocations: 1M/月まで無料
- Bandwidth: 100GB/月まで無料
- Edge Requests: 無制限

## 🎯 次のステップ

1. **独自ドメイン設定**（オプション）
   - Vercel Dashboard → Domains
   - カスタムドメイン追加

2. **監視設定**
   - Vercel Analytics有効化
   - エラー通知設定

3. **パフォーマンス最適化**
   - エッジキャッシュ設定
   - 圧縮最適化