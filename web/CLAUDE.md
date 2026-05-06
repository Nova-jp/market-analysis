# CLAUDE.md — web/ フロントエンド層

Next.js 16 (App Router) + React 19 + Tailwind CSS 4 + Recharts 3。
FastAPI (`api/main.py`) がビルド済みの `out/` ディレクトリを静的ファイルとして配信する構成。

---

## ディレクトリ構成

```
web/
├── app/
│   ├── layout.tsx           # グローバルレイアウト・ナビゲーション
│   ├── page.tsx             # ホームページ（ダッシュボード）
│   ├── yield-curve/         # イールドカーブ分析
│   ├── pca/                 # PCA 主成分分析
│   ├── asw/                 # ASW 期間構造分析（パスワード保護）
│   ├── forward-curve/       # フォワードカーブ分析（パスワード保護）
│   ├── market-amount/       # 市中残存額統計
│   └── export/              # Excel エクスポート
├── components/              # 再利用可能コンポーネント
└── lib/
    └── api.ts               # API 呼び出しラッパー・型定義
```

---

## API 呼び出しパターン

**必ず `lib/api.ts` の `fetcher` を使う。直接 `fetch` は書かない。**

```typescript
import { fetcher } from '@/lib/api';

// 例: データ取得
const data = await fetcher(`/api/yield-curve/${date}`);
```

- エラーハンドリングは `fetcher` 内で統一済み（`res.ok` チェック → `detail` フィールドをエラーに付与）。
- 型は `lib/api.ts` で定義済みのものを使う。新しいエンドポイントを追加する場合は同ファイルに型を追加。

---

## コンポーネント規約

- **グラフ**: Recharts を使う（`ResponsiveContainer` でレスポンシブ化）。
- **スタイル**: Tailwind CSS 4 クラスのみ使用（インラインスタイルは最小限に）。
- Server Components / Client Components を意識する（インタラクティブな部分は `"use client"`）。

---

## パスワード保護ページ

ASW・フォワードカーブ・スワップページは簡易パスワード保護。

```typescript
// sessionStorage でログイン状態を保持
const isUnlocked = sessionStorage.getItem('swapUnlocked') === 'true';
// パスワード: 0720
```

ページロード時に `swapUnlocked` を確認し、未認証ならパスワード入力フォームを表示する。

---

## ビルド・配信フロー

```bash
# フロントエンドビルド（静的エクスポート）
cd web && npm run build
# → out/ ディレクトリが生成される

# FastAPI がこの out/ を静的ファイルとして配信
# api/main.py で /_next/ と各ページパスをマウント済み
```

**Dockerfile でマルチステージビルド（Node.js → Python）が自動実行される。ローカル開発時は別途 `npm run dev`（port 3000）を起動し、API は port 8000 に向ける。**

---

## 開発コマンド

```bash
cd web

# 依存関係インストール
npm install

# 開発サーバー起動（port 3000）
npm run dev

# 型チェック
npx tsc --noEmit

# Lint
npm run lint

# 本番ビルド
npm run build
```
