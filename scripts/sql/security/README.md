# Row Level Security (RLS) ポリシー設定ガイド

## 概要

このディレクトリには、Supabaseデータベースのセキュリティポリシーを設定するSQLスクリプトが含まれています。

## セキュリティ方針

### アクセス制御の原則

1. **読み取り（SELECT）**: パブリックアクセス許可
   - Webアプリケーション（匿名ユーザー）からの読み取りを許可
   - Anon Keyでのアクセスが可能

2. **書き込み（INSERT/UPDATE/DELETE）**: 認証済みユーザーのみ
   - Service Role Keyを使用した書き込みのみ許可
   - データ収集スクリプトやバックエンドサービスからの操作

### 対象テーブル

| テーブル | 説明 | READ | WRITE |
|---------|------|------|-------|
| `bond_data` | JSDA国債データ | 全員 | 認証済み |
| `boj_holdings` | 日銀保有国債データ | 全員 | 認証済み |
| `bond_summary` | 分析用ビュー（bond_dataから生成） | 全員 | - |

## 実行方法

### 方法1: Supabase Dashboard (推奨)

1. [Supabase Dashboard](https://app.supabase.com/) にログイン
2. プロジェクトを選択
3. 左サイドバーから **SQL Editor** を選択
4. **New query** をクリック
5. `scripts/sql/security/setup_rls_policies.sql` の内容をコピー＆ペースト
6. **Run** をクリックして実行

### 方法2: Supabase CLI

```bash
# Supabase CLIがインストールされている場合
supabase db execute --file scripts/sql/security/setup_rls_policies.sql
```

### 方法3: psql (PostgreSQL CLI)

```bash
# Supabaseの接続情報を使用
psql "postgresql://postgres:[YOUR-PASSWORD]@[YOUR-PROJECT-REF].supabase.co:5432/postgres" \
  -f scripts/sql/security/setup_rls_policies.sql
```

## 実行結果の確認

### RLS有効化状態の確認

```sql
SELECT tablename, rowsecurity
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN ('bond_data', 'boj_holdings');
```

期待される出力:
```
  tablename   | rowsecurity
--------------+-------------
 bond_data    | t
 boj_holdings | t
```

### ポリシー一覧の確認

```sql
SELECT
    schemaname,
    tablename,
    policyname,
    roles,
    cmd,
    qual,
    with_check
FROM pg_policies
WHERE schemaname = 'public'
  AND tablename IN ('bond_data', 'boj_holdings')
ORDER BY tablename, policyname;
```

期待される出力:
```
schemaname | tablename    | policyname                          | roles          | cmd
-----------+--------------+-------------------------------------+----------------+--------
public     | bond_data    | bond_data_delete_authenticated      | {authenticated}| DELETE
public     | bond_data    | bond_data_insert_authenticated      | {authenticated}| INSERT
public     | bond_data    | bond_data_select_public             | {public}       | SELECT
public     | bond_data    | bond_data_update_authenticated      | {authenticated}| UPDATE
public     | boj_holdings | boj_holdings_delete_authenticated   | {authenticated}| DELETE
public     | boj_holdings | boj_holdings_insert_authenticated   | {authenticated}| INSERT
public     | boj_holdings | boj_holdings_select_public          | {public}       | SELECT
public     | boj_holdings | boj_holdings_update_authenticated   | {authenticated}| UPDATE
```

## テスト

### 読み取りテスト（Anon Keyで実行）

Webアプリケーションから正常に読み取れることを確認:

```bash
# ローカル開発サーバーを起動
python -m app.web.main

# ブラウザで http://127.0.0.1:8000 にアクセス
# ダッシュボードやイールドカーブページが正常に表示されることを確認
```

### 書き込みテスト（Service Role Keyで実行）

データ収集スクリプトが正常に動作することを確認:

```bash
# 仮想環境を有効化
source venv/bin/activate

# 単日データ収集テスト
python scripts/collect_single_day.py

# 成功すればOK
```

## トラブルシューティング

### エラー: "new row violates row-level security policy"

**原因**: Service Role Keyを使用していない、または環境変数が正しく設定されていない

**解決策**:
1. `.env`ファイルを確認
2. `SUPABASE_KEY`にService Role Keyが設定されているか確認
3. 環境変数が正しくロードされているか確認

```bash
# 環境変数の確認
python -c "from app.core.config import settings; print(f'URL: {settings.SUPABASE_URL[:20]}..., Key: {settings.SUPABASE_KEY[:20]}...')"
```

### エラー: "permission denied for table"

**原因**: RLSポリシーが正しく設定されていない

**解決策**:
1. `setup_rls_policies.sql`を再実行
2. ポリシー確認クエリで設定を確認
3. 必要に応じてSupabaseサポートに問い合わせ

## 関連ドキュメント

- [Supabase Row Level Security Documentation](https://supabase.com/docs/guides/auth/row-level-security)
- [PostgreSQL RLS Policies](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)
- プロジェクト内: `CLAUDE.md` - データベース接続設計

## 変更履歴

- 2025-12-21: 初版作成、RLSポリシー統一設定
