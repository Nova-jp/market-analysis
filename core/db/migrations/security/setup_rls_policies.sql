-- =========================================
-- Row Level Security (RLS) ポリシー設定
-- =========================================
-- 目的: データの適切なアクセス制御
--
-- ポリシー方針:
--   - SELECT (読み取り): パブリックアクセス許可
--   - INSERT/UPDATE/DELETE: 認証済みユーザーのみ (Service Role Key)
--
-- 対象テーブル:
--   1. bond_data (JSDA 国債データ)
--   2. boj_holdings (日銀保有国債データ)
--   3. bond_summary (ビュー)
-- =========================================

-- =========================================
-- 1. bond_data テーブル
-- =========================================

-- 既存ポリシーを削除（冪等性確保）
DROP POLICY IF EXISTS "Allow public read access" ON bond_data;
DROP POLICY IF EXISTS "Allow public insert" ON bond_data;
DROP POLICY IF EXISTS "Allow public update" ON bond_data;
DROP POLICY IF EXISTS "Allow public delete" ON bond_data;

-- RLS有効化（既に有効な場合はスキップ）
ALTER TABLE bond_data ENABLE ROW LEVEL SECURITY;

-- 読み取り: 誰でも可能
CREATE POLICY "bond_data_select_public"
ON bond_data
FOR SELECT
USING (true);

-- 書き込み: 認証済みユーザーのみ
CREATE POLICY "bond_data_insert_authenticated"
ON bond_data
FOR INSERT
TO authenticated
WITH CHECK (true);

CREATE POLICY "bond_data_update_authenticated"
ON bond_data
FOR UPDATE
TO authenticated
USING (true)
WITH CHECK (true);

CREATE POLICY "bond_data_delete_authenticated"
ON bond_data
FOR DELETE
TO authenticated
USING (true);

-- =========================================
-- 2. boj_holdings テーブル
-- =========================================

-- 既存ポリシーを削除（冪等性確保）
DROP POLICY IF EXISTS "Allow read access" ON boj_holdings;
DROP POLICY IF EXISTS "Allow insert access" ON boj_holdings;

-- RLS有効化
ALTER TABLE boj_holdings ENABLE ROW LEVEL SECURITY;

-- 読み取り: 誰でも可能
CREATE POLICY "boj_holdings_select_public"
ON boj_holdings
FOR SELECT
USING (true);

-- 書き込み: 認証済みユーザーのみ
CREATE POLICY "boj_holdings_insert_authenticated"
ON boj_holdings
FOR INSERT
TO authenticated
WITH CHECK (true);

CREATE POLICY "boj_holdings_update_authenticated"
ON boj_holdings
FOR UPDATE
TO authenticated
USING (true)
WITH CHECK (true);

CREATE POLICY "boj_holdings_delete_authenticated"
ON boj_holdings
FOR DELETE
TO authenticated
USING (true);

-- =========================================
-- 3. bond_summary ビュー (参考)
-- =========================================
-- 注意: VIEWはRLSポリシーを直接持てません
-- ベーステーブル (bond_data) のRLSポリシーが適用されます
--
-- もし追加のアクセス制御が必要な場合は、
-- セキュリティ定義関数 (SECURITY DEFINER) を使用するか、
-- マテリアライズドビューに変換してください

-- =========================================
-- ポリシー確認クエリ
-- =========================================
-- 以下のクエリで設定されたポリシーを確認できます:
--
-- SELECT schemaname, tablename, policyname, roles, cmd, qual, with_check
-- FROM pg_policies
-- WHERE tablename IN ('bond_data', 'boj_holdings')
-- ORDER BY tablename, policyname;
--
-- RLS有効化状態の確認:
-- SELECT tablename, rowsecurity
-- FROM pg_tables
-- WHERE tablename IN ('bond_data', 'boj_holdings');
