# Supabase接続問題 検証レポート

**作成日時**: 2025-10-16
**実行者**: Claude Code Assistant
**プロジェクト**: Market Analytics System v3.0

---

## 📊 検証結果サマリー

| 項目 | 状態 | 詳細 |
|------|------|------|
| **環境変数** | ✅ 正常 | `.env`ファイル存在、全キー設定済み |
| **Anon Key接続** | ✅ 正常 | 読み取り成功（RLS適用） |
| **Service Role Key接続** | ✅ 正常 | 読み取り成功（全権限） |
| **データベーススキーマ** | ⚠️ **不一致** | ローカルSQLと本番DBで構造が異なる |
| **データ書き込み** | ❌ **失敗** | スキーマ不一致により書き込み不可 |
| **自動データ収集** | ❌ **停止中** | スキーマ不一致により動作不可 |

---

## 🔍 発見された問題

### **問題1: データベーススキーマの不一致**

#### 期待されるスキーマ（scripts/create_bond_table.sql）
```sql
-- 利払日情報
interest_payment_month INTEGER,  -- J列: 利払日（月）
interest_payment_day INTEGER,    -- K列: 利払日（日）
```

#### 実際のSupabaseスキーマ
```sql
-- 利払日情報
interest_payment_date VARCHAR,   -- J列: 利払日（MM/DD形式）
interest_payment_day INTEGER,    -- K列: 利払日（日）
```

**差異**:
- ❌ `interest_payment_month`カラムが**存在しない**
- ✅ `interest_payment_date`カラムが**存在する**（MM/DD形式の文字列）

#### 影響範囲
1. **データ収集スクリプト**: `data/processors/bond_data_processor.py`が`interest_payment_month`を含むデータを送信しようとして失敗
2. **Webアプリ**: 読み取りは動作するが、古いデータ（2025-10-14のみ）しか存在しない
3. **自動データ収集**: Cloud Runのスケジューラーも同様にスキーマエラーで失敗

---

### **問題2: 最新データが不足**

#### 現状
- **最新データ**: 2025-10-14（1日分のみ）
- **期待**: 毎日18:00に自動更新

#### 原因
スキーマ不一致により、10月14日以降のデータ収集が**すべて失敗**している。

---

## 💡 修正方針

### **方針A: Supabaseスキーマを更新（推奨）**

**メリット**:
- ローカルのSQL定義と一致
- 月・日を別々のINTEGER型で管理（クエリが高速）
- 将来の拡張性が高い

**手順**:
1. Supabaseダッシュボードで以下のSQLを実行:
```sql
-- 新規カラム追加
ALTER TABLE bond_data ADD COLUMN interest_payment_month INTEGER;

-- 既存データ移行
UPDATE bond_data
SET interest_payment_month = CAST(SPLIT_PART(interest_payment_date, '/', 1) AS INTEGER)
WHERE interest_payment_date IS NOT NULL;

-- 古いカラム削除
ALTER TABLE bond_data DROP COLUMN interest_payment_date;

-- インデックス作成
CREATE INDEX idx_interest_payment ON bond_data(interest_payment_month, interest_payment_day);
```

2. 検証:
```bash
python3 scripts/verify_supabase_write.py
```

---

### **方針B: コードをSupabaseスキーマに合わせる（非推奨）**

**デメリット**:
- 複数のファイルを修正する必要がある
- データ型が文字列になり、クエリ性能が低下
- CLAUDE.mdの設計思想と矛盾

**修正が必要なファイル**:
1. `data/processors/bond_data_processor.py`
2. `data/collectors/historical_bond_collector.py`
3. `app/api/endpoints/*.py`（クエリ処理）
4. `scripts/create_bond_table.sql`（ドキュメント更新）

---

## 🚀 推奨アクションプラン

### **ステップ1: Supabaseスキーマ修正（本番環境）**
```sql
-- Supabaseダッシュボード → SQL Editor で実行:

-- 1. 新しいカラム追加
ALTER TABLE bond_data ADD COLUMN interest_payment_month INTEGER;

-- 2. 既存データ移行
UPDATE bond_data
SET interest_payment_month = CAST(SPLIT_PART(interest_payment_date, '/', 1) AS INTEGER)
WHERE interest_payment_date IS NOT NULL;

-- 3. 古いカラム削除
ALTER TABLE bond_data DROP COLUMN interest_payment_date;

-- 4. インデックス作成
CREATE INDEX idx_interest_payment ON bond_data(interest_payment_month, interest_payment_day);
```

### **ステップ2: ローカル検証**
```bash
# 書き込みテスト
python3 scripts/verify_supabase_write.py

# 実際のデータ収集テスト（今日のデータ）
python3 scripts/collect_single_day.py 2025-10-16
```

### **ステップ3: Webアプリ動作確認**
```bash
# ローカルサーバー起動
python3 -m app.web.main

# ブラウザで確認
# → http://127.0.0.1:8000
# → クイック選択に最新日が表示されるか確認
```

### **ステップ4: Cloud Run自動収集の再開**
```bash
# 手動実行テスト
gcloud scheduler jobs run daily-data-collection --location="asia-northeast1"

# ログ確認
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=market-analytics" --limit=50
```

---

## 📝 技術的詳細

### **実際のテーブル構造（Supabase本番環境）**

```
合計カラム数: 29

主要カラム:
1. id (UUID)
2. trade_date (DATE)
3. issue_type (INTEGER)
4. bond_code (VARCHAR)
5. bond_name (VARCHAR)
6. due_date (DATE)
7. coupon_rate (DECIMAL)
8. ave_compound_yield (DECIMAL)
9. ave_price (DECIMAL)
10. price_change (DECIMAL)
11. interest_payment_date (VARCHAR) ← ⚠️ 問題のカラム
12. interest_payment_day (INTEGER)
13. ave_simple_yield (DECIMAL)
...
```

### **期待されるテーブル構造（scripts/create_bond_table.sql）**

```
合計カラム数: 29

差異:
- interest_payment_month (INTEGER) ← ✅ 必要
- interest_payment_date (VARCHAR) ← ❌ 不要（削除）
```

---

## ✅ 検証に使用したスクリプト

以下のスクリプトを新規作成しました:

1. **scripts/verify_supabase_read.py**
   - 環境変数確認
   - Anon Key / Service Role Key接続テスト
   - 日付クエリテスト

2. **scripts/verify_supabase_write.py**
   - テストデータ書き込み
   - 書き込み結果検証
   - テストデータクリーンアップ

3. **scripts/check_db_schema.py**
   - 実際のテーブル構造取得
   - 期待スキーマとの比較

---

## 🎯 結論

### **根本原因**
Supabaseの本番データベースが、ローカルの`scripts/create_bond_table.sql`と**異なるスキーマ**で構築されている。

具体的には、利払日情報が以下のように異なる:
- **期待**: `interest_payment_month` (INTEGER) + `interest_payment_day` (INTEGER)
- **実際**: `interest_payment_date` (VARCHAR, "MM/DD"形式) + `interest_payment_day` (INTEGER)

### **影響**
- データ収集スクリプトが`interest_payment_month`を送信 → Supabaseが受け付けず → エラー
- 10月14日以降のデータが収集されていない
- Webアプリは読み取り可能だが、表示できるデータが古い

### **推奨対応**
**方針A（Supabaseスキーマ更新）**を推奨します。

理由:
- ローカル設計と一致する
- データ型が適切（INTEGER型）
- クエリ性能が向上
- コード変更が不要

---

## 📞 次のステップ

1. **ユーザーに確認**: Supabaseスキーマを更新してよいか
2. **スキーマ更新実行**: 上記SQLを実行
3. **検証**: 書き込みテスト、データ収集テスト
4. **本番確認**: Webアプリとスケジューラーの動作確認

---

**レポート終了**
