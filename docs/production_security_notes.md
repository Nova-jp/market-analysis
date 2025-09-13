# 本番環境セキュリティチェックリスト

## データベースセキュリティ

### ⚠️ 現在の開発環境設定（本番では変更必要）
```sql
-- 開発環境: 匿名ユーザーでもデータ操作可能
CREATE POLICY "Allow public insert on bond_data" ON bond_data FOR INSERT WITH CHECK (true);
```

### ✅ 本番環境で実装すべきセキュリティポリシー
```sql
-- 本番環境: 認証されたユーザーのみデータ操作可能
DROP POLICY IF EXISTS "Allow public insert on bond_data" ON bond_data;
CREATE POLICY "Allow authenticated insert on bond_data" ON bond_data 
FOR INSERT WITH CHECK (auth.role() = 'authenticated');

-- 同様に他のテーブルも修正
DROP POLICY IF EXISTS "Allow public insert on swap_data" ON swap_data;
CREATE POLICY "Allow authenticated insert on swap_data" ON swap_data 
FOR INSERT WITH CHECK (auth.role() = 'authenticated');

DROP POLICY IF EXISTS "Allow public insert on economic_indicators" ON economic_indicators;
CREATE POLICY "Allow authenticated insert on economic_indicators" ON economic_indicators 
FOR INSERT WITH CHECK (auth.role() = 'authenticated');

DROP POLICY IF EXISTS "Allow public insert on analysis_results" ON analysis_results;
CREATE POLICY "Allow authenticated insert on analysis_results" ON analysis_results 
FOR INSERT WITH CHECK (auth.role() = 'authenticated');
```

## 追加のセキュリティ対策

### API キー管理
- [ ] Service Role Key の使用（データ投入用）
- [ ] anon key の制限確認
- [ ] 環境変数の本番環境分離

### 認証システム
- [ ] Supabase Auth の実装
- [ ] ユーザー権限管理
- [ ] API 呼び出し制限

### データアクセス制限
- [ ] IP制限の検討
- [ ] レート制限の実装
- [ ] ログ監視の設定

---
**作成日**: 2025-09-06  
**最終更新**: 開発段階でセキュリティポリシーを緩和