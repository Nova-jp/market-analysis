# Legacy SQL Functions

これらのファイルは `calculate_market_amount_unified.sql` に統合されました。

## 統合された関数

以下の4つのSQL RPC関数が、1つの統一関数 `calculate_market_amount_unified()` に統合されました：

1. `calculate_market_amount_batch.sql` - 年単位バッチ計算
2. `calculate_market_amount_monthly.sql` - 月単位バッチ計算
3. `calculate_market_amount_biweekly.sql` - 半月単位バッチ計算
4. `calculate_market_amount_ultra_fast.sql` - 全件計算

## 後方互換性

既存の関数名（`_batch`, `_monthly`, `_biweekly`, `_ultra_fast`）は、統一関数を呼び出すラッパー関数として維持されており、**Pythonコードの変更は不要**です。

## 統一関数の利点

- **コード重複の削減**: 約200行のSQL重複を削減
- **保守性の向上**: 核心ロジックが1箇所に集約
- **柔軟性の向上**: 任意の日付範囲で実行可能（NULL=全件処理）
- **後方互換性**: 既存のスクリプトは無変更で動作

## 使用例

```sql
-- 統一関数（推奨）
SELECT * FROM calculate_market_amount_unified('2024-01-01', '2024-01-31');
SELECT * FROM calculate_market_amount_unified(NULL, NULL);  -- 全件

-- 既存関数名も引き続き使用可能
SELECT * FROM calculate_market_amount_batch('2024-01-01', '2024-12-31');
SELECT * FROM calculate_market_amount_monthly('2024-01-01', '2024-01-31');
SELECT * FROM calculate_market_amount_biweekly('2024-01-01', '2024-01-15');
SELECT * FROM calculate_market_amount_ultra_fast();
```

## アーカイブ情報

- **アーカイブ日**: 2024-12-17
- **理由**: コード重複削減・保守性向上
- **統合先**: `scripts/sql/calculate_market_amount_unified.sql`
- **影響**: なし（後方互換性100%維持）
