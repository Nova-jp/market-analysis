-- =====================================================
-- 金利スワップ清算値段テーブル
-- Interest Rate Swap Settlement Rates
-- =====================================================
-- データソース: JPX (日本取引所グループ)
-- URL: https://www.jpx.co.jp/jscc/toukei_irs.html
-- 更新頻度: 日次
-- =====================================================

CREATE TABLE IF NOT EXISTS irs_settlement_rates (
    -- 主キー
    id BIGSERIAL PRIMARY KEY,

    -- 取引日
    trade_date DATE NOT NULL,

    -- プロダクトタイプ
    -- 'OIS', '3M_TIBOR', '6M_TIBOR', '1M_TIBOR'
    product_type VARCHAR(20) NOT NULL,

    -- 期間 (Tenor)
    -- 例: '1D', '1W', '1M', '3M(0×3)', '1Y', '10Y' など
    tenor VARCHAR(20) NOT NULL,

    -- 金利
    rate DECIMAL(10, 5) NOT NULL,

    -- 単位
    -- '%' または 'bp' (ベーシスポイント)
    unit VARCHAR(10) NOT NULL,

    -- メタデータ
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- 一意制約: 同一日・プロダクト・期間の重複を防ぐ
    CONSTRAINT unique_irs_rate UNIQUE (trade_date, product_type, tenor)
);

-- =====================================================
-- インデックス作成
-- =====================================================

-- 取引日でのクエリ最適化
CREATE INDEX IF NOT EXISTS idx_irs_trade_date
ON irs_settlement_rates(trade_date DESC);

-- プロダクトタイプでのクエリ最適化
CREATE INDEX IF NOT EXISTS idx_irs_product_type
ON irs_settlement_rates(product_type);

-- 複合インデックス: 日付とプロダクトでのフィルタリング
CREATE INDEX IF NOT EXISTS idx_irs_date_product
ON irs_settlement_rates(trade_date DESC, product_type);

-- =====================================================
-- Row Level Security (RLS) ポリシー設定
-- =====================================================

-- RLSを有効化
ALTER TABLE irs_settlement_rates ENABLE ROW LEVEL SECURITY;

-- 読み取りポリシー: 誰でも読み取り可能
CREATE POLICY "irs_settlement_rates_read_policy"
ON irs_settlement_rates
FOR SELECT
USING (true);

-- 書き込みポリシー: 認証済みユーザーのみ
CREATE POLICY "irs_settlement_rates_write_policy"
ON irs_settlement_rates
FOR ALL
USING (auth.role() = 'authenticated');

-- =====================================================
-- コメント追加
-- =====================================================

COMMENT ON TABLE irs_settlement_rates IS
'金利スワップ清算値段（JPX日次データ）';

COMMENT ON COLUMN irs_settlement_rates.trade_date IS
'取引日';

COMMENT ON COLUMN irs_settlement_rates.product_type IS
'プロダクトタイプ (OIS, 3M_TIBOR, 6M_TIBOR, 1M_TIBOR)';

COMMENT ON COLUMN irs_settlement_rates.tenor IS
'期間 (1D, 1W, 1M, 3M(0×3), 1Y, 10Yなど)';

COMMENT ON COLUMN irs_settlement_rates.rate IS
'金利';

COMMENT ON COLUMN irs_settlement_rates.unit IS
'単位 (%またはbp)';
