-- =====================================================
-- Cloud SQL用テーブル作成スクリプト
-- Supabase RLSポリシーを除外した標準PostgreSQL版
-- =====================================================

-- 既存テーブル削除（クリーンインストール用）
DROP TABLE IF EXISTS bond_data CASCADE;
DROP TABLE IF EXISTS boj_holdings CASCADE;
DROP TABLE IF EXISTS irs_data CASCADE;
DROP TABLE IF EXISTS bond_auction CASCADE;

-- =====================================================
-- 1. bond_data テーブル（JSDA国債データ）
-- =====================================================
CREATE TABLE bond_data (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,

    -- A-E列: 基本情報
    trade_date DATE NOT NULL,
    issue_type INTEGER NOT NULL,
    bond_code VARCHAR(50) NOT NULL,
    bond_name VARCHAR(200) NOT NULL,
    due_date DATE,

    -- F-I列: 価格・利回り情報
    coupon_rate DECIMAL(8,4),
    ave_compound_yield DECIMAL(8,4),
    ave_price DECIMAL(10,3),
    price_change DECIMAL(8,3),

    -- J-K列: 利払い情報
    interest_payment_month INTEGER,
    interest_payment_day INTEGER,

    -- O列: 単利利回り
    ave_simple_yield DECIMAL(8,4),

    -- P-Q列: 最高値情報
    high_price DECIMAL(10,3),
    high_simple_yield DECIMAL(8,4),

    -- R-S列: 最低値情報
    low_price DECIMAL(10,3),
    low_simple_yield DECIMAL(8,4),

    -- U列: 統計情報
    reporting_members INTEGER,

    -- V-AC列: 詳細統計値
    highest_compound_yield DECIMAL(8,4),
    highest_price_change DECIMAL(8,3),
    lowest_compound_yield DECIMAL(8,4),
    lowest_price_change DECIMAL(8,3),
    median_compound_yield DECIMAL(8,4),
    median_simple_yield DECIMAL(8,4),
    median_price DECIMAL(10,3),
    median_price_change DECIMAL(8,3),

    -- メタデータ
    data_source VARCHAR(20) DEFAULT 'JSDA',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- 制約
    UNIQUE(trade_date, bond_code)
);

-- インデックス作成
CREATE INDEX idx_jsda_trade_date ON bond_data(trade_date);
CREATE INDEX idx_jsda_issue_type ON bond_data(issue_type);
CREATE INDEX idx_jsda_bond_code ON bond_data(bond_code);
CREATE INDEX idx_jsda_ave_compound_yield ON bond_data(ave_compound_yield);
CREATE INDEX idx_jsda_ave_price ON bond_data(ave_price);
CREATE INDEX idx_jsda_due_date ON bond_data(due_date);

-- 分析用ビュー
CREATE VIEW bond_summary AS
SELECT
    trade_date,
    issue_type,
    CASE
        WHEN issue_type = 1 THEN 'T-bills'
        WHEN issue_type = 2 THEN '国債'
        ELSE '不明'
    END as issue_type_name,
    COUNT(*) as bond_count,
    AVG(ave_compound_yield) as avg_compound_yield,
    AVG(ave_price) as avg_price,
    AVG(reporting_members) as avg_reporting_members
FROM bond_data
WHERE ave_compound_yield IS NOT NULL
  AND ave_price IS NOT NULL
GROUP BY trade_date, issue_type
ORDER BY trade_date DESC, issue_type;

-- =====================================================
-- 2. boj_holdings テーブル（日銀保有国債）
-- =====================================================
CREATE TABLE boj_holdings (
    data_date DATE NOT NULL,
    bond_code TEXT NOT NULL,
    bond_type TEXT NOT NULL,
    issue_number INTEGER NOT NULL,
    face_value BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (data_date, bond_code)
);

CREATE INDEX idx_boj_holdings_data_date ON boj_holdings (data_date);
CREATE INDEX idx_boj_holdings_bond_code ON boj_holdings (bond_code);
CREATE INDEX idx_boj_holdings_bond_type ON boj_holdings (bond_type);

COMMENT ON TABLE boj_holdings IS '日本銀行が保有する国債の銘柄別残高';

-- =====================================================
-- 3. irs_data テーブル（金利スワップ）
-- =====================================================
CREATE TABLE irs_data (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    product_type VARCHAR(20) NOT NULL,
    tenor VARCHAR(20) NOT NULL,
    rate DECIMAL(10, 5) NOT NULL,
    unit VARCHAR(10) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_irs_rate UNIQUE (trade_date, product_type, tenor)
);

CREATE INDEX idx_irs_trade_date ON irs_data(trade_date DESC);
CREATE INDEX idx_irs_product_type ON irs_data(product_type);
CREATE INDEX idx_irs_date_product ON irs_data(trade_date DESC, product_type);

COMMENT ON TABLE irs_data IS '金利スワップ清算値段（JPX日次データ）';

-- =====================================================
-- 4. bond_auction テーブル（国債入札結果）
-- =====================================================
CREATE TABLE bond_auction (
    bond_code VARCHAR(9) NOT NULL,
    auction_date DATE NOT NULL,
    issue_number INTEGER NOT NULL,
    issue_date DATE NOT NULL,
    maturity_date DATE NOT NULL,
    coupon_rate NUMERIC(6, 4),
    planned_amount INTEGER,
    offered_amount INTEGER,
    allocated_amount NUMERIC(10, 2),
    average_price NUMERIC(8, 4),
    average_yield NUMERIC(8, 6),
    lowest_price NUMERIC(8, 4),
    highest_yield NUMERIC(8, 6),
    fixed_rate_or_noncompetitive NUMERIC(10, 2),
    type1_noncompetitive NUMERIC(10, 2),
    type2_noncompetitive NUMERIC(10, 2),
    data_source VARCHAR(100) DEFAULT 'MOF_JGB_Historical_Data',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bond_code, auction_date)
);

CREATE INDEX idx_bond_auction_bond_code ON bond_auction(bond_code);
CREATE INDEX idx_bond_auction_date ON bond_auction(auction_date);
CREATE INDEX idx_bond_auction_issue_number ON bond_auction(issue_number);
CREATE INDEX idx_bond_auction_maturity_date ON bond_auction(maturity_date);
CREATE INDEX idx_bond_auction_code_date ON bond_auction(bond_code, auction_date);

COMMENT ON TABLE bond_auction IS '国債入札結果データ（財務省ヒストリカルデータ）';

-- =====================================================
-- 完了メッセージ
-- =====================================================
DO $$
BEGIN
    RAISE NOTICE 'Cloud SQL用テーブル作成完了';
    RAISE NOTICE '作成テーブル: bond_data, boj_holdings, irs_data, bond_auction';
END $$;
