-- =====================================================
-- mof_international_transactions テーブル
-- 対内対外証券投資（週次・財務省）
-- =====================================================

DROP TABLE IF EXISTS mof_international_transactions;

CREATE TABLE mof_international_transactions (
    -- 期間
    start_date DATE NOT NULL PRIMARY KEY,
    end_date DATE NOT NULL,
    
    -- 1. 対外証券投資（居住者による取得・処分） Portfolio Investment Assets
    outward_equity_acquisition BIGINT, -- 株式・投資ファンド持分 取得
    outward_equity_disposition BIGINT, -- 株式・投資ファンド持分 処分
    outward_equity_net BIGINT,         -- 株式・投資ファンド持分 ネット
    
    outward_long_term_acquisition BIGINT, -- 中長期債 取得
    outward_long_term_disposition BIGINT, -- 中長期債 処分
    outward_long_term_net BIGINT,         -- 中長期債 ネット
    
    outward_subtotal_net BIGINT, -- 小計 ネット
    
    outward_short_term_acquisition BIGINT, -- 短期債 取得
    outward_short_term_disposition BIGINT, -- 短期債 処分
    outward_short_term_net BIGINT,         -- 短期債 ネット
    
    outward_total_net BIGINT, -- 合計 ネット
    
    -- 2. 対内証券投資（非居住者による取得・処分） Portfolio Investment Liabilities
    inward_equity_acquisition BIGINT, -- 株式・投資ファンド持分 取得
    inward_equity_disposition BIGINT, -- 株式・投資ファンド持分 処分
    inward_equity_net BIGINT,         -- 株式・投資ファンド持分 ネット
    
    inward_long_term_acquisition BIGINT, -- 中長期債 取得
    inward_long_term_disposition BIGINT, -- 中長期債 処分
    inward_long_term_net BIGINT,         -- 中長期債 ネット
    
    inward_subtotal_net BIGINT, -- 小計 ネット
    
    inward_short_term_acquisition BIGINT, -- 短期債 取得
    inward_short_term_disposition BIGINT, -- 短期債 処分
    inward_short_term_net BIGINT,         -- 短期債 ネット
    
    inward_total_net BIGINT, -- 合計 ネット
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_mof_intl_trans_end_date ON mof_international_transactions(end_date);

COMMENT ON TABLE mof_international_transactions IS '対内対外証券投資（週次・財務省）';
COMMENT ON COLUMN mof_international_transactions.outward_equity_acquisition IS '単位: 億円';