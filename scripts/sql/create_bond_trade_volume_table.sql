CREATE TABLE IF NOT EXISTS bond_trade_volume_by_investor (
    id SERIAL PRIMARY KEY,
    reference_date DATE NOT NULL,
    investor_type VARCHAR(100) NOT NULL,
    transaction_type VARCHAR(20) NOT NULL, -- 'total', 'outright', 'repo'
    side VARCHAR(20) NOT NULL,            -- 'volume', 'sell', 'buy', 'net'
    
    -- JGBs
    jgb_total NUMERIC,                    -- 国債
    jgb_super_long NUMERIC,               -- 超長期
    jgb_long NUMERIC,                     -- 利付長期
    jgb_medium NUMERIC,                   -- 利付中期
    jgb_discount NUMERIC,                 -- 割引
    jgb_tbill NUMERIC,                    -- 国庫短期証券等
    
    -- Other Public
    municipal_public NUMERIC,             -- 公募地方債
    govt_guaranteed NUMERIC,              -- 政府保証債
    filp_agency NUMERIC,                  -- 財投機関債等
    bank_debenture NUMERIC,               -- 金融債
    samurai NUMERIC,                      -- 円貨建外国債
    
    -- Corporate
    corporate_total NUMERIC,              -- 社債
    corporate_electric NUMERIC,           -- 電力債
    corporate_general NUMERIC,            -- 一般債
    
    -- Specialized
    abs NUMERIC,                          -- 特定社債
    convertible NUMERIC,                  -- 新株予約権付社債
    
    -- Private Offering
    private_offering_total NUMERIC,       -- 非公募債
    private_municipal NUMERIC,            -- 地方債
    private_others NUMERIC,               -- その他
    
    -- Grand Total
    grand_total NUMERIC,                  -- 合計
    
    -- Short-term (CP)
    cp_total NUMERIC,                     -- 短期社債等 計
    cp_foreign NUMERIC,                   -- うち非居住者発行分
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(reference_date, investor_type, transaction_type, side)
);

CREATE INDEX IF NOT EXISTS idx_bond_trade_volume_date ON bond_trade_volume_by_investor(reference_date);
CREATE INDEX IF NOT EXISTS idx_bond_trade_volume_investor ON bond_trade_volume_by_investor(investor_type);