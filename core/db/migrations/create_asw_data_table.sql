-- Minimalist table for storing Asset Swap Spread (ASW) calculations
-- Primary Key is (trade_date, bond_code) to allow daily tracking per bond
-- Redundant info (name, maturity, yield) is omitted as it exists in bond_data

DROP TABLE IF EXISTS "ASW_data";

CREATE TABLE IF NOT EXISTS "ASW_data" (
    trade_date DATE NOT NULL,
    bond_code VARCHAR(20) NOT NULL,
    
    -- Asset Swap Spreads (ASW) only
    asw_act365_pa FLOAT, -- Payment Annual
    asw_act365_sa FLOAT, -- Payment Semi-Annual
    
    calculation_log JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (trade_date, bond_code)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_asw_data_date ON "ASW_data"(trade_date);
CREATE INDEX IF NOT EXISTS idx_asw_data_code ON "ASW_data"(bond_code);