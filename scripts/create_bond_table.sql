-- JSDA完全版テーブル（選択25列）
-- L(11), M(12), N(13), T(19)を除く全列を取得

-- 既存テーブル削除
DROP TABLE IF EXISTS clean_bond_data CASCADE;
DROP TABLE IF EXISTS bond_data CASCADE;

-- JSDAデータテーブル作成（25列）
CREATE TABLE clean_bond_data (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,

    -- A-E列: 基本情報
    trade_date DATE NOT NULL,                    -- A列: 日付
    issue_type INTEGER NOT NULL,                 -- B列: 銘柄種別 (1=T-bills, 2=国債)  
    bond_code VARCHAR(50) NOT NULL,              -- C列: 銘柄コード
    bond_name VARCHAR(200) NOT NULL,             -- D列: 銘柄名
    due_date DATE,                               -- E列: 償還期日
    
    -- F-I列: 価格・利回り情報
    coupon_rate DECIMAL(8,4),                    -- F列: 利率 (99.999=null)
    ave_compound_yield DECIMAL(8,4),             -- G列: 平均値複利 (999.999=null)
    ave_price DECIMAL(10,3),                     -- H列: 平均値単価 (999.99=null)
    price_change DECIMAL(8,3),                   -- I列: 平均値単価前日比
    
    -- J-K列: 利払い情報
    interest_payment_month INTEGER,              -- J列: 利払日（月）
    interest_payment_day INTEGER,                -- K列: 利払日（日）
    
    -- O列: 単利利回り（L,M,Nはスキップ）
    ave_simple_yield DECIMAL(8,4),               -- O列: 平均値単利
    
    -- P-Q列: 最高値情報
    high_price DECIMAL(10,3),                    -- P列: 最高値単価
    high_simple_yield DECIMAL(8,4),              -- Q列: 最高値単利
    
    -- R-S列: 最低値情報
    low_price DECIMAL(10,3),                     -- R列: 最低値単価
    low_simple_yield DECIMAL(8,4),               -- S列: 最低値単利
    
    -- U列: 統計情報（Tはスキップ）
    reporting_members INTEGER,                   -- U列: 報告社数
    
    -- V-AC列: 詳細統計値
    highest_compound_yield DECIMAL(8,4),         -- V列: 最高値複利
    highest_price_change DECIMAL(8,3),           -- W列: 最高値単価前日比
    lowest_compound_yield DECIMAL(8,4),          -- X列: 最低値複利
    lowest_price_change DECIMAL(8,3),            -- Y列: 最低値単価前日比
    median_compound_yield DECIMAL(8,4),          -- Z列: 中央値複利
    median_simple_yield DECIMAL(8,4),            -- AA列: 中央値単利
    median_price DECIMAL(10,3),                  -- AB列: 中央値単価
    median_price_change DECIMAL(8,3),            -- AC列: 中央値単価前日比
    
    -- メタデータ
    data_source VARCHAR(20) DEFAULT 'JSDA',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- 制約
    UNIQUE(trade_date, bond_code)
);

-- インデックス作成
CREATE INDEX idx_jsda_trade_date ON clean_bond_data(trade_date);
CREATE INDEX idx_jsda_issue_type ON clean_bond_data(issue_type);
CREATE INDEX idx_jsda_bond_code ON clean_bond_data(bond_code);
CREATE INDEX idx_jsda_ave_compound_yield ON clean_bond_data(ave_compound_yield);
CREATE INDEX idx_jsda_ave_price ON clean_bond_data(ave_price);
CREATE INDEX idx_jsda_due_date ON clean_bond_data(due_date);

-- RLS設定（開発環境用）
ALTER TABLE clean_bond_data ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read access" ON clean_bond_data FOR SELECT USING (true);
CREATE POLICY "Allow public insert" ON clean_bond_data FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow public update" ON clean_bond_data FOR UPDATE USING (true);
CREATE POLICY "Allow public delete" ON clean_bond_data FOR DELETE USING (true);

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
FROM clean_bond_data
WHERE ave_compound_yield IS NOT NULL
  AND ave_price IS NOT NULL
GROUP BY trade_date, issue_type
ORDER BY trade_date DESC, issue_type;