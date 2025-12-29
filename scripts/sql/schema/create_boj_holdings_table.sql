-- 日銀保有国債 銘柄別残高テーブル
-- データソース: https://www.boj.or.jp/statistics/boj/other/mei/index.htm

CREATE TABLE IF NOT EXISTS boj_holdings (
    data_date DATE NOT NULL,                    -- データ基準日
    bond_code TEXT NOT NULL,                    -- 銘柄コード（9桁0パディング）
    bond_type TEXT NOT NULL,                    -- 国債種別（2年債、5年債、10年債等）
    issue_number INTEGER NOT NULL,              -- 回号（Issue Number）
    face_value BIGINT,                          -- 額面金額（億円、整数）
    created_at TIMESTAMP DEFAULT NOW(),         -- 登録日時

    -- 複合主キー（日付 + 銘柄コード）
    PRIMARY KEY (data_date, bond_code)
);

-- インデックス作成
CREATE INDEX IF NOT EXISTS idx_boj_holdings_data_date ON boj_holdings (data_date);
CREATE INDEX IF NOT EXISTS idx_boj_holdings_bond_code ON boj_holdings (bond_code);
CREATE INDEX IF NOT EXISTS idx_boj_holdings_bond_type ON boj_holdings (bond_type);

-- コメント追加
COMMENT ON TABLE boj_holdings IS '日本銀行が保有する国債の銘柄別残高';
COMMENT ON COLUMN boj_holdings.data_date IS 'データ基準日（○○年○月○日現在）';
COMMENT ON COLUMN boj_holdings.bond_code IS '銘柄コード（9桁0パディング、bond_dataテーブルと同形式）';
COMMENT ON COLUMN boj_holdings.bond_type IS '国債種別（2年債、5年債、10年債、20年債、30年債、40年債、物価連動債等）';
COMMENT ON COLUMN boj_holdings.issue_number IS '回号（Issue Number）';
COMMENT ON COLUMN boj_holdings.face_value IS '額面金額（億円）';
COMMENT ON COLUMN boj_holdings.created_at IS 'レコード登録日時';

-- RLSポリシー設定
ALTER TABLE boj_holdings ENABLE ROW LEVEL SECURITY;

-- 読み取り: 誰でも可能
CREATE POLICY "boj_holdings_select_public"
ON boj_holdings
FOR SELECT
USING (true);

-- 書き込み: 認証済みユーザーのみ (Service Role Key)
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
