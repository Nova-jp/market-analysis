-- ============================================================
-- bond_auction テーブル作成スクリプト
-- ============================================================
-- 国債入札結果データ（財務省ヒストリカルデータ）
--
-- データソース: https://www.mof.go.jp/jgbs/reference/appendix/jgb_historical_data.xls
-- 作成日: 2025-01-13
-- ============================================================

CREATE TABLE IF NOT EXISTS bond_auction (
    -- ========== 主キー（複合キー） ==========
    bond_code VARCHAR(9) NOT NULL,           -- 銘柄コード（9桁: 回号5桁 + 種類コード4桁）
    auction_date DATE NOT NULL,              -- 入札日

    -- ========== 基本情報 ==========
    issue_number INTEGER NOT NULL,           -- 回号
    issue_date DATE NOT NULL,                -- 発行日
    maturity_date DATE NOT NULL,             -- 償還日
    coupon_rate NUMERIC(6, 4),               -- 表面利率（%）

    -- ========== 発行規模（単位: 億円） ==========
    planned_amount INTEGER,                  -- 発行予定額
    offered_amount INTEGER,                  -- 応募額
    allocated_amount NUMERIC(10, 2),         -- 落札・割当額

    -- ========== 価格・利回り（価格競争入札） ==========
    average_price NUMERIC(8, 4),             -- 平均価格（額面100円あたり）
    average_yield NUMERIC(8, 6),             -- 平均利回（%）
    lowest_price NUMERIC(8, 4),              -- 最低価格（額面100円あたり）
    highest_yield NUMERIC(8, 6),             -- 最高利回（%）

    -- ========== 非価格競争入札（単位: 億円） ==========
    fixed_rate_or_noncompetitive NUMERIC(10, 2),  -- 定率/非競争
    type1_noncompetitive NUMERIC(10, 2),          -- 第Ⅰ非価格競争
    type2_noncompetitive NUMERIC(10, 2),          -- 第Ⅱ非価格競争

    -- ========== メタデータ ==========
    data_source VARCHAR(100) DEFAULT 'MOF_JGB_Historical_Data',  -- データソース
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- ========== 主キー制約 ==========
    PRIMARY KEY (bond_code, auction_date)
);

-- ============================================================
-- インデックス作成
-- ============================================================

-- bond_code での検索用
CREATE INDEX IF NOT EXISTS idx_bond_auction_bond_code
    ON bond_auction(bond_code);

-- 入札日での検索用
CREATE INDEX IF NOT EXISTS idx_bond_auction_date
    ON bond_auction(auction_date);

-- 回号での検索用
CREATE INDEX IF NOT EXISTS idx_bond_auction_issue_number
    ON bond_auction(issue_number);

-- 償還日での検索用
CREATE INDEX IF NOT EXISTS idx_bond_auction_maturity_date
    ON bond_auction(maturity_date);

-- 複合インデックス（bond_codeと入札日の範囲検索用）
CREATE INDEX IF NOT EXISTS idx_bond_auction_code_date
    ON bond_auction(bond_code, auction_date);

-- ============================================================
-- テーブルコメント
-- ============================================================

COMMENT ON TABLE bond_auction IS
    '国債入札結果データ（財務省ヒストリカルデータ）- 一次市場データ';

-- カラムコメント
COMMENT ON COLUMN bond_auction.bond_code IS
    '銘柄コード（9桁: 回号5桁0パディング + 種類コード4桁）例: 004640042 = 464回2年債';

COMMENT ON COLUMN bond_auction.auction_date IS
    '入札日（主キーの一部）- 同一銘柄で複数回入札が実施される場合がある';

COMMENT ON COLUMN bond_auction.issue_number IS
    '回号 - 債券の発行回次を示す番号';

COMMENT ON COLUMN bond_auction.issue_date IS
    '発行日 - 実際に国債が発行される日';

COMMENT ON COLUMN bond_auction.maturity_date IS
    '償還日 - 元本が償還される満期日';

COMMENT ON COLUMN bond_auction.coupon_rate IS
    '表面利率（単位: %）- クーポンレート';

COMMENT ON COLUMN bond_auction.planned_amount IS
    '発行予定額（単位: 億円）- 入札前に計画された発行額';

COMMENT ON COLUMN bond_auction.offered_amount IS
    '応募額（単位: 億円）- 入札における応募総額';

COMMENT ON COLUMN bond_auction.allocated_amount IS
    '落札・割当額（単位: 億円）- 実際に落札・割当された金額';

COMMENT ON COLUMN bond_auction.average_price IS
    '平均価格（額面100円あたり）- 価格競争入札の平均落札価格';

COMMENT ON COLUMN bond_auction.average_yield IS
    '平均利回（単位: %）- 価格競争入札の平均落札利回り';

COMMENT ON COLUMN bond_auction.lowest_price IS
    '最低価格（額面100円あたり）- 価格競争入札の最低落札価格';

COMMENT ON COLUMN bond_auction.highest_yield IS
    '最高利回（単位: %）- 価格競争入札の最高落札利回り';

COMMENT ON COLUMN bond_auction.fixed_rate_or_noncompetitive IS
    '定率/非競争（単位: 億円）- 定率入札または非競争入札の金額';

COMMENT ON COLUMN bond_auction.type1_noncompetitive IS
    '第Ⅰ非価格競争（単位: 億円）- 第一種非価格競争入札の金額';

COMMENT ON COLUMN bond_auction.type2_noncompetitive IS
    '第Ⅱ非価格競争（単位: 億円）- 第二種非価格競争入札の金額';

COMMENT ON COLUMN bond_auction.data_source IS
    'データソース - データの取得元を示す識別子';

-- ============================================================
-- Row Level Security (RLS) 設定
-- ============================================================

-- RLSを有効化
ALTER TABLE bond_auction ENABLE ROW LEVEL SECURITY;

-- 全ユーザーに読み取り権限を付与
CREATE POLICY "Allow public read access"
    ON bond_auction
    FOR SELECT
    USING (true);

-- サービスロールのみに書き込み権限を付与
CREATE POLICY "Allow service role full access"
    ON bond_auction
    FOR ALL
    USING (auth.role() = 'service_role');

-- ============================================================
-- 完了メッセージ
-- ============================================================

DO $$
BEGIN
    RAISE NOTICE 'bond_auction テーブル作成完了';
    RAISE NOTICE '主キー: (bond_code, auction_date)';
    RAISE NOTICE 'インデックス: 5個作成';
END $$;
