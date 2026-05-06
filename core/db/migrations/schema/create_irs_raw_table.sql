-- =====================================================
-- IRS生データテーブル
-- IRS Raw Historical Data
-- =====================================================
-- データソース: ヒストリカルデータ（Excel）
-- 用途: TONA OISなどのヒストリカル金利データ保存
-- =====================================================

CREATE TABLE IF NOT EXISTS irs_raw (
    -- 主キー
    id BIGSERIAL PRIMARY KEY,

    -- 取引日
    trade_date DATE NOT NULL,

    -- 金利種別
    -- 例: 'TONA', 'LIBOR', 'TIBOR' など
    rate_type VARCHAR(50) NOT NULL,

    -- 期間 (Tenor)
    -- 例: '1D', '1W', '2W', '3W', '1M', '2M', '1Y', '10Y', '40Y' など
    tenor VARCHAR(10) NOT NULL,

    -- BIDレート（%）
    rate DECIMAL(10, 6),

    -- メタデータ
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- 一意制約: 同一日・金利種別・期間の重複を防ぐ
    CONSTRAINT unique_irs_raw_rate UNIQUE (trade_date, rate_type, tenor)
);

-- =====================================================
-- インデックス作成
-- =====================================================

-- 取引日でのクエリ最適化
CREATE INDEX IF NOT EXISTS idx_irs_raw_trade_date
ON irs_raw(trade_date DESC);

-- 金利種別でのクエリ最適化
CREATE INDEX IF NOT EXISTS idx_irs_raw_rate_type
ON irs_raw(rate_type);

-- 期間でのクエリ最適化
CREATE INDEX IF NOT EXISTS idx_irs_raw_tenor
ON irs_raw(tenor);

-- 複合インデックス: 日付と金利種別でのフィルタリング
CREATE INDEX IF NOT EXISTS idx_irs_raw_date_type
ON irs_raw(trade_date DESC, rate_type);

-- 複合インデックス: 金利種別と期間でのフィルタリング
CREATE INDEX IF NOT EXISTS idx_irs_raw_type_tenor
ON irs_raw(rate_type, tenor);

-- =====================================================
-- Row Level Security (RLS) ポリシー設定
-- =====================================================

-- RLSを有効化
ALTER TABLE irs_raw ENABLE ROW LEVEL SECURITY;

-- 読み取りポリシー: 誰でも読み取り可能
CREATE POLICY "irs_raw_read_policy"
ON irs_raw
FOR SELECT
USING (true);

-- 書き込みポリシー: 認証済みユーザーのみ
CREATE POLICY "irs_raw_write_policy"
ON irs_raw
FOR ALL
USING (auth.role() = 'authenticated');

-- =====================================================
-- コメント追加
-- =====================================================

COMMENT ON TABLE irs_raw IS
'IRS生データ（ヒストリカルデータ）';

COMMENT ON COLUMN irs_raw.trade_date IS
'取引日';

COMMENT ON COLUMN irs_raw.rate_type IS
'金利種別 (TONA, LIBOR, TIBORなど)';

COMMENT ON COLUMN irs_raw.tenor IS
'期間 (1D, 1W, 1M, 1Y, 10Yなど)';

COMMENT ON COLUMN irs_raw.rate IS
'BIDレート（%）';
