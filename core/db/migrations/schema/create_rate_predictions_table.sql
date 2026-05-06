
-- =====================================================
-- rate_predictions テーブル
-- GBDT等による金利予測結果を保持
-- =====================================================

CREATE TABLE IF NOT EXISTS rate_predictions (
    id BIGSERIAL PRIMARY KEY,
    prediction_date DATE NOT NULL,      -- 予測を実行した日（基準日）
    target_date DATE NOT NULL,          -- 予測対象の日
    product_type VARCHAR(20) NOT NULL,  -- OIS, TIBOR等
    tenor VARCHAR(20) NOT NULL,         -- 10Y等
    horizon_days INTEGER NOT NULL,      -- 予測期間（3日後=3, 5日後=5）
    predicted_rate DECIMAL(10, 5),      -- 予測された金利
    actual_rate DECIMAL(10, 5),         -- 実際の金利（後で埋める）
    model_name VARCHAR(50),             -- 使用したモデル名
    feature_importance JSONB,           -- 特徴量重要度（オプション）
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_prediction UNIQUE (prediction_date, product_type, tenor, horizon_days)
);

CREATE INDEX IF NOT EXISTS idx_rate_predictions_date ON rate_predictions(prediction_date DESC);
CREATE INDEX IF NOT EXISTS idx_rate_predictions_target ON rate_predictions(target_date);

COMMENT ON TABLE rate_predictions IS '金利予測結果（GBDT等）';
