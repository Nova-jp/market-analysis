CREATE TABLE IF NOT EXISTS imm_forward_matrix (
    trade_date     DATE          NOT NULL,
    start_imm_code VARCHAR(4)    NOT NULL,
    end_imm_code   VARCHAR(4)    NOT NULL,
    start_date     DATE          NOT NULL,
    end_date       DATE          NOT NULL,
    forward_rate   DECIMAL(10,6),
    tenor_months   SMALLINT      NOT NULL,
    UNIQUE (trade_date, start_imm_code, end_imm_code)
);

CREATE INDEX IF NOT EXISTS idx_imm_fwd_date ON imm_forward_matrix (trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_imm_fwd_pair ON imm_forward_matrix (start_imm_code, end_imm_code);
