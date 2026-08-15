CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    account_id TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    merchant_id TEXT NOT NULL,
    merchant_country TEXT NOT NULL,
    raw_event JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS transactions_account_time_idx
    ON transactions (account_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS assessments (
    transaction_id TEXT PRIMARY KEY REFERENCES transactions(transaction_id),
    event_id TEXT NOT NULL UNIQUE,
    assessed_at TIMESTAMPTZ NOT NULL,
    model_version TEXT NOT NULL,
    risk_score DOUBLE PRECISION NOT NULL CHECK (risk_score >= 0 AND risk_score <= 1),
    recommended_action TEXT NOT NULL,
    signals JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS assessments_action_idx
    ON assessments (recommended_action);

-- Labels deliberately have no transaction foreign key: real labels may arrive first.
CREATE TABLE IF NOT EXISTS fraud_labels (
    transaction_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    labelled_at TIMESTAMPTZ NOT NULL,
    is_fraud BOOLEAN NOT NULL,
    fraud_type TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    event_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (id) WHERE published_at IS NULL;
