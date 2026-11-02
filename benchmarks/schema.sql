-- Disposable benchmark fixture inferred from this repository's queries.
-- This is not the unavailable schema used for the original performance claims.
CREATE TABLE tasks (
  id BIGSERIAL PRIMARY KEY,
  type TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'PENDING',
  idempotency_key TEXT NOT NULL,
  result JSONB,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  last_error TEXT,
  retry_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE email_logs (
  task_id BIGINT PRIMARY KEY REFERENCES tasks(id),
  recipient TEXT NOT NULL,
  subject TEXT,
  sent_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE payment_logs (
  task_id BIGINT PRIMARY KEY REFERENCES tasks(id),
  transaction_id TEXT NOT NULL UNIQUE,
  amount NUMERIC NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE operation_logs (
  task_id BIGINT NOT NULL REFERENCES tasks(id),
  operation_type TEXT NOT NULL,
  operation_key TEXT NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (task_id, operation_type)
);
