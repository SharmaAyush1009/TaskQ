ALTER TABLE tasks 
ADD CONSTRAINT unique_task_idempotency 
UNIQUE (type, idempotency_key);

CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_tasks_type ON tasks(type);
CREATE INDEX idx_tasks_updated_at ON tasks(updated_at);
CREATE INDEX idx_tasks_idempotency ON tasks(type, idempotency_key);
CREATE INDEX idx_tasks_retry_at ON tasks(retry_at) WHERE retry_at IS NOT NULL;