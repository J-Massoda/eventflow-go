CREATE TABLE IF NOT EXISTS events (
    id text PRIMARY KEY,
    source text NOT NULL,
    event_type text NOT NULL,
    occurred_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    idempotency_key text NOT NULL,
    digest text NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'processing', 'delivered', 'dead')),
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    lease_until timestamptz,
    lease_token text,
    created_at timestamptz NOT NULL DEFAULT now(),
    delivered_at timestamptz,
    last_error text NOT NULL DEFAULT '',
    UNIQUE (source, idempotency_key)
);
CREATE INDEX IF NOT EXISTS events_due_idx ON events (next_attempt_at, created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS events_leased_idx ON events (lease_until) WHERE status = 'processing';
CREATE TABLE IF NOT EXISTS delivery_attempts (
    event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    attempt_number integer NOT NULL,
    attempted_at timestamptz NOT NULL,
    http_status integer NOT NULL,
    outcome text NOT NULL,
    error_message text NOT NULL,
    PRIMARY KEY (event_id, attempt_number)
);
