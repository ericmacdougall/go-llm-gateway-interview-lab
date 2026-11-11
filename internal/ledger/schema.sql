CREATE TABLE IF NOT EXISTS gateway_budgets (
    client_id text PRIMARY KEY,
    limit_amount bigint NOT NULL CHECK (limit_amount >= 0),
    spent bigint NOT NULL DEFAULT 0 CHECK (spent >= 0),
    held bigint NOT NULL DEFAULT 0 CHECK (held >= 0)
);

CREATE TABLE IF NOT EXISTS gateway_attempts (
    id text PRIMARY KEY,
    client_id text NOT NULL REFERENCES gateway_budgets(client_id),
    request_id text NOT NULL,
    attempt jsonb NOT NULL,
    estimate bigint NOT NULL CHECK (estimate >= 0),
    state text NOT NULL CHECK (state IN ('reserved', 'dispatching', 'uncertain', 'settled', 'released')),
    actual bigint CHECK (actual >= 0),
    provider_request_id text NOT NULL DEFAULT '',
    evidence_ref text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    next_check_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((state = 'settled') = (actual IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS gateway_attempts_due ON gateway_attempts (next_check_at, id)
    WHERE state = 'uncertain';
CREATE INDEX IF NOT EXISTS gateway_attempts_stale ON gateway_attempts (updated_at, id)
    WHERE state IN ('reserved', 'dispatching');

CREATE TABLE IF NOT EXISTS gateway_audit_head (
    singleton boolean PRIMARY KEY CHECK (singleton),
    key_public bytea NOT NULL CHECK (octet_length(key_public) = 32),
    seq bigint NOT NULL CHECK (seq >= 0),
    digest bytea NOT NULL CHECK (octet_length(digest) = 32)
);

CREATE TABLE IF NOT EXISTS gateway_audit (
    seq bigint PRIMARY KEY,
    kind text NOT NULL,
    attempt_id text NOT NULL,
    payload bytea NOT NULL,
    prev_digest bytea NOT NULL CHECK (octet_length(prev_digest) = 32),
    digest bytea NOT NULL CHECK (octet_length(digest) = 32),
    signature bytea NOT NULL CHECK (octet_length(signature) = 64)
);

CREATE UNIQUE INDEX IF NOT EXISTS gateway_attempt_one_settlement
    ON gateway_audit (attempt_id)
    WHERE kind IN ('settled', 'manual_resolution');
CREATE TABLE IF NOT EXISTS gateway_approvals (
    client_id text NOT NULL REFERENCES gateway_budgets(client_id),
    id text NOT NULL CHECK (length(id) BETWEEN 1 AND 128),
    workload text NOT NULL CHECK (length(workload) > 0),
    model text NOT NULL CHECK (length(model) BETWEEN 1 AND 256),
    requester jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('pending', 'approved', 'denied', 'revoked')),
    ttl_seconds bigint NOT NULL CHECK (ttl_seconds > 0),
    expires_at timestamptz NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    decided_by text NOT NULL DEFAULT '',
    reason text NOT NULL DEFAULT '',
    PRIMARY KEY (client_id, id)
);

CREATE INDEX IF NOT EXISTS gateway_approvals_active
    ON gateway_approvals (client_id, workload, model, expires_at)
    WHERE state = 'approved';
