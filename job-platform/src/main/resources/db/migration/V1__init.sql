CREATE TABLE jobs (
    id               UUID PRIMARY KEY,
    type             VARCHAR(100) NOT NULL,
    payload          JSONB        NOT NULL DEFAULT '{}'::jsonb,
    status           VARCHAR(20)  NOT NULL,
    attempts         INT          NOT NULL DEFAULT 0,
    max_attempts     INT          NOT NULL DEFAULT 5,
    idempotency_key  VARCHAR(200) UNIQUE,
    next_run_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    locked_by        VARCHAR(100),
    lease_expires_at TIMESTAMPTZ,
    last_error       TEXT,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_expired_lease ON jobs(lease_expires_at) WHERE status = 'RUNNING';

CREATE TABLE job_executions (
    id            BIGSERIAL PRIMARY KEY,
    job_id        UUID        NOT NULL REFERENCES jobs(id),
    attempt       INT         NOT NULL,
    worker_id     VARCHAR(100),
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    outcome       VARCHAR(30) NOT NULL,   -- RUNNING, SUCCEEDED, FAILED_RETRYABLE, FAILED_PERMANENT, FAILED_EXHAUSTED, LEASE_EXPIRED
    error_class   VARCHAR(200),
    error_message TEXT
);
CREATE INDEX idx_exec_job ON job_executions(job_id, attempt);

CREATE TABLE outbox_events (
    id           BIGSERIAL PRIMARY KEY,
    job_id       UUID         NOT NULL,
    exchange     VARCHAR(100) NOT NULL,
    routing_key  VARCHAR(100) NOT NULL,
    payload      TEXT         NOT NULL,
    available_at TIMESTAMPTZ  NOT NULL DEFAULT now(),   -- delayed retries live here
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox_events(available_at) WHERE published_at IS NULL;
