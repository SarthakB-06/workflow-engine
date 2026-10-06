-- All 5 tables are created in Week 1, including columns that won't be
-- used until later phases (e.g., owner_id, lease_expiry, compensation_task_type).
-- Reason: ALTER TABLE on a live production table with millions of rows is
-- painful and risky. Creating the full schema now costs nothing.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── 1. workflow_definitions ──────────────────────────────────────────────────
-- Immutable blueprints. A "change" to a workflow means creating version N+1,
-- never editing an existing row — editing would corrupt in-flight executions.
CREATE TABLE workflow_definitions (
    id         UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    name       TEXT        NOT NULL,
    version    INT         NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (name, version)
);

-- ── 2. workflow_steps ────────────────────────────────────────────────────────
-- Each row is one step inside a definition. step_order is 1-indexed and must
-- be contiguous — the engine advances by step_order, so gaps = skipped steps.
--
-- retry_policy is JSONB (not columns) so we can tune the shape later without
-- a schema migration. compensation_task_type is NULL for non-Saga steps.
CREATE TABLE workflow_steps (
    id                     UUID  PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_id            UUID  NOT NULL REFERENCES workflow_definitions(id) ON DELETE CASCADE,
    step_name              TEXT  NOT NULL,
    step_order             INT   NOT NULL,
    task_type              TEXT  NOT NULL,
    compensation_task_type TEXT  NULL,       -- Week 8 (Saga)
    retry_policy           JSONB NOT NULL
                               DEFAULT '{"max_retries":3,"backoff_base_seconds":1,"backoff_max_seconds":60}',
    timeout_seconds        INT   NOT NULL DEFAULT 30
);

-- ── 3. workflow_executions ───────────────────────────────────────────────────
-- ⚠️  CACHED PROJECTION — not the source of truth.
-- status and current_step are derived from workflow_events and updated in the
-- same DB transaction as the event append. They exist purely as a read
-- optimisation so we don't replay the event log on every GET request.
--
-- owner_id / lease_expiry: unused until Phase 14-15 (distributed ownership).
CREATE TABLE workflow_executions (
    id                     UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_definition_id UUID        NOT NULL REFERENCES workflow_definitions(id),
    status                 TEXT        NOT NULL DEFAULT 'PENDING',
    current_step           INT         NOT NULL DEFAULT 0,
    input                  JSONB       NOT NULL DEFAULT '{}',
    owner_id               TEXT        NULL,           -- Phase 14-15
    lease_expiry           TIMESTAMPTZ NULL,           -- Phase 14-15
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── 4. task_executions ───────────────────────────────────────────────────────
-- One row per task attempt. idempotency_key = execution_id + ":" + step_id.
-- Workers check this key before executing anything. The queue delivers
-- at-least-once; idempotency makes the observable effect exactly-once.
CREATE TABLE task_executions (
    id                    UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_execution_id UUID        NOT NULL REFERENCES workflow_executions(id),
    step_id               UUID        NOT NULL REFERENCES workflow_steps(id),
    status                TEXT        NOT NULL DEFAULT 'PENDING',
    attempt               INT         NOT NULL DEFAULT 0,
    idempotency_key       TEXT        NOT NULL UNIQUE,
    next_retry_at         TIMESTAMPTZ NULL,     -- Week 6 (retry scheduler)
    worker_id             TEXT        NULL,     -- Week 7 (heartbeat)
    started_at            TIMESTAMPTZ NULL,
    completed_at          TIMESTAMPTZ NULL
);

-- ── 5. workflow_events ───────────────────────────────────────────────────────
-- THE source of truth. Append-only — rows are never updated or deleted.
-- sequence_number (not created_at) is the ordering key because:
--   • Two events in the same millisecond → same created_at, undefined order.
--   • Clock skew across machines → timestamps can go backward.
--   • sequence_number is monotonic by construction + enforced by the UNIQUE.
CREATE TABLE workflow_events (
    id                    UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_execution_id UUID        NOT NULL REFERENCES workflow_executions(id),
    sequence_number       INT         NOT NULL,
    event_type            TEXT        NOT NULL,
    payload               JSONB       NOT NULL DEFAULT '{}',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_execution_id, sequence_number)
);

-- ── Indexes ──────────────────────────────────────────────────────────────────
-- Only the query patterns the engine actually runs. Don't index everything —
-- indexes cost write throughput and disk space.

-- Crash recovery: find all RUNNING executions on startup.
CREATE INDEX idx_executions_status
    ON workflow_executions(status);

-- Task queries by execution (very frequent).
CREATE INDEX idx_tasks_execution_id
    ON task_executions(workflow_execution_id);

-- Retry scheduler: find tasks due for retry (Week 6).
CREATE INDEX idx_tasks_next_retry_at
    ON task_executions(next_retry_at)
    WHERE next_retry_at IS NOT NULL;

-- Task status queries (timeout sweeper in Week 6).
CREATE INDEX idx_tasks_status
    ON task_executions(status);

-- Event replay: fetch events in order for a given execution.
CREATE INDEX idx_events_execution_seq
    ON workflow_events(workflow_execution_id, sequence_number);
