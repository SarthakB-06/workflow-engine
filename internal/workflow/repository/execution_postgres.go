package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sarthak/workflow-engine/internal/workflow/domain"
)


type ExecutionRepository interface {
	// BeginTx starts a database transaction.
	// The service calls this, then passes the tx to every write method below.
	BeginTx(ctx context.Context) (pgx.Tx, error)

	// ── Write methods (all transactional) ────────────────────────────────────

	CreateExecution(ctx context.Context, tx pgx.Tx, exec *domain.WorkflowExecution) error

	// AppendEvent inserts one event. The caller sets SequenceNumber.
	// The UNIQUE(execution_id, sequence_number) DB constraint is the safety net.
	AppendEvent(ctx context.Context, tx pgx.Tx, event *domain.Event) error

	// UpdateExecutionProjection updates the cached status + current_step.
	// Must always be called AFTER AppendEvent in the same transaction.
	// Must always reflect the output of ApplyEvent — never set fields directly.
	UpdateExecutionProjection(ctx context.Context, tx pgx.Tx, id uuid.UUID, status domain.WorkflowStatus, currentStep int) error

	CreateTaskExecution(ctx context.Context, tx pgx.Tx, task *domain.TaskExecution) error

	UpdateTaskStatus(ctx context.Context, tx pgx.Tx, taskID uuid.UUID, status domain.TaskStatus) error

	// ── Read methods (no transaction needed) ─────────────────────────────────


	GetExecutionByID(ctx context.Context, id uuid.UUID) (*domain.WorkflowExecution, error)


	GetTaskExecutionByIdempotencyKey(ctx context.Context, key string) (*domain.TaskExecution, error)

	GetTaskExecutionByID(ctx context.Context, id uuid.UUID) (*domain.TaskExecution, error)

	// GetEventsByExecutionID returns events ordered by sequence_number ASC.
	// This is the direct input to RebuildState — order is critical.
	GetEventsByExecutionID(ctx context.Context, executionID uuid.UUID) ([]domain.Event, error)

	// GetNextSequenceNumber returns MAX(sequence_number)+1 for an execution.
	// Must be called inside a transaction so it sees the tx's own inserts.
	GetNextSequenceNumber(ctx context.Context, tx pgx.Tx, executionID uuid.UUID) (int, error)

	// GetRunningExecutions returns all RUNNING executions.
	// Stub for now — implemented in Week 5 (crash recovery).
	GetRunningExecutions(ctx context.Context) ([]*domain.WorkflowExecution, error)
}

// ── Implementation ────────────────────────────────────────────────────────────

type postgresExecutionRepo struct {
	pool *pgxpool.Pool
}

func NewExecutionRepository(pool *pgxpool.Pool) ExecutionRepository {
	return &postgresExecutionRepo{pool: pool}
}

func (r *postgresExecutionRepo) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}

// CreateExecution inserts a new workflow_executions row (status = PENDING).
// Called at the very start of StartExecution, before any events are appended.
func (r *postgresExecutionRepo) CreateExecution(
	ctx context.Context,
	tx pgx.Tx,
	exec *domain.WorkflowExecution,
) error {
	exec.ID = uuid.New()
	now := time.Now().UTC()
	exec.CreatedAt = now
	exec.UpdatedAt = now

	// Input is map[string]any — must be JSON-encoded before storing as JSONB.
	inputJSON, err := json.Marshal(exec.Input)
	if err != nil {
		return fmt.Errorf("marshal execution input: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO workflow_executions
		   (id, workflow_definition_id, status, current_step, input, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		exec.ID,
		exec.WorkflowDefinitionID,
		string(exec.Status),
		exec.CurrentStep,
		inputJSON,
		exec.CreatedAt,
		exec.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert workflow_execution: %w", err)
	}
	return nil
}

// AppendEvent inserts one event row into workflow_events.
// The caller is responsible for setting event.SequenceNumber correctly.
// Assign a new UUID and timestamp here — callers don't need to set those.
func (r *postgresExecutionRepo) AppendEvent(
	ctx context.Context,
	tx pgx.Tx,
	event *domain.Event,
) error {
	event.ID = uuid.New()
	event.CreatedAt = time.Now().UTC()

	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO workflow_events
		   (id, workflow_execution_id, sequence_number, event_type, payload, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		event.ID,
		event.WorkflowExecutionID,
		event.SequenceNumber,
		string(event.Type),
		payloadJSON,
		event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert workflow_event (seq=%d): %w", event.SequenceNumber, err)
	}
	return nil
}

// UpdateExecutionProjection updates the cached status + current_step projection.
// This MUST always be called within the same transaction as AppendEvent, and
// MUST always use the status returned by ApplyEvent — not a status you decide
// independently. Violating either rule breaks the correctness guarantee.
func (r *postgresExecutionRepo) UpdateExecutionProjection(
	ctx context.Context,
	tx pgx.Tx,
	id uuid.UUID,
	status domain.WorkflowStatus,
	currentStep int,
) error {
	_, err := tx.Exec(ctx,
		`UPDATE workflow_executions
		 SET status = $1, current_step = $2, updated_at = $3
		 WHERE id = $4`,
		string(status), currentStep, time.Now().UTC(), id,
	)
	if err != nil {
		return fmt.Errorf("update execution projection: %w", err)
	}
	return nil
}

// CreateTaskExecution inserts a new task_executions row.
// The idempotency_key unique constraint prevents duplicate task rows
// for the same (execution, step) pair.
func (r *postgresExecutionRepo) CreateTaskExecution(
	ctx context.Context,
	tx pgx.Tx,
	task *domain.TaskExecution,
) error {
	task.ID = uuid.New()

	_, err := tx.Exec(ctx,
		`INSERT INTO task_executions
		   (id, workflow_execution_id, step_id, status, attempt, idempotency_key)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		task.ID,
		task.WorkflowExecutionID,
		task.StepID,
		string(task.Status),
		task.Attempt,
		task.IdempotencyKey,
	)
	if err != nil {
		return fmt.Errorf("insert task_execution: %w", err)
	}
	return nil
}

// UpdateTaskStatus updates a single task's status column.
func (r *postgresExecutionRepo) UpdateTaskStatus(
	ctx context.Context,
	tx pgx.Tx,
	taskID uuid.UUID,
	status domain.TaskStatus,
) error {
	_, err := tx.Exec(ctx,
		`UPDATE task_executions SET status = $1 WHERE id = $2`,
		string(status), taskID,
	)
	if err != nil {
		return fmt.Errorf("update task status: %w", err)
	}
	return nil
}

// GetExecutionByID fetches a single execution row.
// Returns ErrNotFound (same sentinel as the workflow repo) if no row exists.
func (r *postgresExecutionRepo) GetExecutionByID(
	ctx context.Context,
	id uuid.UUID,
) (*domain.WorkflowExecution, error) {
	exec := &domain.WorkflowExecution{}
	var statusStr string
	var inputJSON []byte

	err := r.pool.QueryRow(ctx,
		`SELECT id, workflow_definition_id, status, current_step, input,
		        created_at, updated_at
		 FROM workflow_executions
		 WHERE id = $1`,
		id,
	).Scan(
		&exec.ID,
		&exec.WorkflowDefinitionID,
		&statusStr,
		&exec.CurrentStep,
		&inputJSON,
		&exec.CreatedAt,
		&exec.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query workflow_execution: %w", err)
	}

	exec.Status = domain.WorkflowStatus(statusStr)
	if err := json.Unmarshal(inputJSON, &exec.Input); err != nil {
		return nil, fmt.Errorf("unmarshal execution input: %w", err)
	}
	return exec, nil
}

func (r *postgresExecutionRepo) GetTaskExecutionByIdempotencyKey(
	ctx context.Context,
	key string,
) (*domain.TaskExecution, error) {
	task := &domain.TaskExecution{}
	var statusStr string
	err := r.pool.QueryRow(ctx,
		`SELECT id, workflow_execution_id, step_id, status, attempt,
		        idempotency_key, next_retry_at, worker_id, started_at, completed_at
		 FROM task_executions
		 WHERE idempotency_key = $1`,
		key,
	).Scan(
		&task.ID,
		&task.WorkflowExecutionID,
		&task.StepID,
		&statusStr,
		&task.Attempt,
		&task.IdempotencyKey,
		&task.NextRetryAt,
		&task.WorkerID,
		&task.StartedAt,
		&task.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query task_execution by idempotency_key: %w", err)
	}
	task.Status = domain.TaskStatus(statusStr)
	return task, nil
}


func (r *postgresExecutionRepo) GetTaskExecutionByID(
	ctx context.Context,
	id uuid.UUID,
) (*domain.TaskExecution, error) {
	task := &domain.TaskExecution{}
	var statusStr string
	err := r.pool.QueryRow(ctx,
		`SELECT id, workflow_execution_id, step_id, status, attempt,
		        idempotency_key, next_retry_at, worker_id, started_at, completed_at
		 FROM task_executions
		 WHERE id = $1`,
		id,
	).Scan(
		&task.ID,
		&task.WorkflowExecutionID,
		&task.StepID,
		&statusStr,
		&task.Attempt,
		&task.IdempotencyKey,
		&task.NextRetryAt,
		&task.WorkerID,
		&task.StartedAt,
		&task.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query task_execution by id: %w", err)
	}
	task.Status = domain.TaskStatus(statusStr)
	return task, nil
}


// GetEventsByExecutionID fetches all events for one execution, in sequence order.
// ORDER BY sequence_number ASC — never use created_at for ordering events.
func (r *postgresExecutionRepo) GetEventsByExecutionID(
	ctx context.Context,
	executionID uuid.UUID,
) ([]domain.Event, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, workflow_execution_id, sequence_number, event_type, payload, created_at
		 FROM workflow_events
		 WHERE workflow_execution_id = $1
		 ORDER BY sequence_number ASC`,
		executionID,
	)
	if err != nil {
		return nil, fmt.Errorf("query workflow_events: %w", err)
	}
	defer rows.Close()

	var events []domain.Event
	for rows.Next() {
		var e domain.Event
		var eventTypeStr string
		var payloadJSON []byte

		if err := rows.Scan(
			&e.ID,
			&e.WorkflowExecutionID,
			&e.SequenceNumber,
			&eventTypeStr,
			&payloadJSON,
			&e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workflow_event row: %w", err)
		}

		e.Type = domain.EventType(eventTypeStr)
		if err := json.Unmarshal(payloadJSON, &e.Payload); err != nil {
			return nil, fmt.Errorf("unmarshal event payload: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// GetNextSequenceNumber returns the next available sequence number.
//
// How it works: MAX(sequence_number) + 1 inside a transaction.
// PostgreSQL transactions see their own uncommitted inserts, so if this
// transaction has already inserted seq=1 and seq=2, MAX() returns 2 and
// we get back 3 — exactly what we want.
//
// The UNIQUE(execution_id, sequence_number) constraint acts as a final
// guard: if two concurrent transactions somehow race and pick the same
// number, one of them will fail at INSERT time with a unique violation.
func (r *postgresExecutionRepo) GetNextSequenceNumber(
	ctx context.Context,
	tx pgx.Tx,
	executionID uuid.UUID,
) (int, error) {
	var next int
	err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence_number), 0) + 1
		 FROM workflow_events
		 WHERE workflow_execution_id = $1`,
		executionID,
	).Scan(&next)
	if err != nil {
		return 0, fmt.Errorf("get next sequence number: %w", err)
	}
	return next, nil
}

// GetRunningExecutions returns all executions with status = RUNNING.
// Stub — Week 5 fills in the query and adds the recovery logic.
func (r *postgresExecutionRepo) GetRunningExecutions(
	ctx context.Context,
) ([]*domain.WorkflowExecution, error) {
	return nil, nil
}
