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


var ErrNotFound = errors.New("not found")


type WorkflowRepository interface {
	CreateWorkflow(ctx context.Context, wf *domain.WorkflowDefinition) error
	GetWorkflowByID(ctx context.Context, id uuid.UUID) (*domain.WorkflowDefinition, error)
	ListWorkflows(ctx context.Context) ([]*domain.WorkflowDefinition, error)
}


type postgresWorkflowRepo struct {
	pool *pgxpool.Pool
}


func NewWorkflowRepository(pool *pgxpool.Pool) WorkflowRepository {
	return &postgresWorkflowRepo{pool: pool}
}



func (r *postgresWorkflowRepo) CreateWorkflow(
	ctx context.Context,
	wf *domain.WorkflowDefinition,
) error {

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if Commit() was already called


	wf.ID = uuid.New()

	wf.CreatedAt = time.Now().UTC()
	_, err = tx.Exec(ctx,
		`INSERT INTO workflow_definitions (id, name, version, created_at)
		 VALUES ($1, $2, $3, $4)`,
		wf.ID, wf.Name, wf.Version, wf.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert workflow_definition: %w", err)
	}
	
	for i := range wf.Steps {
		step := &wf.Steps[i] // pointer so we're editing the original, not a copy
		step.ID = uuid.New()
		step.WorkflowID = wf.ID
		// Apply defaults for any fields the caller left as zero-values.
		if step.TimeoutSeconds == 0 {
			step.TimeoutSeconds = 30
		}
		if step.RetryPolicy == (domain.RetryPolicy{}) {
			step.RetryPolicy = domain.DefaultRetryPolicy
		}

		policyJSON, err := json.Marshal(step.RetryPolicy)
		if err != nil {
			return fmt.Errorf("marshal retry_policy for step %d: %w", step.StepOrder, err)
		}
		_, err = tx.Exec(ctx,
			`INSERT INTO workflow_steps
			   (id, workflow_id, step_name, step_order, task_type,
			    compensation_task_type, retry_policy, timeout_seconds)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			step.ID,
			step.WorkflowID,
			step.StepName,
			step.StepOrder,
			step.TaskType,
			step.CompensationTaskType, // nil is fine — pgx maps it to SQL NULL
			policyJSON,
			step.TimeoutSeconds,
		)
		if err != nil {
			return fmt.Errorf("insert workflow_step order=%d: %w", step.StepOrder, err)
		}
	}
	// Commit the transaction. If this succeeds, all rows are durably written.
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}


func (r *postgresWorkflowRepo) GetWorkflowByID(
	ctx context.Context,
	id uuid.UUID,
) (*domain.WorkflowDefinition, error) {
	wf := &domain.WorkflowDefinition{}
	err := r.pool.QueryRow(ctx,
		`SELECT id, name, version, created_at
		 FROM workflow_definitions
		 WHERE id = $1`,
		id,
	).Scan(&wf.ID, &wf.Name, &wf.Version, &wf.CreatedAt)
	if err != nil {
		// pgx returns pgx.ErrNoRows when no row matched.
		// We translate it to our own ErrNotFound so the caller
		// doesn't need to import pgx just to check for "not found".
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query workflow_definition: %w", err)
	}
	// Fetch the steps in a separate query (ordered by step_order).
	steps, err := r.getStepsByWorkflowID(ctx, id)
	if err != nil {
		return nil, err
	}
	wf.Steps = steps
	return wf, nil
}



func (r *postgresWorkflowRepo) ListWorkflows(
	ctx context.Context,
) ([]*domain.WorkflowDefinition, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, name, version, created_at
		 FROM workflow_definitions
		 ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("list workflow_definitions: %w", err)
	}
	defer rows.Close()
	var wfs []*domain.WorkflowDefinition
	for rows.Next() {
		wf := &domain.WorkflowDefinition{}
		if err := rows.Scan(&wf.ID, &wf.Name, &wf.Version, &wf.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan workflow_definition row: %w", err)
		}
		wfs = append(wfs, wf)
	}
	// rows.Err() returns any error that happened during iteration.
	// Always check this — an error mid-iteration doesn't stop the loop,
	// it just sets this field.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow_definitions: %w", err)
	}
	return wfs, nil
}



func (r *postgresWorkflowRepo) getStepsByWorkflowID(
	ctx context.Context,
	workflowID uuid.UUID,
) ([]domain.WorkflowStep, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, workflow_id, step_name, step_order, task_type,
		        compensation_task_type, retry_policy, timeout_seconds
		 FROM workflow_steps
		 WHERE workflow_id = $1
		 ORDER BY step_order ASC`,
		workflowID,
	)
	if err != nil {
		return nil, fmt.Errorf("query workflow_steps: %w", err)
	}
	defer rows.Close()
	var steps []domain.WorkflowStep
	for rows.Next() {
		var s domain.WorkflowStep
		var policyJSON []byte // raw bytes from the JSONB column
		if err := rows.Scan(
			&s.ID,
			&s.WorkflowID,
			&s.StepName,
			&s.StepOrder,
			&s.TaskType,
			&s.CompensationTaskType, // scans as nil if SQL NULL
			&policyJSON,
			&s.TimeoutSeconds,
		); err != nil {
			return nil, fmt.Errorf("scan workflow_step row: %w", err)
		}
		// Unmarshal the JSONB bytes back into our RetryPolicy struct.
		if err := json.Unmarshal(policyJSON, &s.RetryPolicy); err != nil {
			return nil, fmt.Errorf("unmarshal retry_policy: %w", err)
		}
		steps = append(steps, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow_steps: %w", err)
	}
	return steps, nil
}
