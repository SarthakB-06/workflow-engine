package service

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sarthak/workflow-engine/internal/queue"
	"github.com/sarthak/workflow-engine/internal/workflow/domain"
	"github.com/sarthak/workflow-engine/internal/workflow/repository"
)


type ExecutionService struct {
	workflowRepo repository.WorkflowRepository  // to fetch the workflow definition + steps
	execRepo     repository.ExecutionRepository // to write executions, events, tasks
	q            queue.Queue                    // to dispatch tasks to workers
}

func NewExecutionService(
	workflowRepo repository.WorkflowRepository,
	execRepo repository.ExecutionRepository,
	q queue.Queue,
) *ExecutionService {
	return &ExecutionService{
		workflowRepo: workflowRepo,
		execRepo:     execRepo,
		q:            q,
	}
}

func (s *ExecutionService) StartExecution(
	ctx context.Context,
	workflowID uuid.UUID,
	input map[string]any,
) (*domain.WorkflowExecution, error) {

	wf, err := s.workflowRepo.GetWorkflowByID(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("get workflow definition: %w", err)
	}

	var firstStep *domain.WorkflowStep
	for i := range wf.Steps {
		if wf.Steps[i].StepOrder == 1 {
			firstStep = &wf.Steps[i]
			break
		}
	}
	if firstStep == nil {
		return nil, fmt.Errorf("workflow %s has no step with order=1", workflowID)
	}

	tx, err := s.execRepo.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // rolls back automatically if Commit is never called

	exec := &domain.WorkflowExecution{
		WorkflowDefinitionID: workflowID,
		Status:               domain.WorkflowStatusPending,
		CurrentStep:          0,
		Input:                input,
	}
	if err := s.execRepo.CreateExecution(ctx, tx, exec); err != nil {
		return nil, fmt.Errorf("create execution row: %w", err)
	}

	// The local state variable mirrors what RebuildState would produce.
	// Every ApplyEvent call advances it. At the end it must equal the
	// projection row we've written to Postgres — that's the invariant.
	state := domain.ExecutionState{
		ExecutionID: exec.ID,
		Status:      domain.WorkflowStatusPending,
		TaskStates:  make(map[uuid.UUID]domain.TaskStatus),
	}

	// emitEvent does: GetNextSeq → AppendEvent → ApplyEvent (in that order always)
	state, err = emitEvent(ctx, s.execRepo, tx, state, domain.Event{
		WorkflowExecutionID: exec.ID,
		Type:                domain.EventWorkflowStarted,
		Payload:             map[string]any{},
	})
	if err != nil {
		return nil, fmt.Errorf("emit WORKFLOW_STARTED: %w", err)
	}
	// Update the cached projection to match the state ApplyEvent returned.
	// Rule: only ever write what ApplyEvent produced — never decide the status yourself.
	if err := s.execRepo.UpdateExecutionProjection(
		ctx, tx, exec.ID, state.Status, state.CurrentStep,
	); err != nil {
		return nil, fmt.Errorf("update projection after WORKFLOW_STARTED: %w", err)
	}


	idempotencyKey := fmt.Sprintf("%s:%s", exec.ID.String(), firstStep.ID.String())
	task := &domain.TaskExecution{
		WorkflowExecutionID: exec.ID,
		StepID:              firstStep.ID,
		Status:              domain.TaskStatusPending,
		Attempt:             1,
		IdempotencyKey:      idempotencyKey,
	}
	if err := s.execRepo.CreateTaskExecution(ctx, tx, task); err != nil {
		return nil, fmt.Errorf("create task execution: %w", err)
	}

	state, err = emitEvent(ctx, s.execRepo, tx, state, domain.Event{
		WorkflowExecutionID: exec.ID,
		Type:                domain.EventTaskCreated,
		Payload: map[string]any{
			"step_id":    firstStep.ID.String(),
			"step_order": float64(firstStep.StepOrder),
			"task_type":  firstStep.TaskType,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("emit TASK_CREATED: %w", err)
	}

	// ── Step 8: emit TASK_DISPATCHED ──────────────────────────────────────────
	state, err = emitEvent(ctx, s.execRepo, tx, state, domain.Event{
		WorkflowExecutionID: exec.ID,
		Type:                domain.EventTaskDispatched,
		Payload: map[string]any{
			"step_id":    firstStep.ID.String(),
			"step_order": float64(firstStep.StepOrder),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("emit TASK_DISPATCHED: %w", err)
	}
	// Update the task row status to whatever ApplyEvent returned (DISPATCHED).
	if err := s.execRepo.UpdateTaskStatus(
		ctx, tx, task.ID, state.TaskStates[firstStep.ID],
	); err != nil {
		return nil, fmt.Errorf("update task to DISPATCHED: %w", err)
	}

	// ── Step 9: commit ────────────────────────────────────────────────────────
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit start execution: %w", err)
	}

	// Reflect final state on the struct we return to the caller.
	exec.Status = state.Status
	exec.CurrentStep = state.CurrentStep

	// ── Step 10: enqueue AFTER commit ────────────────────────────────────────
	if err := s.q.Enqueue(ctx, queue.Task{
		WorkflowExecutionID: exec.ID.String(),
		StepID:              firstStep.ID.String(),
		StepOrder:           firstStep.StepOrder,
		TaskType:            firstStep.TaskType,
		IdempotencyKey:      idempotencyKey,
		Payload:             input,
	}); err != nil {
		// The execution IS in the DB correctly. Do not return an error.
		// Week 5 crash recovery re-enqueues DISPATCHED tasks on startup.
		log.Printf("[WARN] enqueue failed after commit (will recover on restart): %v", err)
	}

	return exec, nil
}


func (s *ExecutionService) GetExecution(
	ctx context.Context,
	id uuid.UUID,
) (*domain.WorkflowExecution, error) {
	exec, err := s.execRepo.GetExecutionByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get execution: %w", err)
	}

	// Replay all events and compare with the cached row.
	events, err := s.execRepo.GetEventsByExecutionID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get events for rebuild check: %w", err)
	}
	rebuilt, err := domain.RebuildState(events)
	if err != nil {
		return nil, fmt.Errorf("rebuild state: %w", err)
	}

	// The invariant. If this fires, there is a bug in the write path.
	if rebuilt.Status != exec.Status {
		log.Printf("[BUG] projection mismatch execution=%s  cached=%s  rebuilt=%s",
			id, exec.Status, rebuilt.Status)
	}

	return exec, nil
}

func emitEvent(
	ctx context.Context,
	repo repository.ExecutionRepository,
	tx pgx.Tx,
	state domain.ExecutionState,
	event domain.Event,
) (domain.ExecutionState, error) {
	// Step 1: assign sequence number
	seq, err := repo.GetNextSequenceNumber(ctx, tx, event.WorkflowExecutionID)
	if err != nil {
		return state, fmt.Errorf("get next sequence number: %w", err)
	}
	event.SequenceNumber = seq

	// Step 2: persist the event
	if err := repo.AppendEvent(ctx, tx, &event); err != nil {
		return state, fmt.Errorf("append event %s: %w", event.Type, err)
	}

	// Step 3: validate transition + advance state
	newState, err := domain.ApplyEvent(state, event)
	if err != nil {
		// ApplyEvent returned an error = invalid state transition.
		// This is a programming bug, not a user error.
		return state, fmt.Errorf("apply event %s: %w", event.Type, err)
	}

	return newState, nil
}
