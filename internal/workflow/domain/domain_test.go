package domain_test


import (
	"testing"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sarthak/workflow-engine/internal/workflow/domain"
)



func TestCanTransition(t *testing.T) {

	tests := []struct {
		name    string
		current domain.WorkflowStatus
		next    domain.WorkflowStatus
		want    bool
	}{
		// Valid transitions
		{"pending→running",   domain.WorkflowStatusPending,   domain.WorkflowStatusRunning,   true},
		{"pending→cancelled", domain.WorkflowStatusPending,   domain.WorkflowStatusCancelled, true},
		{"running→completed", domain.WorkflowStatusRunning,   domain.WorkflowStatusCompleted, true},
		{"running→failed",    domain.WorkflowStatusRunning,   domain.WorkflowStatusFailed,    true},
		{"running→cancelled", domain.WorkflowStatusRunning,   domain.WorkflowStatusCancelled, true},
		// Invalid transitions — terminal states have no outgoing edges
		{"completed→running",  domain.WorkflowStatusCompleted, domain.WorkflowStatusRunning,  false},
		{"failed→running",     domain.WorkflowStatusFailed,    domain.WorkflowStatusRunning,  false},
		{"cancelled→running",  domain.WorkflowStatusCancelled, domain.WorkflowStatusRunning,  false},
		// Invalid transitions — can't skip states
		{"pending→completed",  domain.WorkflowStatusPending,   domain.WorkflowStatusCompleted, false},
		{"pending→failed",     domain.WorkflowStatusPending,   domain.WorkflowStatusFailed,    false},
		// Self-transitions are never valid
		{"running→running",    domain.WorkflowStatusRunning,   domain.WorkflowStatusRunning,   false},
		{"pending→pending",    domain.WorkflowStatusPending,   domain.WorkflowStatusPending,   false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := domain.CanTransition(tt.current, tt.next)
			assert.Equal(t, tt.want, got)
		})
	}
}



func TestCanTransitionTask(t *testing.T) {
	tests := []struct {
		name    string
		current domain.TaskStatus
		next    domain.TaskStatus
		want    bool
	}{
		// Valid transitions
		{"pending→dispatched",   domain.TaskStatusPending,    domain.TaskStatusDispatched, true},
		{"dispatched→running",   domain.TaskStatusDispatched, domain.TaskStatusRunning,    true},
		{"running→success",      domain.TaskStatusRunning,    domain.TaskStatusSuccess,    true},
		{"running→failed",       domain.TaskStatusRunning,    domain.TaskStatusFailed,     true},
		{"running→timedout",     domain.TaskStatusRunning,    domain.TaskStatusTimedOut,   true},
		{"failed→retrying",      domain.TaskStatusFailed,     domain.TaskStatusRetrying,   true},
		{"timedout→retrying",    domain.TaskStatusTimedOut,   domain.TaskStatusRetrying,   true},
		{"retrying→dispatched",  domain.TaskStatusRetrying,   domain.TaskStatusDispatched, true},
		// Terminal state — SUCCESS has no outgoing edges
		{"success→dispatched",   domain.TaskStatusSuccess,    domain.TaskStatusDispatched, false},
		{"success→running",      domain.TaskStatusSuccess,    domain.TaskStatusRunning,    false},
		// Can't skip states
		{"pending→running",      domain.TaskStatusPending,    domain.TaskStatusRunning,    false},
		{"pending→success",      domain.TaskStatusPending,    domain.TaskStatusSuccess,    false},
		{"dispatched→success",   domain.TaskStatusDispatched, domain.TaskStatusSuccess,    false},
		// Self-transitions never valid
		{"running→running",      domain.TaskStatusRunning,    domain.TaskStatusRunning,    false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := domain.CanTransitionTask(tt.current, tt.next)
			assert.Equal(t, tt.want, got)
		})
	}
}


func TestValidate(t *testing.T) {
	validStep := func(order int) domain.WorkflowStep {
		return domain.WorkflowStep{
			StepName:       "step",
			StepOrder:      order,
			TaskType:       "DO_SOMETHING",
			TimeoutSeconds: 30,
		}
	}
	tests := []struct {
		name    string
		wf      domain.WorkflowDefinition
		wantErr bool
	}{
		{
			name: "valid single step",
			wf: domain.WorkflowDefinition{
				Name: "my-workflow", Version: 1,
				Steps: []domain.WorkflowStep{validStep(1)},
			},
			wantErr: false,
		},
		{
			name: "valid multi step",
			wf: domain.WorkflowDefinition{
				Name: "my-workflow", Version: 1,
				Steps: []domain.WorkflowStep{validStep(1), validStep(2), validStep(3)},
			},
			wantErr: false,
		},
		{
			name:    "missing name",
			wf:      domain.WorkflowDefinition{Version: 1, Steps: []domain.WorkflowStep{validStep(1)}},
			wantErr: true,
		},
		{
			name:    "version zero",
			wf:      domain.WorkflowDefinition{Name: "x", Version: 0, Steps: []domain.WorkflowStep{validStep(1)}},
			wantErr: true,
		},
		{
			name:    "no steps",
			wf:      domain.WorkflowDefinition{Name: "x", Version: 1},
			wantErr: true,
		},
		{
			name: "duplicate step_order",
			wf: domain.WorkflowDefinition{
				Name: "x", Version: 1,
				Steps: []domain.WorkflowStep{validStep(1), validStep(1)},
			},
			wantErr: true,
		},
		{
			name: "non-contiguous step_order (gap)",
			wf: domain.WorkflowDefinition{
				Name: "x", Version: 1,
				Steps: []domain.WorkflowStep{validStep(1), validStep(3)},
			},
			wantErr: true,
		},
		{
			name: "step_order not starting at 1",
			wf: domain.WorkflowDefinition{
				Name: "x", Version: 1,
				Steps: []domain.WorkflowStep{validStep(2), validStep(3)},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.wf.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}


func newState() domain.ExecutionState {
	return domain.ExecutionState{
		ExecutionID: uuid.New(),
		Status:      domain.WorkflowStatusPending,
		TaskStates:  make(map[uuid.UUID]domain.TaskStatus),
	}
}


func newEvent(execID uuid.UUID, eventType domain.EventType, payload map[string]any) domain.Event {
	return domain.Event{
		ID:                  uuid.New(),
		WorkflowExecutionID: execID,
		SequenceNumber:      1,
		Type:                eventType,
		Payload:             payload,
	}
}



func TestApplyEvent_WorkflowLifecycle(t *testing.T) {
	t.Run("WORKFLOW_STARTED advances PENDING to RUNNING", func(t *testing.T) {
		state := newState() 
		event := newEvent(state.ExecutionID, domain.EventWorkflowStarted, map[string]any{})
		got, err := domain.ApplyEvent(state, event)
		require.NoError(t, err)
		assert.Equal(t, domain.WorkflowStatusRunning, got.Status)
	})
	t.Run("WORKFLOW_COMPLETED advances RUNNING to COMPLETED", func(t *testing.T) {
		state := newState()
		state.Status = domain.WorkflowStatusRunning
		got, err := domain.ApplyEvent(state,
			newEvent(state.ExecutionID, domain.EventWorkflowCompleted, map[string]any{}))
		require.NoError(t, err)
		assert.Equal(t, domain.WorkflowStatusCompleted, got.Status)
	})
	t.Run("WORKFLOW_FAILED advances RUNNING to FAILED", func(t *testing.T) {
		state := newState()
		state.Status = domain.WorkflowStatusRunning
		got, err := domain.ApplyEvent(state,
			newEvent(state.ExecutionID, domain.EventWorkflowFailed, map[string]any{}))
		require.NoError(t, err)
		assert.Equal(t, domain.WorkflowStatusFailed, got.Status)
	})
	t.Run("WORKFLOW_CANCELLED from PENDING", func(t *testing.T) {
		state := newState()
		got, err := domain.ApplyEvent(state,
			newEvent(state.ExecutionID, domain.EventWorkflowCancelled, map[string]any{}))
		require.NoError(t, err)
		assert.Equal(t, domain.WorkflowStatusCancelled, got.Status)
	})
	t.Run("WORKFLOW_STARTED from RUNNING returns error (invalid transition)", func(t *testing.T) {
		state := newState()
		state.Status = domain.WorkflowStatusRunning // already RUNNING
		_, err := domain.ApplyEvent(state,
			newEvent(state.ExecutionID, domain.EventWorkflowStarted, map[string]any{}))
		require.Error(t, err)
	})
	t.Run("WORKFLOW_COMPLETED from PENDING returns error", func(t *testing.T) {
		state := newState() // PENDING, not RUNNING
		_, err := domain.ApplyEvent(state,
			newEvent(state.ExecutionID, domain.EventWorkflowCompleted, map[string]any{}))
		require.Error(t, err)
	})
}
func TestApplyEvent_TaskLifecycle(t *testing.T) {
	stepID := uuid.New()
	payload := map[string]any{
		"step_id":    stepID.String(),
		"step_order": float64(1),
	}
	t.Run("TASK_CREATED registers step as PENDING", func(t *testing.T) {
		state := newState()
		state.Status = domain.WorkflowStatusRunning
		got, err := domain.ApplyEvent(state,
			newEvent(state.ExecutionID, domain.EventTaskCreated, payload))
		require.NoError(t, err)
		assert.Equal(t, domain.TaskStatusPending, got.TaskStates[stepID])
	})
	t.Run("TASK_DISPATCHED advances PENDING to DISPATCHED", func(t *testing.T) {
		state := newState()
		state.Status = domain.WorkflowStatusRunning
		state.TaskStates[stepID] = domain.TaskStatusPending
		got, err := domain.ApplyEvent(state,
			newEvent(state.ExecutionID, domain.EventTaskDispatched, payload))
		require.NoError(t, err)
		assert.Equal(t, domain.TaskStatusDispatched, got.TaskStates[stepID])
	})
	t.Run("TASK_STARTED advances DISPATCHED to RUNNING", func(t *testing.T) {
		state := newState()
		state.Status = domain.WorkflowStatusRunning
		state.TaskStates[stepID] = domain.TaskStatusDispatched
		got, err := domain.ApplyEvent(state,
			newEvent(state.ExecutionID, domain.EventTaskStarted, payload))
		require.NoError(t, err)
		assert.Equal(t, domain.TaskStatusRunning, got.TaskStates[stepID])
	})
	t.Run("TASK_SUCCEEDED advances RUNNING to SUCCESS and updates CurrentStep", func(t *testing.T) {
		state := newState()
		state.Status = domain.WorkflowStatusRunning
		state.TaskStates[stepID] = domain.TaskStatusRunning
		got, err := domain.ApplyEvent(state,
			newEvent(state.ExecutionID, domain.EventTaskSucceeded, payload))
		require.NoError(t, err)
		assert.Equal(t, domain.TaskStatusSuccess, got.TaskStates[stepID])
		assert.Equal(t, 1, got.CurrentStep) // step_order=1 was set in payload
	})
	t.Run("TASK_FAILED advances RUNNING to FAILED", func(t *testing.T) {
		state := newState()
		state.Status = domain.WorkflowStatusRunning
		state.TaskStates[stepID] = domain.TaskStatusRunning
		got, err := domain.ApplyEvent(state,
			newEvent(state.ExecutionID, domain.EventTaskFailed, payload))
		require.NoError(t, err)
		assert.Equal(t, domain.TaskStatusFailed, got.TaskStates[stepID])
	})
	t.Run("TASK_RETRYING advances FAILED to RETRYING", func(t *testing.T) {
		state := newState()
		state.Status = domain.WorkflowStatusRunning
		state.TaskStates[stepID] = domain.TaskStatusFailed
		got, err := domain.ApplyEvent(state,
			newEvent(state.ExecutionID, domain.EventTaskRetrying, payload))
		require.NoError(t, err)
		assert.Equal(t, domain.TaskStatusRetrying, got.TaskStates[stepID])
	})
	t.Run("TASK_DISPATCHED from RUNNING returns error (invalid)", func(t *testing.T) {
		state := newState()
		state.Status = domain.WorkflowStatusRunning
		state.TaskStates[stepID] = domain.TaskStatusRunning // already RUNNING
		_, err := domain.ApplyEvent(state,
			newEvent(state.ExecutionID, domain.EventTaskDispatched, payload))
		require.Error(t, err)
	})
	t.Run("TASK event with missing step_id returns error", func(t *testing.T) {
		state := newState()
		state.Status = domain.WorkflowStatusRunning
		_, err := domain.ApplyEvent(state, domain.Event{
			ID:                  uuid.New(),
			WorkflowExecutionID: state.ExecutionID,
			Type:                domain.EventTaskCreated,
			Payload:             map[string]any{}, // missing step_id
		})
		require.Error(t, err)
	})
	t.Run("unknown event type returns error", func(t *testing.T) {
		state := newState()
		_, err := domain.ApplyEvent(state, domain.Event{
			ID:                  uuid.New(),
			WorkflowExecutionID: state.ExecutionID,
			Type:                domain.EventType("UNKNOWN_EVENT"),
			Payload:             map[string]any{},
		})
		require.Error(t, err)
	})
	t.Run("nil TaskStates is defensively initialised", func(t *testing.T) {
		// ApplyEvent should not panic if TaskStates is nil (zero-value ExecutionState).
		state := domain.ExecutionState{
			ExecutionID: uuid.New(),
			Status:      domain.WorkflowStatusRunning,
			TaskStates:  nil, // intentionally nil
		}
		_, err := domain.ApplyEvent(state,
			newEvent(state.ExecutionID, domain.EventTaskCreated, map[string]any{
				"step_id":    uuid.New().String(),
				"step_order": float64(1),
			}))
		require.NoError(t, err) // should not panic
	})
}




func TestRebuildState(t *testing.T) {
	t.Run("empty event log returns initial PENDING state", func(t *testing.T) {
		state, err := domain.RebuildState([]domain.Event{})
		require.NoError(t, err)
		assert.Equal(t, domain.WorkflowStatusPending, state.Status)
		assert.Equal(t, 0, state.CurrentStep)
	})
	t.Run("full happy-path sequence produces correct final state", func(t *testing.T) {
		execID := uuid.New()
		stepID := uuid.New()
		stepPayload := map[string]any{
			"step_id":    stepID.String(),
			"step_order": float64(1),
		}
		events := []domain.Event{
			{ID: uuid.New(), WorkflowExecutionID: execID, SequenceNumber: 1, Type: domain.EventWorkflowStarted, Payload: map[string]any{}},
			{ID: uuid.New(), WorkflowExecutionID: execID, SequenceNumber: 2, Type: domain.EventTaskCreated, Payload: stepPayload},
			{ID: uuid.New(), WorkflowExecutionID: execID, SequenceNumber: 3, Type: domain.EventTaskDispatched, Payload: stepPayload},
			{ID: uuid.New(), WorkflowExecutionID: execID, SequenceNumber: 4, Type: domain.EventTaskStarted, Payload: stepPayload},
			{ID: uuid.New(), WorkflowExecutionID: execID, SequenceNumber: 5, Type: domain.EventTaskSucceeded, Payload: stepPayload},
			{ID: uuid.New(), WorkflowExecutionID: execID, SequenceNumber: 6, Type: domain.EventWorkflowCompleted, Payload: map[string]any{}},
		}
		state, err := domain.RebuildState(events)
		require.NoError(t, err)
		assert.Equal(t, execID, state.ExecutionID)
		assert.Equal(t, domain.WorkflowStatusCompleted, state.Status)
		assert.Equal(t, 1, state.CurrentStep)
		assert.Equal(t, domain.TaskStatusSuccess, state.TaskStates[stepID])
	})
	t.Run("invalid transition mid-sequence propagates error", func(t *testing.T) {
		execID := uuid.New()
		// WORKFLOW_COMPLETED before WORKFLOW_STARTED — invalid
		events := []domain.Event{
			{ID: uuid.New(), WorkflowExecutionID: execID, SequenceNumber: 1,
				Type: domain.EventWorkflowCompleted, Payload: map[string]any{}},
		}
		_, err := domain.RebuildState(events)
		require.Error(t, err)
	})
}