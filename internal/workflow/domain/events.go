package domain



import (
	"errors"
	"time"
	"github.com/google/uuid"
)


type EventType string
const (
	// Workflow lifecycle
	EventWorkflowStarted   EventType = "WORKFLOW_STARTED"
	EventWorkflowCompleted EventType = "WORKFLOW_COMPLETED"
	EventWorkflowFailed    EventType = "WORKFLOW_FAILED"
	EventWorkflowCancelled EventType = "WORKFLOW_CANCELLED"
	// Task lifecycle
	EventTaskCreated    EventType = "TASK_CREATED"
	EventTaskDispatched EventType = "TASK_DISPATCHED"
	EventTaskStarted    EventType = "TASK_STARTED"
	EventTaskSucceeded  EventType = "TASK_SUCCEEDED"
	EventTaskFailed     EventType = "TASK_FAILED"
	EventTaskTimedOut   EventType = "TASK_TIMED_OUT"
	EventTaskRetrying   EventType = "TASK_RETRYING"
	// Saga compensation (Week 8 — defined now so event names are consistent)
	EventCompensationStarted EventType = "COMPENSATION_STARTED"
	EventStepCompensated     EventType = "STEP_COMPENSATED"
)


type Event struct {
	ID                  uuid.UUID      `json:"id"`
	WorkflowExecutionID uuid.UUID      `json:"workflow_execution_id"`
	SequenceNumber      int            `json:"sequence_number"`
	Type                EventType      `json:"event_type"`
	Payload             map[string]any `json:"payload"`
	CreatedAt           time.Time      `json:"created_at"`
}

func ApplyEvent(state ExecutionState, event Event) (ExecutionState, error) {
	// Defensive init: callers may pass a zero-value ExecutionState.
	if state.TaskStates == nil {
		state.TaskStates = make(map[uuid.UUID]TaskStatus)
	}
	switch event.Type {
	// ── Workflow lifecycle ──────────────────────────────────────────────────
	case EventWorkflowStarted:
		if !CanTransition(state.Status, WorkflowStatusRunning) {
			return state, errors.New("ApplyEvent: WORKFLOW_STARTED invalid from " + string(state.Status))
		}
		state.Status = WorkflowStatusRunning
	case EventWorkflowCompleted:
		if !CanTransition(state.Status, WorkflowStatusCompleted) {
			return state, errors.New("ApplyEvent: WORKFLOW_COMPLETED invalid from " + string(state.Status))
		}
		state.Status = WorkflowStatusCompleted
	case EventWorkflowFailed:
		if !CanTransition(state.Status, WorkflowStatusFailed) {
			return state, errors.New("ApplyEvent: WORKFLOW_FAILED invalid from " + string(state.Status))
		}
		state.Status = WorkflowStatusFailed
	case EventWorkflowCancelled:
		if !CanTransition(state.Status, WorkflowStatusCancelled) {
			return state, errors.New("ApplyEvent: WORKFLOW_CANCELLED invalid from " + string(state.Status))
		}
		state.Status = WorkflowStatusCancelled
	// ── Task lifecycle ──────────────────────────────────────────────────────
	case EventTaskCreated:
		stepID, err := uuidFromPayload(event.Payload, "step_id")
		if err != nil {
			return state, err
		}
		// TASK_CREATED bootstraps the step into the map at PENDING.
		// No CanTransitionTask check here — PENDING is the initial state,
		// not a transition from a previous state.
		state.TaskStates[stepID] = TaskStatusPending
	case EventTaskDispatched:
		stepID, err := uuidFromPayload(event.Payload, "step_id")
		if err != nil {
			return state, err
		}
		if !CanTransitionTask(state.TaskStates[stepID], TaskStatusDispatched) {
			return state, errors.New("ApplyEvent: TASK_DISPATCHED invalid from " + string(state.TaskStates[stepID]))
		}
		state.TaskStates[stepID] = TaskStatusDispatched
	case EventTaskStarted:
		stepID, err := uuidFromPayload(event.Payload, "step_id")
		if err != nil {
			return state, err
		}
		if !CanTransitionTask(state.TaskStates[stepID], TaskStatusRunning) {
			return state, errors.New("ApplyEvent: TASK_STARTED invalid from " + string(state.TaskStates[stepID]))
		}
		state.TaskStates[stepID] = TaskStatusRunning
	case EventTaskSucceeded:
		stepID, err := uuidFromPayload(event.Payload, "step_id")
		if err != nil {
			return state, err
		}
		if !CanTransitionTask(state.TaskStates[stepID], TaskStatusSuccess) {
			return state, errors.New("ApplyEvent: TASK_SUCCEEDED invalid from " + string(state.TaskStates[stepID]))
		}
		state.TaskStates[stepID] = TaskStatusSuccess
		// Advance the step pointer. The engine uses this to know
		// which step to enqueue next.
		if order, ok := event.Payload["step_order"].(float64); ok {
			state.CurrentStep = int(order)
		}
	case EventTaskFailed:
		stepID, err := uuidFromPayload(event.Payload, "step_id")
		if err != nil {
			return state, err
		}
		if !CanTransitionTask(state.TaskStates[stepID], TaskStatusFailed) {
			return state, errors.New("ApplyEvent: TASK_FAILED invalid from " + string(state.TaskStates[stepID]))
		}
		state.TaskStates[stepID] = TaskStatusFailed
	case EventTaskTimedOut:
		stepID, err := uuidFromPayload(event.Payload, "step_id")
		if err != nil {
			return state, err
		}
		if !CanTransitionTask(state.TaskStates[stepID], TaskStatusTimedOut) {
			return state, errors.New("ApplyEvent: TASK_TIMED_OUT invalid from " + string(state.TaskStates[stepID]))
		}
		state.TaskStates[stepID] = TaskStatusTimedOut
	case EventTaskRetrying:
		stepID, err := uuidFromPayload(event.Payload, "step_id")
		if err != nil {
			return state, err
		}
		if !CanTransitionTask(state.TaskStates[stepID], TaskStatusRetrying) {
			return state, errors.New("ApplyEvent: TASK_RETRYING invalid from " + string(state.TaskStates[stepID]))
		}
		state.TaskStates[stepID] = TaskStatusRetrying
	// ── Saga (Week 8) ───────────────────────────────────────────────────────
	case EventCompensationStarted, EventStepCompensated:
		// Intentional no-op for now. Events are still stored; Week 8 adds
		// compensation sub-state to ExecutionState and handles them here.
	default:
		return state, errors.New("ApplyEvent: unknown event type: " + string(event.Type))
	}
	return state, nil
}


func RebuildState(events []Event) (ExecutionState, error) {
	state := ExecutionState{
		Status:     WorkflowStatusPending, // every execution starts here
		TaskStates: make(map[uuid.UUID]TaskStatus),
	}
	if len(events) > 0 {
		state.ExecutionID = events[0].WorkflowExecutionID
	}
	for _, e := range events {
		var err error
		state, err = ApplyEvent(state, e)
		if err != nil {
			return state, err
		}
	}
	return state, nil
}

func uuidFromPayload(payload map[string]any, key string) (uuid.UUID, error) {
	raw, ok := payload[key]
	if !ok {
		return uuid.Nil, errors.New("event payload missing key: " + key)
	}
	s, ok := raw.(string)
	if !ok {
		return uuid.Nil, errors.New("event payload key " + key + " is not a string")
	}
	return uuid.Parse(s)
}