package domain

import (
	"time"
	"github.com/google/uuid"
)

type TaskStatus string
const (
	TaskStatusPending    TaskStatus = "PENDING"
	TaskStatusDispatched TaskStatus = "DISPATCHED"
	TaskStatusRunning    TaskStatus = "RUNNING"
	TaskStatusSuccess    TaskStatus = "SUCCESS"
	TaskStatusFailed     TaskStatus = "FAILED"
	TaskStatusRetrying   TaskStatus = "RETRYING"
	TaskStatusTimedOut   TaskStatus = "TIMED_OUT"
)


var validTaskTransitions = map[TaskStatus][]TaskStatus{
	TaskStatusPending:    {TaskStatusDispatched},
	TaskStatusDispatched: {TaskStatusRunning},
	TaskStatusRunning:    {TaskStatusSuccess, TaskStatusFailed, TaskStatusTimedOut},
	TaskStatusFailed:     {TaskStatusRetrying},
	TaskStatusTimedOut:   {TaskStatusRetrying},
	TaskStatusRetrying:   {TaskStatusDispatched},
	TaskStatusSuccess:    {},
}

func CanTransitionTask(current, next TaskStatus) bool {
	allowed, ok := validTaskTransitions[current]
	if !ok {
		return false
	}
	for _, s := range allowed {
		if s == next {
			return true
		}
	}
	return false
}

type TaskExecution struct {
	ID                  uuid.UUID  `json:"id"`
	WorkflowExecutionID uuid.UUID  `json:"workflow_execution_id"`
	StepID              uuid.UUID  `json:"step_id"`
	Status              TaskStatus `json:"status"`
	Attempt             int        `json:"attempt"`
	IdempotencyKey      string     `json:"idempotency_key"`
	NextRetryAt         *time.Time `json:"next_retry_at,omitempty"` // Week 6
	WorkerID            *string    `json:"worker_id,omitempty"`     // Week 7
	StartedAt           *time.Time `json:"started_at,omitempty"`
	CompletedAt         *time.Time `json:"completed_at,omitempty"`
}