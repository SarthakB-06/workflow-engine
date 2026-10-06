package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)


type WorkflowStatus string

const (
	WorkflowStatusPending   WorkflowStatus = "PENDING"
	WorkflowStatusRunning   WorkflowStatus = "RUNNING"
	WorkflowStatusCompleted WorkflowStatus = "COMPLETED"
	WorkflowStatusFailed    WorkflowStatus = "FAILED"
	WorkflowStatusCancelled WorkflowStatus = "CANCELLED"
)


var validWorkflowTransitions = map[WorkflowStatus][]WorkflowStatus{
	WorkflowStatusPending:   {WorkflowStatusRunning, WorkflowStatusCancelled},
	WorkflowStatusRunning:   {WorkflowStatusCompleted, WorkflowStatusFailed, WorkflowStatusCancelled},
	WorkflowStatusCompleted: {},
	WorkflowStatusFailed:    {},
	WorkflowStatusCancelled: {},
}


func CanTransition(current, next WorkflowStatus) bool {
	allowed, ok := validWorkflowTransitions[current]
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


type RetryPolicy struct {
	MaxRetries         int `json:"max_retries"`
	BackoffBaseSeconds int `json:"backoff_base_seconds"`
	BackoffMaxSeconds  int `json:"backoff_max_seconds"`
}


var DefaultRetryPolicy = RetryPolicy{
	MaxRetries:         3,
	BackoffBaseSeconds: 1,
	BackoffMaxSeconds:  60,
}


type WorkflowStep struct {
	ID                   uuid.UUID   `json:"id"`
	WorkflowID           uuid.UUID   `json:"workflow_id"`
	StepName             string      `json:"step_name"`
	StepOrder            int         `json:"step_order"`
	TaskType             string      `json:"task_type"`
	CompensationTaskType *string     `json:"compensation_task_type,omitempty"`
	RetryPolicy          RetryPolicy `json:"retry_policy"`
	TimeoutSeconds       int         `json:"timeout_seconds"`
}


type WorkflowDefinition struct {
	ID        uuid.UUID      `json:"id"`
	Name      string         `json:"name"`
	Version   int            `json:"version"`
	Steps     []WorkflowStep `json:"steps"`
	CreatedAt time.Time      `json:"created_at"`
}


func (w *WorkflowDefinition) Validate() error {
	if w.Name == "" {
		return errors.New("workflow name is required")
	}
	if w.Version < 1 {
		return errors.New("version must be >= 1")
	}
	if len(w.Steps) == 0 {
		return errors.New("workflow must have at least one step")
	}
	seen := make(map[int]bool, len(w.Steps))
	for _, s := range w.Steps {
		if s.StepName == "" {
			return errors.New("every step must have a step_name")
		}
		if s.TaskType == "" {
			return errors.New("every step must have a task_type")
		}
		if s.TimeoutSeconds < 0 {
			return errors.New("timeout_seconds must be >= 0")
		}
		if seen[s.StepOrder] {
			return errors.New("duplicate step_order detected")
		}
		seen[s.StepOrder] = true
	}
	for i := 1; i <= len(w.Steps); i++ {
		if !seen[i] {
			return errors.New("step_order must be contiguous starting at 1")
		}
	}
	return nil
}