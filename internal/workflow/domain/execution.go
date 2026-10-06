package domain

import (
	"time"


	"github.com/google/uuid"

)


type WorkflowExecution struct {
	ID                   uuid.UUID      `json:"id"`
	WorkflowDefinitionID uuid.UUID      `json:"workflow_definition_id"`
	Status               WorkflowStatus `json:"status"`
	CurrentStep          int            `json:"current_step"`
	Input                map[string]any `json:"input"`
	OwnerID              *string        `json:"owner_id,omitempty"`    // Phase 14-15
	LeaseExpiry          *time.Time     `json:"lease_expiry,omitempty"` // Phase 14-15
	CreatedAt            time.Time      `json:"created_at"`
	UpdatedAt            time.Time      `json:"updated_at"`
}

type ExecutionState struct {
	ExecutionID uuid.UUID
	Status      WorkflowStatus
	CurrentStep int
	TaskStates  map[uuid.UUID]TaskStatus // step_id → current task status
}


