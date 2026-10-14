package worker

import (
	"context"
	"fmt"

	"github.com/sarthak/workflow-engine/internal/queue"
)

// HandlerFunc is the type signature every task handler must implement.
// It receives the task (with payload) and returns an error if it failed.
//

type HandlerFunc func(ctx context.Context, task queue.Task) error

type Registry struct {
	handlers map[string]HandlerFunc
}

// NewRegistry creates an empty handler registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]HandlerFunc),
	}
}

// Register adds a handler for the given task type.
//
func (r *Registry) Register(taskType string, handler HandlerFunc) {
	r.handlers[taskType] = handler
}

// Get retrieves the handler for a task type.
func (r *Registry) Get(taskType string) (HandlerFunc, bool) {
	h, ok := r.handlers[taskType]
	return h, ok
}


func StubHandler(ctx context.Context, task queue.Task) error {
	fmt.Printf("[WORKER] ✓ executed task_type=%s  execution=%s  step=%d  idempotency_key=%s\n",
		task.TaskType,
		task.WorkflowExecutionID,
		task.StepOrder,
		task.IdempotencyKey,
	)
	return nil 
}
