package worker

import (
	"context"
	"log"

	"github.com/sarthak/workflow-engine/internal/queue"
)


type Worker struct {
	q        queue.Queue
	registry *Registry
}

// NewWorker creates a worker bound to the given queue and handler registry.
func NewWorker(q queue.Queue, registry *Registry) *Worker {
	return &Worker{
		q:        q,
		registry: registry,
	}
}


func (w *Worker) Run(ctx context.Context) {
	log.Println("[WORKER] started, waiting for tasks...")

	taskCh := w.q.Dequeue(ctx)

	for {
		select {
		case <-ctx.Done():
			// Context cancelled (server shutting down) — exit the loop cleanly.
			log.Println("[WORKER] shutting down")
			return

		case task, ok := <-taskCh:
			// Go learning note — receiving from a channel:
			// The two-value receive `task, ok := <-taskCh` gives you:
			//   task = the value received
			//   ok   = false if the channel was closed (no more values coming)
			// A closed channel always returns the zero value with ok=false.
			if !ok {
				log.Println("[WORKER] queue channel closed, shutting down")
				return
			}

			w.processTask(ctx, task)
		}
	}
}

// processTask looks up and calls the handler for a single task.
// Errors from the handler are logged — in Week 4 they will also emit
// a TASK_FAILED event and trigger the retry/compensation logic.
func (w *Worker) processTask(ctx context.Context, task queue.Task) {
	log.Printf("[WORKER] received task  type=%s  execution=%s  step=%d",
		task.TaskType, task.WorkflowExecutionID, task.StepOrder)

	handler, ok := w.registry.Get(task.TaskType)
	if !ok {
		// Unknown task type — this is a programming error (forgot to register).
		// In Week 6, this will emit a TASK_FAILED event.
		log.Printf("[WORKER] ERROR no handler registered for task_type=%s", task.TaskType)
		return
	}

	if err := handler(ctx, task); err != nil {
		// Handler returned an error — task failed.
		// In Week 4, this will emit TASK_FAILED + trigger retry logic.
		log.Printf("[WORKER] ERROR handler failed  type=%s  error=%v", task.TaskType, err)
		return
	}

	// Handler succeeded.
	// In Week 4, this will emit TASK_SUCCEEDED + advance to the next step.
	log.Printf("[WORKER] ✓ task completed  type=%s  execution=%s",
		task.TaskType, task.WorkflowExecutionID)
}
