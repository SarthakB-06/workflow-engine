package queue

import "context"


type Task struct {
	WorkflowExecutionID string
	StepID              string
	StepOrder           int
	TaskType            string         // e.g. "CHARGE_PAYMENT" — used to look up the handler
	IdempotencyKey      string         // execution_id + ":" + step_id
	Payload             map[string]any // workflow input passed through to the handler
}

// Queue is the abstraction over the transport layer.
//
// Week 1–6: backed by MemoryQueue (Go channel, single process)
// Week 7+:  backed by RedisStreamsQueue (persistent, multi-worker)
//
// The rest of the codebase depends only on this interface — swapping the
// backing implementation in Week 7 requires zero changes to callers.
//

type Queue interface {
	// Enqueue adds a task to the queue.
	// Returns an error if the queue is full or the context is cancelled.
	Enqueue(ctx context.Context, task Task) error

	// Dequeue returns a read-only channel that delivers tasks.
	// The worker reads from this channel in a loop.
	Dequeue(ctx context.Context) <-chan Task
}
