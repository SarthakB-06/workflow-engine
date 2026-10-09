package queue

import (
	"context"
	"fmt"
)

// MemoryQueue is a buffered Go channel wrapped as a Queue.
//
// Why a buffered channel?
// A buffered channel of size N can hold N items without blocking the sender.
// If the channel is full, Enqueue returns an error instead of blocking —
// this prevents the execution service from stalling if workers fall behind.
//
// Limitation: tasks in the channel are lost on process restart.
// That's acceptable in Weeks 1–6 because crash recovery (Week 5) will
// re-enqueue any DISPATCHED tasks found in the DB on startup.
// Week 7 replaces this with Redis Streams, which persists the queue.
type MemoryQueue struct {
	ch chan Task
}

// NewMemoryQueue creates a MemoryQueue with the given buffer capacity.
// A capacity of 100 is a reasonable default for local development.
func NewMemoryQueue(capacity int) *MemoryQueue {
	return &MemoryQueue{
		ch: make(chan Task, capacity),
	}
}

// Enqueue adds a task to the channel without blocking.
//
// Go learning note — select statement:
// `select` is like a switch for channels. It picks whichever case is ready.
// - If ctx is cancelled, we return immediately with the context error.
// - If the channel has space, we send the task immediately.
// - `default` runs if neither case is ready (channel full, ctx not cancelled).
//   This makes Enqueue non-blocking — it fails fast instead of waiting.
func (q *MemoryQueue) Enqueue(ctx context.Context, task Task) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case q.ch <- task:
		return nil
	default:
		return fmt.Errorf("queue is full (capacity: %d) — worker may be falling behind", cap(q.ch))
	}
}

// Dequeue returns the underlying channel directly.
// The worker reads from it in a for-range or select loop.
//
// Go learning note — returning a channel:
// We return `<-chan Task` (receive-only channel), not `chan Task` (bidirectional).
// This prevents the caller from accidentally sending INTO the channel —
// only Enqueue is allowed to send. Receive-only channels are a compile-time
// safety constraint.
func (q *MemoryQueue) Dequeue(ctx context.Context) <-chan Task {
	return q.ch
}
