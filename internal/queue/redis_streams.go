package queue

// RedisStreamsQueue will replace MemoryQueue in Week 7.
//
// Why Redis Streams instead of a plain Redis list?
// Streams support consumer groups: multiple worker instances can each receive
// a task exactly once (rather than racing on a list POP). They also support
// explicit acknowledgement — if a worker crashes before ACKing, the message
// is re-delivered to another worker. This is what makes Week 7's dead-worker
// reassignment work correctly alongside the idempotency key from Week 6.
//
// Stub kept here so the package structure and the design intent are visible
// before the implementation is needed.
type RedisStreamsQueue struct {
	// redisClient *redis.Client  — added in Week 7
	// streamKey  string
	// groupName  string
}
