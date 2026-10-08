package service

// ExecutionService will handle workflow execution business logic.
//
// Week 2 implementation plan:
//   - StartExecution(ctx, workflowID, input) → creates a WorkflowExecution row,
//     calls ApplyEvent with WORKFLOW_STARTED, creates the first TaskExecution,
//     calls ApplyEvent with TASK_CREATED, enqueues the task to the queue.
//   - This is where the event-sourcing write path begins.
//
// Nothing here yet — keeping the file so the package structure is clear
// before we need it.
type ExecutionService struct {
	// Fields added in Week 2:
	//   repo  repository.ExecutionRepository
	//   queue queue.Queue
}
