package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/sarthak/workflow-engine/internal/api/handlers"
	"github.com/sarthak/workflow-engine/internal/workflow/service"
)

// NewRouter now takes both services.
// The signature changes here — main.go will be updated in step 6.
func NewRouter(
	workflowSvc  *service.WorkflowService,
	executionSvc *service.ExecutionService,
) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	wfHandler   := handlers.NewWorkflowHandler(workflowSvc)
	execHandler := handlers.NewExecutionHandler(executionSvc)

	r.Route("/api/v1", func(r chi.Router) {

		r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"status":"ok"}`))
		})

		// ── Workflow definitions ──────────────────────────────────────────
		r.Route("/workflows", func(r chi.Router) {
			r.Get("/",     wfHandler.ListWorkflows)   // GET  /api/v1/workflows
			r.Post("/",    wfHandler.CreateWorkflow)   // POST /api/v1/workflows
			r.Get("/{id}", wfHandler.GetWorkflow)      // GET  /api/v1/workflows/{id}

			// ── Executions (nested under workflow) ────────────────────────
			r.Post("/{id}/executions", execHandler.StartExecution) // POST /api/v1/workflows/{id}/executions
		})

		// ── Executions (top-level read) ───────────────────────────────────
		r.Route("/executions", func(r chi.Router) {
			r.Get("/{execID}", execHandler.GetExecution) // GET /api/v1/executions/{execID}
		})
	})

	return r
}
