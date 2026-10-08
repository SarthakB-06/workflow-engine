package api


import (
	"net/http"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/sarthak/workflow-engine/internal/api/handlers"
	"github.com/sarthak/workflow-engine/internal/workflow/service"
)
func NewRouter(workflowSvc *service.WorkflowService) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	// ── Routes ───────────────────────────────────────────────────────────────
	wfHandler := handlers.NewWorkflowHandler(workflowSvc)
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"status":"ok"}`))
		})
		r.Route("/workflows", func(r chi.Router) {
			r.Get("/", wfHandler.ListWorkflows)          // GET  /api/v1/workflows
			r.Post("/", wfHandler.CreateWorkflow)        // POST /api/v1/workflows
			r.Get("/{id}", wfHandler.GetWorkflow)        // GET  /api/v1/workflows/{id}
		})
		// Execution endpoints — registered here as stubs, implemented in Week 2.
		// r.Route("/workflows/{id}/executions", func(r chi.Router) {
		//     r.Post("/", executionHandler.StartExecution)
		//     r.Get("/{execID}", executionHandler.GetExecution)
		// })
	})
	return r
}
