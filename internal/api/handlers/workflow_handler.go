package handlers




import (
	"encoding/json"
	"errors"
	"net/http"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sarthak/workflow-engine/internal/workflow/domain"
	"github.com/sarthak/workflow-engine/internal/workflow/repository"
	"github.com/sarthak/workflow-engine/internal/workflow/service"
)
// WorkflowHandler holds a reference to the WorkflowService.
// All workflow-related HTTP endpoints are methods on this struct.
type WorkflowHandler struct {
	svc *service.WorkflowService
}
func NewWorkflowHandler(svc *service.WorkflowService) *WorkflowHandler {
	return &WorkflowHandler{svc: svc}
}



type createWorkflowRequest struct {
	Name    string            `json:"name"`
	Version int               `json:"version"`
	Steps   []createStepInput `json:"steps"`
}
type createStepInput struct {
	StepName             string              `json:"step_name"`
	StepOrder            int                 `json:"step_order"`
	TaskType             string              `json:"task_type"`
	CompensationTaskType *string             `json:"compensation_task_type,omitempty"`
	RetryPolicy          *domain.RetryPolicy `json:"retry_policy,omitempty"`
	TimeoutSeconds       int                 `json:"timeout_seconds"`
}
// CreateWorkflow handles POST /api/v1/workflows
func (h *WorkflowHandler) CreateWorkflow(w http.ResponseWriter, r *http.Request) {
	
	var req createWorkflowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	// Map the request type → domain type.
	// The handler does this mapping; the service never sees raw HTTP types.
	wf := &domain.WorkflowDefinition{
		Name:    req.Name,
		Version: req.Version,
	}
	for _, s := range req.Steps {
		step := domain.WorkflowStep{
			StepName:             s.StepName,
			StepOrder:            s.StepOrder,
			TaskType:             s.TaskType,
			CompensationTaskType: s.CompensationTaskType,
			TimeoutSeconds:       s.TimeoutSeconds,
		}
		// Only set RetryPolicy if the caller provided one.
		// If nil, the repository will apply DefaultRetryPolicy.
		if s.RetryPolicy != nil {
			step.RetryPolicy = *s.RetryPolicy
		}
		wf.Steps = append(wf.Steps, step)
	}
	if err := h.svc.CreateWorkflow(r.Context(), wf); err != nil {
		// Map error types to HTTP status codes.
		if service.IsValidationError(err) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusCreated, wf)
}
// GetWorkflow handles GET /api/v1/workflows/{id}
func (h *WorkflowHandler) GetWorkflow(w http.ResponseWriter, r *http.Request) {
	// chi.URLParam extracts the {id} path parameter set up in routes.go.
	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid workflow id: must be a UUID")
		return
	}
	wf, err := h.svc.GetWorkflow(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			writeError(w, http.StatusNotFound, "workflow not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, wf)
}
// ListWorkflows handles GET /api/v1/workflows
func (h *WorkflowHandler) ListWorkflows(w http.ResponseWriter, r *http.Request) {
	wfs, err := h.svc.ListWorkflows(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	if wfs == nil {
		wfs = []*domain.WorkflowDefinition{}
	}
	writeJSON(w, http.StatusOK, wfs)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	
	json.NewEncoder(w).Encode(v)
}
// writeError writes a JSON error response: {"error": "message"}
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
