package handlers

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sarthak/workflow-engine/internal/workflow/repository"
	"github.com/sarthak/workflow-engine/internal/workflow/service"
)

// ExecutionHandler wires execution-related HTTP endpoints to ExecutionService.
type ExecutionHandler struct {
	svc *service.ExecutionService
}

func NewExecutionHandler(svc *service.ExecutionService) *ExecutionHandler {
	return &ExecutionHandler{svc: svc}
}

// startExecutionRequest is the JSON body for POST /api/v1/workflows/{id}/executions.
// input is the data passed into the workflow — accessible to every task handler.
type startExecutionRequest struct {
	Input map[string]any `json:"input"`
}


func (h *ExecutionHandler) StartExecution(w http.ResponseWriter, r *http.Request) {
	// Parse the workflow definition ID from the URL path.
	workflowIDStr := chi.URLParam(r, "id")
	workflowID, err := uuid.Parse(workflowIDStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid workflow id: must be a UUID")
		return
	}

	// Decode request body. Input is optional — default to empty map if body is empty.
	var req startExecutionRequest
	req.Input = map[string]any{} // default

	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
	}

	exec, err := h.svc.StartExecution(r.Context(), workflowID, req.Input)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			writeError(w, http.StatusNotFound, "workflow not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	// 201 Created — the execution is now RUNNING and step 1 is DISPATCHED.
	writeJSON(w, http.StatusCreated, exec)
}

// GetExecution handles GET /api/v1/executions/{execID}
func (h *ExecutionHandler) GetExecution(w http.ResponseWriter, r *http.Request) {
	execIDStr := chi.URLParam(r, "execID")
	execID, err := uuid.Parse(execIDStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid execution id: must be a UUID")
		return
	}

	exec, err := h.svc.GetExecution(r.Context(), execID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			writeError(w, http.StatusNotFound, "execution not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusOK, exec)
}
