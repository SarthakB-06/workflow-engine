package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/sarthak/workflow-engine/internal/workflow/domain"
	"github.com/sarthak/workflow-engine/internal/workflow/repository"
)

type WorkflowService struct {
	repo repository.WorkflowRepository
}

func NewWorkflowService(repo repository.WorkflowRepository) *WorkflowService {
	return &WorkflowService{repo: repo}
}

func (s *WorkflowService) CreateWorkflow(
	ctx context.Context,
	wf *domain.WorkflowDefinition,
) error {
	// Step 1: structural validation (contiguous step_order, non-empty fields, etc.)
	if err := wf.Validate(); err != nil {
		// Wrap as a validation error so the handler can map it to HTTP 400.
		return fmt.Errorf("validation: %w", err)
	}
	// Step 2: persist — the repository fills in IDs and applies DB-level defaults.
	if err := s.repo.CreateWorkflow(ctx, wf); err != nil {
		return fmt.Errorf("create workflow: %w", err)
	}
	return nil
}

// GetWorkflow retrieves a workflow definition by its UUID primary key.
func (s *WorkflowService) GetWorkflow(
	ctx context.Context,
	id uuid.UUID,
) (*domain.WorkflowDefinition, error) {
	wf, err := s.repo.GetWorkflowByID(ctx, id)
	if err != nil {

		return nil, fmt.Errorf("get workflow: %w", err)
	}
	return wf, nil
}

// ListWorkflows returns all workflow definitions (without their steps).
func (s *WorkflowService) ListWorkflows(
	ctx context.Context,
) ([]*domain.WorkflowDefinition, error) {
	wfs, err := s.repo.ListWorkflows(ctx)
	if err != nil {
		return nil, fmt.Errorf("list workflows: %w", err)
	}
	return wfs, nil
}

func IsValidationError(err error) bool {
	if err == nil {
		return false
	}
	// Unwrap the chain and check each message.
	for e := err; e != nil; e = errors.Unwrap(e) {
		if len(e.Error()) >= 11 && e.Error()[:11] == "validation:" {
			return true
		}
	}
	return false
}
