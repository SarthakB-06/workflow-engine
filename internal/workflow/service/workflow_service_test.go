package service_test

import (
	"context"
	"errors"
	"testing"
	"time"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sarthak/workflow-engine/internal/workflow/domain"
	"github.com/sarthak/workflow-engine/internal/workflow/repository"
	"github.com/sarthak/workflow-engine/internal/workflow/service"
)



type mockWorkflowRepo struct {
	createWorkflowFn func(ctx context.Context, wf *domain.WorkflowDefinition) error
	getWorkflowByIDFn func(ctx context.Context, id uuid.UUID) (*domain.WorkflowDefinition, error)
	listWorkflowsFn   func(ctx context.Context) ([]*domain.WorkflowDefinition, error)
}


func (m *mockWorkflowRepo) CreateWorkflow(ctx context.Context, wf *domain.WorkflowDefinition) error {
	if m.createWorkflowFn != nil {
		return m.createWorkflowFn(ctx, wf)
	}
	return nil
}

func (m *mockWorkflowRepo) GetWorkflowByID(ctx context.Context, id uuid.UUID) (*domain.WorkflowDefinition, error) {
	if m.getWorkflowByIDFn != nil {
		return m.getWorkflowByIDFn(ctx, id)
	}
	return nil, repository.ErrNotFound
}

func (m *mockWorkflowRepo) ListWorkflows(ctx context.Context) ([]*domain.WorkflowDefinition, error) {
	if m.listWorkflowsFn != nil {
		return m.listWorkflowsFn(ctx)
	}
	return nil, nil
}



func validWorkflowDefinition() *domain.WorkflowDefinition {
	wfID := uuid.New()
	return &domain.WorkflowDefinition{
		ID:        wfID,
		Name:      "order-fulfillment",
		Version:   1,
		CreatedAt: time.Now().UTC(),
		Steps: []domain.WorkflowStep{
			{
				ID:             uuid.New(),
				WorkflowID:     wfID,
				StepName:       "reserve-inventory",
				StepOrder:      1,
				TaskType:       "inventory.reserve",
				RetryPolicy:    domain.DefaultRetryPolicy,
				TimeoutSeconds: 30,
			},
			{
				ID:             uuid.New(),
				WorkflowID:     wfID,
				StepName:       "charge-payment",
				StepOrder:      2,
				TaskType:       "payment.charge",
				RetryPolicy:    domain.DefaultRetryPolicy,
				TimeoutSeconds: 30,
			},
		},
	}
}




func TestCreateWorkflow(t *testing.T) {
	ctx := context.Background()
	t.Run("success — valid workflow passes validation and saves to repo", func(t *testing.T) {
		repoCalled := false
		repo := &mockWorkflowRepo{
			createWorkflowFn: func(ctx context.Context, wf *domain.WorkflowDefinition) error {
				repoCalled = true
				assert.Equal(t, "order-fulfillment", wf.Name)
				return nil
			},
		}
		svc := service.NewWorkflowService(repo)
		wf := validWorkflowDefinition()
		err := svc.CreateWorkflow(ctx, wf)
		require.NoError(t, err)
		assert.True(t, repoCalled, "repository CreateWorkflow must be called on valid input")
	})
	t.Run("short-circuit on validation failure — repo is never called", func(t *testing.T) {
		repoCalled := false
		repo := &mockWorkflowRepo{
			createWorkflowFn: func(ctx context.Context, wf *domain.WorkflowDefinition) error {
				repoCalled = true
				return nil
			},
		}
		svc := service.NewWorkflowService(repo)
		invalidWF := &domain.WorkflowDefinition{
			Name:    "",
			Version: 1,
			Steps:   nil,
		}
		err := svc.CreateWorkflow(ctx, invalidWF)
		require.Error(t, err)
		// Must be recognized as a validation error so HTTP handler maps to 400 Bad Request
		assert.True(t, service.IsValidationError(err), "expected error to be identified as validation error")
		assert.Contains(t, err.Error(), "validation:")
		assert.False(t, repoCalled, "repository must not be called when validation fails")
	})
	t.Run("repository error — wrapped properly and not flagged as validation error", func(t *testing.T) {
		repoErr := errors.New("unique constraint violation or db down")
		repo := &mockWorkflowRepo{
			createWorkflowFn: func(ctx context.Context, wf *domain.WorkflowDefinition) error {
				return repoErr
			},
		}
		svc := service.NewWorkflowService(repo)
		wf := validWorkflowDefinition()
		err := svc.CreateWorkflow(ctx, wf)
		require.Error(t, err)
		// Must NOT be marked as validation error (HTTP layer should map this to 500)
		assert.False(t, service.IsValidationError(err))
		assert.Contains(t, err.Error(), "create workflow:")
		assert.ErrorIs(t, err, repoErr)
	})
}


func TestGetWorkflow(t *testing.T) {
	ctx := context.Background()
	wfID := uuid.New()
	t.Run("success — workflow found", func(t *testing.T) {
		expectedWF := validWorkflowDefinition()
		expectedWF.ID = wfID
		repo := &mockWorkflowRepo{
			getWorkflowByIDFn: func(ctx context.Context, id uuid.UUID) (*domain.WorkflowDefinition, error) {
				assert.Equal(t, wfID, id)
				return expectedWF, nil
			},
		}
		svc := service.NewWorkflowService(repo)
		result, err := svc.GetWorkflow(ctx, wfID)
		require.NoError(t, err)
		assert.Equal(t, expectedWF, result)
	})
	t.Run("not found — returns wrapped ErrNotFound", func(t *testing.T) {
		repo := &mockWorkflowRepo{
			getWorkflowByIDFn: func(ctx context.Context, id uuid.UUID) (*domain.WorkflowDefinition, error) {
				return nil, repository.ErrNotFound
			},
		}
		svc := service.NewWorkflowService(repo)
		result, err := svc.GetWorkflow(ctx, wfID)
		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, repository.ErrNotFound)
		assert.Contains(t, err.Error(), "get workflow:")
	})
}


func TestListWorkflows(t *testing.T) {
	ctx := context.Background()
	t.Run("success — returns all workflow definitions", func(t *testing.T) {
		expectedList := []*domain.WorkflowDefinition{
			validWorkflowDefinition(),
			validWorkflowDefinition(),
		}
		repo := &mockWorkflowRepo{
			listWorkflowsFn: func(ctx context.Context) ([]*domain.WorkflowDefinition, error) {
				return expectedList, nil
			},
		}
		svc := service.NewWorkflowService(repo)
		results, err := svc.ListWorkflows(ctx)
		require.NoError(t, err)
		assert.Len(t, results, 2)
		assert.Equal(t, expectedList, results)
	})
	t.Run("repo error — propagates error", func(t *testing.T) {
		dbErr := errors.New("database connection refused")
		repo := &mockWorkflowRepo{
			listWorkflowsFn: func(ctx context.Context) ([]*domain.WorkflowDefinition, error) {
				return nil, dbErr
			},
		}
		svc := service.NewWorkflowService(repo)
		results, err := svc.ListWorkflows(ctx)
		require.Error(t, err)
		assert.Nil(t, results)
		assert.ErrorIs(t, err, dbErr)
		assert.Contains(t, err.Error(), "list workflows:")
	})
}




func TestIsValidationError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error returns false",
			err:  nil,
			want: false,
		},
		{
			name: "direct validation error returns true",
			err:  errors.New("validation: name required"),
			want: true,
		},
		{
			name: "wrapped validation error returns true",
			err:  errors.Join(errors.New("validation: step order not contiguous")),
			want: true,
		},
		{
			name: "unrelated error returns false",
			err:  errors.New("database connection failed"),
			want: false,
		},
		{
			name: "error containing validation in middle returns false",
			err:  errors.New("failed during validation check"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := service.IsValidationError(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}
