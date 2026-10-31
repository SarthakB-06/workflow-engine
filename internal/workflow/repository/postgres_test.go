package repository_test


import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sarthak/workflow-engine/internal/workflow/domain"
	"github.com/sarthak/workflow-engine/internal/workflow/repository"
)


func setupTestDB(t *testing.T) (*pgxpool.Pool, repository.WorkflowRepository) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping database integration test in -short mode")
	}
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://workflow:workflow@localhost:5432/workflow_engine?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping integration test: failed to configure pgx pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("skipping integration test: cannot connect to postgres at %s: %v", dbURL, err)
	}
	// t.Cleanup runs automatically after the test finishes (even if it fails)
	t.Cleanup(func() {
		pool.Close()
	})
	repo := repository.NewWorkflowRepository(pool)
	return pool, repo
}


func cleanupWorkflow(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := pool.Exec(ctx, `DELETE FROM workflow_definitions WHERE id = $1`, id)
	if err != nil {
		t.Logf("warning: failed to cleanup workflow %s: %v", id, err)
	}
}



func TestPostgresRepository_CreateWorkflow_And_GetByID(t *testing.T) {
	pool, repo := setupTestDB(t)
	ctx := context.Background()
	uniqueName := fmt.Sprintf("order-flow-%s", uuid.New().String()[:8])
	wf := &domain.WorkflowDefinition{
		Name:    uniqueName,
		Version: 1,
		Steps: []domain.WorkflowStep{
			{
				StepName:       "reserve-inventory",
				StepOrder:      1,
				TaskType:       "inventory.reserve",
				RetryPolicy:    domain.DefaultRetryPolicy,
				TimeoutSeconds: 45,
			},
			{
				StepName:       "charge-payment",
				StepOrder:      2,
				TaskType:       "payment.charge",
				RetryPolicy:    domain.DefaultRetryPolicy,
				TimeoutSeconds: 30,
			},
		},
	}
	// 1. Create Workflow
	err := repo.CreateWorkflow(ctx, wf)
	require.NoError(t, err)
	// Ensure cleanup after test completion
	t.Cleanup(func() {
		cleanupWorkflow(t, pool, wf.ID)
	})
	// Verify IDs were assigned by repository
	assert.NotEqual(t, uuid.Nil, wf.ID, "workflow ID must be assigned")
	assert.False(t, wf.CreatedAt.IsZero(), "created_at must be populated")
	require.Len(t, wf.Steps, 2)
	assert.NotEqual(t, uuid.Nil, wf.Steps[0].ID)
	assert.Equal(t, wf.ID, wf.Steps[0].WorkflowID)
	assert.NotEqual(t, uuid.Nil, wf.Steps[1].ID)
	assert.Equal(t, wf.ID, wf.Steps[1].WorkflowID)
	// 2. Fetch Workflow by ID
	fetched, err := repo.GetWorkflowByID(ctx, wf.ID)
	require.NoError(t, err)
	require.NotNil(t, fetched)
	assert.Equal(t, wf.ID, fetched.ID)
	assert.Equal(t, wf.Name, fetched.Name)
	assert.Equal(t, wf.Version, fetched.Version)
	assert.WithinDuration(t, wf.CreatedAt, fetched.CreatedAt, time.Second)
	// 3. Verify Steps are retrieved in correct order
	require.Len(t, fetched.Steps, 2)
	assert.Equal(t, "reserve-inventory", fetched.Steps[0].StepName)
	assert.Equal(t, 1, fetched.Steps[0].StepOrder)
	assert.Equal(t, "inventory.reserve", fetched.Steps[0].TaskType)
	assert.Equal(t, 45, fetched.Steps[0].TimeoutSeconds)
	assert.Equal(t, domain.DefaultRetryPolicy, fetched.Steps[0].RetryPolicy)
	assert.Equal(t, "charge-payment", fetched.Steps[1].StepName)
	assert.Equal(t, 2, fetched.Steps[1].StepOrder)
	assert.Equal(t, "payment.charge", fetched.Steps[1].TaskType)
	assert.Equal(t, 30, fetched.Steps[1].TimeoutSeconds)
}



func TestPostgresRepository_GetWorkflowByID_NotFound(t *testing.T) {
	_, repo := setupTestDB(t)
	ctx := context.Background()
	randomID := uuid.New()
	wf, err := repo.GetWorkflowByID(ctx, randomID)
	require.Error(t, err)
	assert.Nil(t, wf)
	assert.True(t, errors.Is(err, repository.ErrNotFound), "expected ErrNotFound when record does not exist")
}


func TestPostgresRepository_ListWorkflows(t *testing.T) {
	pool, repo := setupTestDB(t)
	ctx := context.Background()
	id1 := uuid.New()
	id2 := uuid.New()
	name1 := fmt.Sprintf("list-test-a-%s", id1.String()[:8])
	name2 := fmt.Sprintf("list-test-b-%s", id2.String()[:8])
	wf1 := &domain.WorkflowDefinition{
		Name:    name1,
		Version: 1,
		Steps: []domain.WorkflowStep{
			{StepName: "step-1", StepOrder: 1, TaskType: "test.step"},
		},
	}
	wf2 := &domain.WorkflowDefinition{
		Name:    name2,
		Version: 1,
		Steps: []domain.WorkflowStep{
			{StepName: "step-1", StepOrder: 1, TaskType: "test.step"},
		},
	}
	require.NoError(t, repo.CreateWorkflow(ctx, wf1))
	t.Cleanup(func() { cleanupWorkflow(t, pool, wf1.ID) })
	require.NoError(t, repo.CreateWorkflow(ctx, wf2))
	t.Cleanup(func() { cleanupWorkflow(t, pool, wf2.ID) })
	list, err := repo.ListWorkflows(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, list)
	// Check both created workflows appear in the list
	found1 := false
	found2 := false
	for _, item := range list {
		if item.ID == wf1.ID {
			found1 = true
			assert.Equal(t, name1, item.Name)
		}
		if item.ID == wf2.ID {
			found2 = true
			assert.Equal(t, name2, item.Name)
		}
	}
	assert.True(t, found1, "wf1 must be present in ListWorkflows")
	assert.True(t, found2, "wf2 must be present in ListWorkflows")
}



func TestPostgresRepository_UniqueNameAndVersionConstraint(t *testing.T) {
	pool, repo := setupTestDB(t)
	ctx := context.Background()
	sharedName := fmt.Sprintf("duplicate-check-%s", uuid.New().String()[:8])
	wf1 := &domain.WorkflowDefinition{
		Name:    sharedName,
		Version: 1,
		Steps: []domain.WorkflowStep{
			{StepName: "step-1", StepOrder: 1, TaskType: "test.step"},
		},
	}
	wf2 := &domain.WorkflowDefinition{
		Name:    sharedName,
		Version: 1, 
		Steps: []domain.WorkflowStep{
			{StepName: "step-1", StepOrder: 1, TaskType: "test.step"},
		},
	}
	err1 := repo.CreateWorkflow(ctx, wf1)
	require.NoError(t, err1)
	t.Cleanup(func() { cleanupWorkflow(t, pool, wf1.ID) })
	err2 := repo.CreateWorkflow(ctx, wf2)
	require.Error(t, err2, "inserting duplicate (name, version) must fail unique constraint")
}