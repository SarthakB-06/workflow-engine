package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sarthak/workflow-engine/internal/api"
	"github.com/sarthak/workflow-engine/internal/config"
	"github.com/sarthak/workflow-engine/internal/queue"
	"github.com/sarthak/workflow-engine/internal/worker"
	"github.com/sarthak/workflow-engine/internal/workflow/repository"
	"github.com/sarthak/workflow-engine/internal/workflow/service"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// background context for the pool — it lives for the whole process lifetime.
	pool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(context.Background()); err != nil {
		log.Fatalf("ping postgres: %v", err)
	}
	log.Println("✓ connected to postgres")

	// Buffer of 100: the engine can enqueue up to 100 tasks before blocking.
	// Week 7 replaces this with Redis Streams.
	memQueue := queue.NewMemoryQueue(100)

	workflowRepo := repository.NewWorkflowRepository(pool)
	execRepo     := repository.NewExecutionRepository(pool)

	 
	workflowSvc  := service.NewWorkflowService(workflowRepo)
	executionSvc := service.NewExecutionService(workflowRepo, execRepo, memQueue)

	// In Week 2 every task_type uses StubHandler (logs and returns nil).
	// Replace individual stubs with real implementations as the project grows.
	registry := worker.NewRegistry()
	registry.Register("CHARGE_PAYMENT",    worker.StubHandler)
	registry.Register("REFUND_PAYMENT",    worker.StubHandler)
	registry.Register("RESERVE_INVENTORY", worker.StubHandler)
	registry.Register("RELEASE_INVENTORY", worker.StubHandler)
	registry.Register("SHIP_ORDER",        worker.StubHandler)

	// A cancellable context drives the worker's shutdown.
	// When we receive SIGINT/SIGTERM below, we cancel this context,
	// which causes worker.Run() to exit its select loop cleanly.
	
	
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker() // safety net — ensures cancel is always called

	w := worker.NewWorker(memQueue, registry)
	go w.Run(workerCtx) // runs concurrently — doesn't block main

	
	// NewRouter now takes both services — routes.go was updated in step 5.
	router := api.NewRouter(workflowSvc, executionSvc)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.ServerPort),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("✓ API server listening on :%s", cfg.ServerPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit // blocks here until Ctrl+C or docker stop

	log.Println("shutdown signal received...")

	// Stop the worker first — no new tasks should be processed
	// while the HTTP server is draining in-flight requests.
	cancelWorker()

	// Give in-flight HTTP requests 30 seconds to complete.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("forced shutdown: %v", err)
	}
	log.Println("server stopped cleanly")
}
