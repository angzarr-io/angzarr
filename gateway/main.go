package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/angzarr-io/angzarr/gateway/discovery"
	gw "github.com/angzarr-io/angzarr/gateway/gen/angzarr_client/proto/angzarr"
	// H-42: angzarr-status DLQ admin protos live under core/main/proto and
	// generate into gen/angzarr/status (their own go_package). The REST
	// surface (`/api/dlq`, `/api/dlq/{id}`, etc.) is dead unless we
	// register this handler.
	status "github.com/angzarr-io/angzarr/gateway/gen/angzarr/status"
)

//go:embed api/*
var apiFS embed.FS

var (
	grpcTarget     = flag.String("grpc-target", "", "gRPC server endpoint (default: GRPC_TARGET env or localhost:1310)")
	httpPort       = flag.Int("http-port", 8080, "HTTP server port")
	descriptorFile = flag.String("descriptor-file", "", "Proto descriptor file for type discovery (default: DESCRIPTOR_PATH env)")
)

func main() {
	flag.Parse()

	target := *grpcTarget
	if target == "" {
		target = os.Getenv("GRPC_TARGET")
	}
	if target == "" {
		target = "localhost:1310"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up gRPC connection
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server at %s: %v", target, err)
	}
	defer conn.Close()

	// Create gRPC-Gateway mux
	gwMux := runtime.NewServeMux()

	// Register all service handlers
	if err := gw.RegisterCommandHandlerCoordinatorServiceHandler(ctx, gwMux, conn); err != nil {
		log.Fatalf("Failed to register CommandHandlerCoordinatorService handler: %v", err)
	}
	if err := gw.RegisterEventQueryServiceHandler(ctx, gwMux, conn); err != nil {
		log.Fatalf("Failed to register EventQueryService handler: %v", err)
	}
	if err := gw.RegisterEventStreamServiceHandler(ctx, gwMux, conn); err != nil {
		log.Fatalf("Failed to register EventStreamService handler: %v", err)
	}
	if err := gw.RegisterSagaCoordinatorServiceHandler(ctx, gwMux, conn); err != nil {
		log.Fatalf("Failed to register SagaCoordinatorService handler: %v", err)
	}
	if err := gw.RegisterProjectorCoordinatorServiceHandler(ctx, gwMux, conn); err != nil {
		log.Fatalf("Failed to register ProjectorCoordinatorService handler: %v", err)
	}
	if err := gw.RegisterProcessManagerCoordinatorServiceHandler(ctx, gwMux, conn); err != nil {
		log.Fatalf("Failed to register ProcessManagerCoordinatorService handler: %v", err)
	}
	// H-42: DLQ admin REST routes (GET /api/dlq, GET /api/dlq/{id},
	// DELETE /api/dlq/{id}, POST /api/dlq/{id}/replay). Connected to the
	// same gRPC backend; the angzarr-status pod serves DlqAdminService on
	// that endpoint via the in-process command-handler binary.
	if err := status.RegisterDlqAdminServiceHandler(ctx, gwMux, conn); err != nil {
		log.Fatalf("Failed to register DlqAdminService handler: %v", err)
	}

	// Load base OpenAPI spec
	apiContent, err := fs.Sub(apiFS, "api")
	if err != nil {
		log.Fatalf("Failed to access embedded API files: %v", err)
	}
	baseSpec, err := fs.ReadFile(apiContent, "angzarr.swagger.json")
	if err != nil {
		log.Fatalf("Failed to read OpenAPI spec: %v", err)
	}

	// Initialize discovery service (loads types from descriptor file)
	discoverySvc, err := discovery.NewService(baseSpec, *descriptorFile)
	if err != nil {
		log.Fatalf("Failed to initialize discovery: %v", err)
	}

	// Create main HTTP mux
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// OpenAPI spec endpoint
	mux.HandleFunc("/openapi.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(discoverySvc.GetSpec())
	})

	// Discovery info endpoint
	mux.HandleFunc("/discovery/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(discoverySvc.GetInfo())
	})

	// Discovery types endpoint
	mux.HandleFunc("/discovery/types", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(discoverySvc.GetTypes())
	})

	// Collisions endpoint
	mux.HandleFunc("/discovery/collisions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(discoverySvc.GetCollisions())
	})

	// Mount gRPC-Gateway at root
	mux.Handle("/", gwMux)

	addr := fmt.Sprintf(":%d", *httpPort)
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		log.Println("Shutting down HTTP server...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)
		}
		cancel()
	}()

	log.Printf("Starting gRPC-Gateway on %s, proxying to %s", addr, target)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}
	log.Println("Server stopped")
}
