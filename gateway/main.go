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
	statusv1 "github.com/angzarr-io/angzarr/gateway/gen/io/angzarr/status/v1"
	angzarrv1 "github.com/angzarr-io/angzarr/gateway/gen/io/angzarr/v1"
	"github.com/angzarr-io/angzarr/gateway/routing"
)

//go:embed api/*
var apiFS embed.FS

var (
	grpcTarget              = flag.String("grpc-target", "", "Single aggregate gRPC target for every domain (default: GRPC_TARGET env; localhost:1310 when no template is set)")
	aggregateTargetTemplate = flag.String("aggregate-target-template", "", "Per-domain aggregate target, {domain} substituted, e.g. {domain}-aggregate:1310 (default: AGGREGATE_TARGET_TEMPLATE env)")
	statusTarget            = flag.String("status-target", "", "angzarr-status gRPC target serving DlqAdminService (default: STATUS_TARGET env; unset = no /api/dlq routes)")
	httpPort                = flag.Int("http-port", 8080, "HTTP server port")
	descriptorFile          = flag.String("descriptor-file", "", "Proto descriptor file for type discovery (default: DESCRIPTOR_PATH env)")
)

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func dial(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// buildGatewayMux registers the REST surface: aggregate services
// (CommandHandlerCoordinator, EventQuery) through the per-domain router,
// and DlqAdminService on angzarr-status when a status connection is given.
// Saga/PM/projector coordinators and EventStream are not exposed: they are
// per-component internal services with no single backend.
func buildGatewayMux(ctx context.Context, aggregates grpc.ClientConnInterface, status grpc.ClientConnInterface) (*runtime.ServeMux, error) {
	gwMux := runtime.NewServeMux(runtime.WithMetadata(routing.DomainAnnotator))

	if err := angzarrv1.RegisterCommandHandlerCoordinatorServiceHandlerClient(ctx, gwMux,
		angzarrv1.NewCommandHandlerCoordinatorServiceClient(aggregates)); err != nil {
		return nil, fmt.Errorf("register CommandHandlerCoordinatorService: %w", err)
	}
	if err := angzarrv1.RegisterEventQueryServiceHandlerClient(ctx, gwMux,
		angzarrv1.NewEventQueryServiceClient(aggregates)); err != nil {
		return nil, fmt.Errorf("register EventQueryService: %w", err)
	}
	if status != nil {
		if err := statusv1.RegisterDlqAdminServiceHandlerClient(ctx, gwMux,
			statusv1.NewDlqAdminServiceClient(status)); err != nil {
			return nil, fmt.Errorf("register DlqAdminService: %w", err)
		}
	}
	return gwMux, nil
}

func main() {
	flag.Parse()

	template := firstNonEmpty(*aggregateTargetTemplate, os.Getenv("AGGREGATE_TARGET_TEMPLATE"))
	target := firstNonEmpty(*grpcTarget, os.Getenv("GRPC_TARGET"))
	if template == "" && target == "" {
		target = "localhost:1310"
	}
	statusAddr := firstNonEmpty(*statusTarget, os.Getenv("STATUS_TARGET"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aggregates, err := routing.NewRouter(template, target, dial)
	if err != nil {
		log.Fatalf("Invalid aggregate routing: %v", err)
	}
	defer aggregates.Close()

	var statusConn grpc.ClientConnInterface
	if statusAddr != "" {
		conn, err := dial(statusAddr)
		if err != nil {
			log.Fatalf("Failed to connect to angzarr-status at %s: %v", statusAddr, err)
		}
		defer conn.Close()
		statusConn = conn
	}

	gwMux, err := buildGatewayMux(ctx, aggregates, statusConn)
	if err != nil {
		log.Fatalf("Failed to build gateway: %v", err)
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
		// Bounded header read and idle time; no write timeout, which would
		// cut off the event stream route.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
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

	log.Printf("Starting gRPC-Gateway on %s (aggregates: template=%q target=%q, status=%q)",
		addr, template, target, statusAddr)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}
	log.Println("Server stopped")
}
