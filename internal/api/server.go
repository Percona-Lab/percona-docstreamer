package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Percona-Lab/docMongoStream/internal/logging"
)

// Server wraps a standard http.Server to provide a clean Start/Stop interface
type Server struct {
	httpServer *http.Server
	mux        *http.ServeMux
	port       string
}

// NewServer creates a new API server instance
func NewServer(port string) *Server {
	if port == "" {
		port = "8080"
	}
	mux := http.NewServeMux()
	return &Server{
		mux:  mux,
		port: port,
		httpServer: &http.Server{
			Addr:    ":" + port,
			Handler: mux,
		},
	}
}

// RegisterRoute allows other modules to register their endpoints
func (s *Server) RegisterRoute(path string, handler http.HandlerFunc) {
	s.mux.HandleFunc(path, handler)
}

// Start runs the HTTP server in a non-blocking goroutine
func (s *Server) Start() {
	go func() {
		logging.PrintInfo(fmt.Sprintf("API Server starting on port %s...", s.port), 0)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logging.PrintError(fmt.Sprintf("API Server failed: %v", err), 0)
		}
	}()
}

// Stop gracefully shuts down the server
func (s *Server) Stop(ctx context.Context) error {
	logging.PrintInfo("Stopping API Server...", 0)
	return s.httpServer.Shutdown(ctx)
}
