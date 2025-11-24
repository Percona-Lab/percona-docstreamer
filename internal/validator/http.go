package validator

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type ValidationRequest struct {
	Namespace string   `json:"namespace"`
	IDs       []string `json:"ids"`
}

// HandleValidateRequest validates documents and returns results immediately
func (vm *Manager) HandleValidateRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
		return
	}

	if !vm.CanRun() {
		http.Error(w, "Validation unavailable: CDC must be running first.", http.StatusServiceUnavailable)
		return
	}

	var req ValidationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Namespace == "" || len(req.IDs) == 0 {
		http.Error(w, "Namespace and IDs are required", http.StatusBadRequest)
		return
	}

	// Synchronous Call
	results, err := vm.ValidateSync(r.Context(), req.Namespace, req.IDs)
	if err != nil {
		http.Error(w, fmt.Sprintf("Validation failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// HandleRetryFailures triggers a re-check of all known failures
func (vm *Manager) HandleRetryFailures(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method not allowed. This endpoint requires a POST request.", http.StatusMethodNotAllowed)
		return
	}

	count, err := vm.RetryAllFailures(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	// Intelligent Response: Check if there was anything to retry
	if count == 0 {
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"message": "No active validation failures found. System is healthy.",
		})
		return
	}

	// UPDATED MESSAGE: Clarify that this is a check, not a fix
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "accepted",
		"message": fmt.Sprintf("Queued re-validation check for %d documents. This process updates status but does not repair data.", count),
	})
}

// HandleGetStats returns the aggregated counters (Valid vs Mismatch)
func (vm *Manager) HandleGetStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method not allowed. Use GET.", http.StatusMethodNotAllowed)
		return
	}

	stats, err := vm.store.GetStats(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch stats: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if stats == nil {
		w.Write([]byte("[]"))
	} else {
		json.NewEncoder(w).Encode(stats)
	}
}
