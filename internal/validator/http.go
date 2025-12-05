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

// HandleAdHocValidation provides a dedicated endpoint for on-demand validation
// that strictly requires the full load to be completed.
func (vm *Manager) HandleAdHocValidation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
		return
	}

	// 1. Enforce 'full load completed' prerequisite
	if !vm.IsCloneComplete() {
		http.Error(w, "Validation unavailable: Full load (clone) process must be completed first.", http.StatusForbidden)
		return
	}

	var req ValidationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Namespace == "" {
		http.Error(w, "Namespace is required", http.StatusBadRequest)
		return
	}
	if len(req.IDs) == 0 {
		// Reject request to validate an entire collection as it requires a full scan
		// which is not supported by the existing API/Manager logic.
		http.Error(w, "Validation of an entire collection is not currently supported. Provide specific document IDs.", http.StatusBadRequest)
		return
	}

	// 2. Perform synchronous validation for the provided records
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

	if count == 0 {
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"message": "No active validation failures found. System is healthy.",
		})
		return
	}

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

// HandleReset clears or reconciles validation data
// Default: Smart Reset (Reconcile stats with actual DB records)
// Query Param: ?mode=erase (Wipe all stats to zero)
func (vm *Manager) HandleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
		return
	}

	mode := r.URL.Query().Get("mode")

	// Optional: Hard Reset (Destructive Wipe)
	if mode == "erase" {
		if err := vm.store.Reset(r.Context()); err != nil {
			http.Error(w, fmt.Sprintf("Failed to reset stats: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"message": "HARD RESET: Validation stats and failure records have been wiped.",
		})
		return
	}

	// Default: Smart Reset (Reconcile)
	if err := vm.store.Reconcile(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf("Failed to reconcile stats: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": "Validation stats successfully reconciled with stored failures.",
	})
}
