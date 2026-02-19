package validator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type ValidationRequest struct {
	Namespace string                   `json:"namespace"`
	IDs       []string                 `json:"ids,omitempty"`       // Legacy simple IDs
	Keys      []map[string]interface{} `json:"keys,omitempty"`      // Complex composite keys (e.g., _id + shard keys)
	ScanType  string                   `json:"scan_type,omitempty"` // "source" (default) or "orphans"
}

// Regex to handle "ObjectId('...')" and "UUID('...')" format often used in manual requests
var apiObjectIDRegex = regexp.MustCompile(`(?i)^ObjectId\(['"]([0-9a-fA-F]{24})['"]\)$`)
var apiUUIDRegex = regexp.MustCompile(`(?i)^UUID\(['"]([0-9a-fA-F-]{36})['"]\)$`)

// parseJSONValue recursively parses JSON into BSON-friendly types.
func parseJSONValue(val interface{}) interface{} {
	switch v := val.(type) {
	case string:
		cleanVal := strings.TrimSpace(v)

		// 1. Handle ObjectId('...') wrapper if present
		if matches := apiObjectIDRegex.FindStringSubmatch(cleanVal); len(matches) == 2 {
			cleanVal = matches[1]
		}

		// 2. Handle UUID('...') wrapper if present
		if matches := apiUUIDRegex.FindStringSubmatch(cleanVal); len(matches) == 2 {
			cleanVal = matches[1]
		}

		// 3. Try parsing as ObjectID
		if oid, err := bson.ObjectIDFromHex(cleanVal); err == nil {
			return oid
		}

		// 4. Try parsing as UUID
		if parsedUUID, err := uuid.Parse(cleanVal); err == nil {
			b := make([]byte, 16)
			copy(b, parsedUUID[:])
			return bson.Binary{Data: b, Subtype: 4}
		}

		return v

	case float64:
		// Convert JSON numbers with no fractional part to int64
		if v == float64(int64(v)) {
			return int64(v)
		}
		return v

	case map[string]interface{}:
		doc := bson.D{}
		for mapKey, mapVal := range v {
			doc = append(doc, bson.E{Key: mapKey, Value: parseJSONValue(mapVal)})
		}
		return doc

	case []interface{}:
		arr := bson.A{}
		for _, arrVal := range v {
			arr = append(arr, parseJSONValue(arrVal))
		}
		return arr

	default:
		return v
	}
}

// convertRequestToKeys converts the request payload into a list of bson.D filters.
func convertRequestToKeys(req ValidationRequest) []bson.D {
	var keys []bson.D

	// 1. Process legacy IDs array
	for _, rawID := range req.IDs {
		val := parseJSONValue(rawID)
		keys = append(keys, bson.D{{Key: "_id", Value: val}})
	}

	// 2. Process complex Keys array
	for _, keyMap := range req.Keys {
		doc := bson.D{}
		for k, v := range keyMap {
			doc = append(doc, bson.E{Key: k, Value: parseJSONValue(v)})
		}
		keys = append(keys, doc)
	}

	return keys
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
	if req.Namespace == "" || (len(req.IDs) == 0 && len(req.Keys) == 0) {
		http.Error(w, "Namespace and at least one ID or Key are required", http.StatusBadRequest)
		return
	}

	// Convert payload strings/maps to BSON Filters
	keys := convertRequestToKeys(req)

	// Synchronous Call
	results, err := vm.ValidateSync(r.Context(), req.Namespace, keys)
	if err != nil {
		http.Error(w, fmt.Sprintf("Validation failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// HandleAdHocValidation provides a dedicated endpoint for on-demand validation and repair
func (vm *Manager) HandleAdHocValidation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
		return
	}

	if !vm.IsCloneComplete() {
		http.Error(w, "Validation unavailable: Full load (clone) process must be completed first.", http.StatusForbidden)
		return
	}

	var req ValidationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Namespace == "" || (len(req.IDs) == 0 && len(req.Keys) == 0) {
		http.Error(w, "Namespace and specific document IDs or Keys are required.", http.StatusBadRequest)
		return
	}

	keys := convertRequestToKeys(req)

	results, err := vm.ValidateSync(r.Context(), req.Namespace, keys)
	if err != nil {
		http.Error(w, fmt.Sprintf("Validation failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "completed",
		"message": "On-demand validation and repair cycle finished.",
		"results": results,
	})
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
		"message": fmt.Sprintf("Queued re-validation and auto-repair for %d documents. Mismatched records will be surgically synchronized from source to target.", count),
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
func (vm *Manager) HandleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
		return
	}

	mode := r.URL.Query().Get("mode")

	// Hard Reset (Destructive Wipe)
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

	// Smart Reset (Reconcile)
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

// HandleGetFailures returns all active validation failures currently stored in the database
func (vm *Manager) HandleGetFailures(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method not allowed. Use GET.", http.StatusMethodNotAllowed)
		return
	}

	failures, err := vm.store.GetAllFailureIDs(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch failures: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if failures == nil {
		w.Write([]byte("[]"))
	} else {
		json.NewEncoder(w).Encode(failures)
	}
}

// HandleGetQueueStatus returns real-time metrics on the validation queue and throttling state
func (vm *Manager) HandleGetQueueStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method not allowed. Use GET.", http.StatusMethodNotAllowed)
		return
	}

	used, capacity, throttled := vm.GetQueueMetrics()

	status := map[string]interface{}{
		"queue_used":     used,
		"queue_capacity": capacity,
		"is_throttled":   throttled,
		"status_message": "Healthy",
	}

	if throttled {
		status["status_message"] = "THROTTLED: Validator is applying backpressure to CDC."
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// HandleScan triggers a background scan of the entire collection
func (vm *Manager) HandleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
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

	vm.QueueScan(req.Namespace, req.ScanType)

	w.Header().Set("Content-Type", "application/json")
	msg := fmt.Sprintf("Full validation scan started for %s (Type: Source -> Target).", req.Namespace)
	if req.ScanType == "orphans" {
		msg = fmt.Sprintf("Orphan scan started for %s (Type: Target -> Source). This will delete extra records on destination.", req.Namespace)
	}

	json.NewEncoder(w).Encode(map[string]string{
		"status":  "accepted",
		"message": msg,
	})
}
