package validator

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/Percona-Lab/docMongoStream/internal/config"
	"github.com/Percona-Lab/docMongoStream/internal/logging"
	"github.com/Percona-Lab/docMongoStream/internal/status"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// ValidationTask holds a batch of IDs to verify
type ValidationTask struct {
	Namespace string
	IDs       []string
}

type RetryItem struct {
	Namespace    string
	ID           string
	AttemptCount int
}

type Manager struct {
	sourceClient    *mongo.Client
	targetClient    *mongo.Client
	tracker         *InFlightTracker
	retryQueue      chan RetryItem
	validationQueue chan ValidationTask
	store           *Store
	statusMgr       *status.Manager
	wg              sync.WaitGroup
	shutdownCtx     context.Context
	shutdownCancel  context.CancelFunc
}

func NewManager(source, target *mongo.Client, tracker *InFlightTracker, store *Store, statusMgr *status.Manager) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	// Ensure sane defaults if config is zero
	queueSize := config.Cfg.Validation.QueueSize
	if queueSize < 100 {
		queueSize = 2000
	}

	vm := &Manager{
		sourceClient:    source,
		targetClient:    target,
		tracker:         tracker,
		retryQueue:      make(chan RetryItem, queueSize),
		validationQueue: make(chan ValidationTask, queueSize),
		store:           store,
		statusMgr:       statusMgr,
		shutdownCtx:     ctx,
		shutdownCancel:  cancel,
	}

	// Start workers with WaitGroup tracking
	// Launch multiple queue workers for parallelism
	workerCount := config.Cfg.Validation.MaxValidationWorkers
	if workerCount < 1 {
		workerCount = 1
	}

	vm.wg.Add(1 + workerCount) // 1 Retry Worker + N Queue Workers

	go vm.startRetryWorker()

	logging.PrintInfo(fmt.Sprintf("[VAL] Starting %d parallel CDC validation workers...", workerCount), 0)
	for i := 0; i < workerCount; i++ {
		go vm.startQueueWorker(i)
	}

	return vm
}

// Close gracefully stops all validation workers
func (vm *Manager) Close() {
	logging.PrintInfo("[VAL] Shutting down validation workers...", 0)

	// 1. Signal workers to stop
	vm.shutdownCancel()

	// 2. Close the main queue
	close(vm.validationQueue)

	// 3. Wait for all workers to finish
	vm.wg.Wait()
	logging.PrintInfo("[VAL] Validation workers stopped.", 0)
}

func (vm *Manager) CanRun() bool {
	select {
	case <-vm.shutdownCtx.Done():
		return false
	default:
	}
	return vm.statusMgr.IsCDCActive() && config.Cfg.Validation.Enabled
}

// ValidateAsync is called by CDC to queue records for verification
func (vm *Manager) ValidateAsync(ns string, ids []string) {
	if !vm.CanRun() {
		return
	}
	select {
	case vm.validationQueue <- ValidationTask{Namespace: ns, IDs: ids}:
	default:
		logging.PrintWarning(fmt.Sprintf("[VAL] Validation queue full. Dropping batch of %d records for %s", len(ids), ns), 0)
	}
}

// startQueueWorker processes the stream of IDs from CDC
func (vm *Manager) startQueueWorker(workerID int) {
	defer vm.wg.Done()

	for task := range vm.validationQueue {
		if vm.shutdownCtx.Err() != nil {
			break
		}

		// Use shutdownCtx derived context
		ctx, cancel := context.WithTimeout(vm.shutdownCtx, 60*time.Second)
		_, err := vm.ValidateSync(ctx, task.Namespace, task.IDs)
		cancel()

		if err != nil && ctx.Err() == nil {
			logging.PrintError(fmt.Sprintf("[VAL] Batch validation failed for %s: %v", task.Namespace, err), 0)
		}
	}
}

// startRetryWorker handles retries with shutdown awareness
func (vm *Manager) startRetryWorker() {
	defer vm.wg.Done()

	// Use configured retry delay
	delayMS := config.Cfg.Validation.RetryIntervalMS
	if delayMS < 10 {
		delayMS = 500
	}
	retryDelay := time.Duration(delayMS) * time.Millisecond

	for {
		select {
		case <-vm.shutdownCtx.Done():
			return
		case item := <-vm.retryQueue:
			select {
			case <-vm.shutdownCtx.Done():
				return
			case <-time.After(retryDelay):
				vm.validateSingle(item)
			}
		}
	}
}

// ValidateSync performs validation and returns results immediately
func (vm *Manager) ValidateSync(ctx context.Context, namespace string, ids []string) ([]ValidationResult, error) {
	results := []ValidationResult{}

	// Fast path check for shutdown
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	for _, id := range ids {
		vm.tracker.ClearDirty(id)
	}

	srcDocs, err := vm.fetchDocs(ctx, vm.sourceClient, namespace, ids)
	if err != nil {
		// Don't return error if it was caused by context cancellation
		if ctx.Err() != nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to fetch source: %w", err)
	}
	dstDocs, err := vm.fetchDocs(ctx, vm.targetClient, namespace, ids)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to fetch target: %w", err)
	}

	for _, id := range ids {
		// Check shutdown inside loop
		if ctx.Err() != nil {
			return nil, nil
		}

		res := ValidationResult{DocID: id}

		if vm.tracker.IsDirty(id) {
			res.Status = "skipped"
			res.Reason = "Active write in progress (Hot Key)"
			select {
			case <-vm.shutdownCtx.Done():
			case vm.retryQueue <- RetryItem{Namespace: namespace, ID: id, AttemptCount: 1}:
			}
			results = append(results, res)
			continue
		}

		src, okSrc := srcDocs[id]
		dst, okDst := dstDocs[id]

		if !okSrc && !okDst {
			res.Status = "valid"
			res.Reason = "Missing in both (Consistent)"
		} else if !okSrc {
			res.Status = "missing_source"
			res.Reason = "Exists in target but not source"
		} else if !okDst {
			res.Status = "missing_target"
			res.Reason = "Exists in source but not target"
		} else if !reflect.DeepEqual(src, dst) {
			res.Status = "mismatch"
			res.Reason = "Field content differs"
		} else {
			res.Status = "valid"
		}

		vm.store.RegisterOutcome(ctx, namespace, res)
		results = append(results, res)
	}

	return results, nil
}

func (vm *Manager) RetryAllFailures(ctx context.Context) (int, error) {
	failures, err := vm.store.GetAllFailureIDs(ctx)
	if err != nil {
		return 0, err
	}
	if len(failures) == 0 {
		return 0, nil
	}

	go func() {
		grouped := make(map[string][]string)
		for _, f := range failures {
			grouped[f.Namespace] = append(grouped[f.Namespace], f.DocID)
		}
		for ns, ids := range grouped {
			if vm.shutdownCtx.Err() != nil {
				return
			}
			vm.ValidateSync(vm.shutdownCtx, ns, ids)
		}
	}()
	return len(failures), nil
}

// ReconcileStats fixes drift between validation_stats and validation_failures
func (vm *Manager) ReconcileStats(ctx context.Context) error {
	return vm.store.Reconcile(ctx)
}

func (vm *Manager) validateSingle(item RetryItem) {
	if vm.shutdownCtx.Err() != nil {
		return
	}

	maxRetries := config.Cfg.Validation.MaxRetries
	if maxRetries < 1 {
		maxRetries = 3
	}

	if vm.tracker.IsDirty(item.ID) {
		if item.AttemptCount >= maxRetries {
			logging.PrintWarning(fmt.Sprintf("[VAL] Dropping Hot Key %s", item.ID), 0)
			return
		}
		select {
		case <-vm.shutdownCtx.Done():
		case vm.retryQueue <- RetryItem{Namespace: item.Namespace, ID: item.ID, AttemptCount: item.AttemptCount + 1}:
		}
		return
	}

	ctx, cancel := context.WithTimeout(vm.shutdownCtx, 5*time.Second)
	defer cancel()
	vm.ValidateSync(ctx, item.Namespace, []string{item.ID})
}

var objectIDWrapperRegex = regexp.MustCompile(`^ObjectId\(['"]([0-9a-fA-F]{24})['"]\)$`)

func (vm *Manager) fetchDocs(ctx context.Context, client *mongo.Client, ns string, ids []string) (map[string]bson.M, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	parts := strings.SplitN(ns, ".", 2)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid namespace: %s", ns)
	}
	dbName, collName := parts[0], parts[1]

	bsonIDs := bson.A{}
	idMap := make(map[string]string)

	for _, rawID := range ids {
		cleanID := rawID
		matches := objectIDWrapperRegex.FindStringSubmatch(strings.TrimSpace(rawID))
		if len(matches) == 2 {
			cleanID = matches[1]
		}
		idMap[cleanID] = rawID

		if oid, err := bson.ObjectIDFromHex(cleanID); err == nil {
			bsonIDs = append(bsonIDs, oid)
		}
		bsonIDs = append(bsonIDs, cleanID)
	}

	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: bsonIDs}}}}
	cursor, err := client.Database(dbName).Collection(collName).Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	results := make(map[string]bson.M)
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		idVal := doc["_id"]
		var foundIDStr string
		if oid, ok := idVal.(bson.ObjectID); ok {
			foundIDStr = oid.Hex()
		} else {
			foundIDStr = fmt.Sprintf("%v", idVal)
		}

		if originalInput, exists := idMap[foundIDStr]; exists {
			results[originalInput] = doc
			results[foundIDStr] = doc
		} else {
			results[foundIDStr] = doc
		}
	}
	return results, nil
}
