package validator

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/flow"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"github.com/Percona-Lab/percona-docstreamer/internal/status"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type ValidationTask struct {
	Namespace string
	Keys      []bson.D
}

type RetryItem struct {
	Namespace    string
	Key          bson.D
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
	flowMgr         *flow.Manager
	wg              sync.WaitGroup
	shutdownCtx     context.Context
	shutdownCancel  context.CancelFunc
	isThrottled     atomic.Bool
}

func NewManager(source, target *mongo.Client, tracker *InFlightTracker, store *Store, statusMgr *status.Manager, flowMgr *flow.Manager) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	queueSize := config.Cfg.Validation.QueueSize
	if queueSize <= 0 {
		queueSize = 2000 // Fallback only if config failed completely
	}

	vm := &Manager{
		sourceClient:    source,
		targetClient:    target,
		tracker:         tracker,
		retryQueue:      make(chan RetryItem, queueSize),
		validationQueue: make(chan ValidationTask, queueSize),
		store:           store,
		statusMgr:       statusMgr,
		flowMgr:         flowMgr,
		shutdownCtx:     ctx,
		shutdownCancel:  cancel,
	}

	workerCount := config.Cfg.Validation.MaxValidationWorkers
	// Ensure at least 1 worker exists to prevent deadlock,
	// but relies on config default (4) for the upper value.
	if workerCount < 1 {
		workerCount = 1
	}

	// Add 1 for the retry worker and N for the queue workers
	vm.wg.Add(1 + workerCount)
	go vm.startRetryWorker()

	logging.PrintInfo(fmt.Sprintf("[VAL] Starting %d parallel CDC validation workers (Queue: %d)...", workerCount, queueSize), 0)
	for i := 0; i < workerCount; i++ {
		go vm.startQueueWorker(i)
	}

	go vm.startBackgroundSweep()

	return vm
}

func (vm *Manager) Close() {
	logging.PrintInfo("[VAL] Shutting down validation workers...", 0)
	vm.shutdownCancel()
	vm.wg.Wait()
	logging.PrintInfo("[VAL] Validation workers stopped.", 0)
}

func (vm *Manager) CanRun() bool {
	select {
	case <-vm.shutdownCtx.Done():
		return false
	default:
	}
	return config.Cfg.Validation.Enabled && vm.IsCloneComplete()
}

func (vm *Manager) IsCloneComplete() bool {
	if vm.statusMgr != nil && vm.statusMgr.IsInitialSyncCompleted() {
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dbName := config.Cfg.Migration.MetadataDB
	if dbName == "" {
		dbName = "docStreamer"
	}
	collName := config.Cfg.Migration.StatusCollection
	if collName == "" {
		collName = "status"
	}
	docID := config.Cfg.Migration.StatusDocID
	if docID == "" {
		docID = "migration_status"
	}

	var doc struct {
		InitialSyncCompleted bool `bson:"initialSyncCompleted"`
	}

	err := vm.targetClient.Database(dbName).Collection(collName).FindOne(ctx, bson.D{{Key: "_id", Value: docID}}).Decode(&doc)
	if err != nil {
		return false
	}

	if doc.InitialSyncCompleted {
		if vm.statusMgr != nil {
			vm.statusMgr.SetInitialSyncCompleted(0)
		}
		return true
	}

	return false
}

func (vm *Manager) startBackgroundSweep() {
	select {
	case <-vm.shutdownCtx.Done():
		return
	case <-time.After(10 * time.Second):
	}

	vm.RetryEligibleFailures()
	lastSweep := time.Now()

	maxInterval := config.Cfg.Validation.HotKeyCheckIntervalMinutes
	if maxInterval < 1 {
		maxInterval = 5
	}

	idlePollInterval := config.Cfg.Validation.IdleCheckIntervalSeconds
	if idlePollInterval < 1 {
		idlePollInterval = 5
	}

	maxRetries := config.Cfg.Validation.MaxRetries
	if maxRetries < 1 {
		maxRetries = 3
	}

	logging.PrintInfo(fmt.Sprintf("[VAL] Smart sweeper started. Max interval: %d min. Idle check every %ds.", maxInterval, idlePollInterval), 0)

	ticker := time.NewTicker(time.Duration(idlePollInterval) * time.Second)
	defer ticker.Stop()

	for {
		if vm.flowMgr != nil {
			paused, _, _ := vm.flowMgr.GetStatus()
			if paused {
				pauseDuration := config.Cfg.FlowControl.PauseDurationMS
				if pauseDuration == 0 {
					pauseDuration = 500
				}
				time.Sleep(time.Duration(pauseDuration) * time.Millisecond)
				continue
			}
		}

		select {
		case <-vm.shutdownCtx.Done():
			return
		case <-ticker.C:
			now := time.Now()

			timeSince := now.Sub(lastSweep)
			if timeSince >= time.Duration(maxInterval)*time.Minute {
				if vm.store.HasEligibleFailures(vm.shutdownCtx, maxRetries) {
					vm.RetryEligibleFailures()
				}
				lastSweep = time.Now()
				continue
			}

			idleSeconds, lagSeconds := vm.statusMgr.GetCDCIdleMetrics()

			if idleSeconds > float64(idlePollInterval) && lagSeconds < 1.0 {

				debounceDuration := time.Duration(idlePollInterval) * 3 * time.Second

				if timeSince > debounceDuration {
					if vm.store.HasEligibleFailures(vm.shutdownCtx, maxRetries) {
						vm.RetryEligibleFailures()
						lastSweep = time.Now()
					}
				}
			}
		}
	}
}

func (vm *Manager) RetryEligibleFailures() {
	if !vm.CanRun() {
		return
	}

	if vm.flowMgr != nil {
		paused, _, _ := vm.flowMgr.GetStatus()
		if paused {
			return
		}
	}

	const maxDBRetries = 10
	failures, err := vm.store.GetEligibleFailures(vm.shutdownCtx, maxDBRetries)
	if err != nil || len(failures) == 0 {
		return
	}

	if config.Cfg.Logging.Level == "debug" {
		logging.PrintInfo(fmt.Sprintf("[VAL] Background sweep: Processing %d items pending re-verification...", len(failures)), 0)
	}

	grouped := make(map[string][]bson.D)
	for _, f := range failures {
		var filter bson.D
		if len(f.Keys) > 0 {
			filter = f.Keys
		} else {
			var idVal interface{} = f.DocID
			if oid, err := bson.ObjectIDFromHex(f.DocID); err == nil {
				idVal = oid
			} else if parsedUUID, err := uuid.Parse(f.DocID); err == nil {
				b := make([]byte, 16)
				copy(b, parsedUUID[:])
				idVal = bson.Binary{Data: b, Subtype: 4}
			}
			filter = bson.D{{Key: "_id", Value: idVal}}
		}
		grouped[f.Namespace] = append(grouped[f.Namespace], filter)
	}

	for ns, filters := range grouped {
		for _, filter := range filters {
			if vm.shutdownCtx.Err() != nil {
				return
			}
			if vm.flowMgr != nil {
				vm.flowMgr.Wait()
			}
			vm.ValidateSync(vm.shutdownCtx, ns, []bson.D{filter})
		}
	}
}

func (vm *Manager) ValidateAsync(ns string, keys []bson.D) {
	if !vm.CanRun() {
		return
	}

	for _, k := range keys {
		vm.tracker.ClearDirty(extractIDString(k))
	}

	task := ValidationTask{Namespace: ns, Keys: keys}

	select {
	case vm.validationQueue <- task:
		if vm.statusMgr != nil {
			vm.statusMgr.SetValidationQueueSize(len(vm.validationQueue))
		}
		return
	default:
	}

	// If we reach here, the channel is 100% full. Set throttle.
	if !vm.isThrottled.Swap(true) {
		msg := fmt.Sprintf("Validator queue full (%d). Throttling CDC to match Validator speed...", cap(vm.validationQueue))
		if config.Cfg.Logging.Level == "debug" {
			logging.PrintWarning(fmt.Sprintf("[VAL] %s", msg), 0)
		}
		if vm.statusMgr != nil {
			vm.statusMgr.SetState("throttled", msg)
		}
	}

	// Block until space opens up
	select {
	case vm.validationQueue <- task:
	case <-vm.shutdownCtx.Done():
		logging.PrintInfo("[VAL] Dropping validation task due to shutdown.", 0)
	}
}

func (vm *Manager) GetQueueMetrics() (int, int, bool) {
	return len(vm.validationQueue), cap(vm.validationQueue), vm.isThrottled.Load()
}

func (vm *Manager) startQueueWorker(workerID int) {
	defer vm.wg.Done()
	for {
		if vm.flowMgr != nil {
			vm.flowMgr.Wait()
		}

		select {
		case <-vm.shutdownCtx.Done():
			return
		case task, ok := <-vm.validationQueue:
			if !ok {
				return
			}

			if vm.statusMgr != nil {
				vm.statusMgr.SetValidationQueueSize(len(vm.validationQueue))
			}

			if vm.isThrottled.Load() && len(vm.validationQueue) < cap(vm.validationQueue)/2 {
				if vm.isThrottled.Swap(false) {
					logging.PrintInfo("[VAL] Validator queue has recovered. Releasing throttle.", 0)
					if vm.statusMgr != nil {
						vm.statusMgr.SetState("running", "Change Data Capture")
					}
				}
			}

			ctx, cancel := context.WithTimeout(vm.shutdownCtx, 60*time.Second)
			_, err := vm.ValidateSync(ctx, task.Namespace, task.Keys)
			cancel()

			if err != nil && ctx.Err() == nil {
				logging.PrintError(fmt.Sprintf("[VAL] Batch validation failed for %s: %v. Persisting to retry queue.", task.Namespace, err), 0)
				vm.persistFailedBatch(task.Namespace, task.Keys, fmt.Sprintf("Batch fetch failed: %v", err))
			}
		}
	}
}

func (vm *Manager) persistFailedBatch(ns string, keys []bson.D, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for _, k := range keys {
		id := extractIDString(k)
		if id == "" {
			continue
		}

		res := ValidationResult{
			DocID:  id,
			Keys:   k,
			Status: "batch_failed",
			Reason: reason,
		}
		vm.store.RegisterOutcome(ctx, ns, res)
	}
}

func (vm *Manager) startRetryWorker() {
	defer vm.wg.Done()
	delayMS := config.Cfg.Validation.RetryIntervalMS
	if delayMS < 10 {
		delayMS = 500
	}
	retryDelay := time.Duration(delayMS) * time.Millisecond

	for {
		if vm.flowMgr != nil {
			vm.flowMgr.Wait()
		}

		select {
		case <-vm.shutdownCtx.Done():
			return
		case item := <-vm.retryQueue:
			go func(retItem RetryItem) {
				select {
				case <-vm.shutdownCtx.Done():
					return
				case <-time.After(retryDelay):
					vm.validateSingle(retItem)
				}
			}(item)
		}
	}
}

func extractIDString(filter bson.D) string {
	for _, e := range filter {
		if e.Key == "_id" {
			switch v := e.Value.(type) {
			case bson.ObjectID:
				return v.Hex()
			case bson.Binary:
				if v.Subtype == 4 {
					if u, err := uuid.FromBytes(v.Data); err == nil {
						return u.String()
					}
				}
				return fmt.Sprintf("%v", v)
			default:
				return fmt.Sprintf("%v", v)
			}
		}
	}
	return ""
}

func (vm *Manager) ValidateSync(ctx context.Context, namespace string, keys []bson.D) ([]ValidationResult, error) {
	return vm.validateInternal(ctx, namespace, keys, 0)
}

func (vm *Manager) validateInternal(ctx context.Context, namespace string, keys []bson.D, currentAttempt int) ([]ValidationResult, error) {
	results := []ValidationResult{}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	ids := make([]string, len(keys))
	for i, k := range keys {
		ids[i] = extractIDString(k)
	}

	srcDocs, err := vm.fetchDocsByIDOnly(ctx, vm.sourceClient, namespace, keys)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch source: %w", err)
	}

	maxRetries := config.Cfg.Validation.MaxRetries
	if maxRetries < 1 {
		maxRetries = 3
	}

	parts := strings.SplitN(namespace, ".", 2)
	targetColl := vm.targetClient.Database(parts[0]).Collection(parts[1])

	for i, id := range ids {
		if ctx.Err() != nil {
			return nil, nil
		}
		key := keys[i]
		res := ValidationResult{DocID: id, Keys: key}

		if vm.tracker.IsDirty(id) {
			if currentAttempt < maxRetries {
				res.Status = "skipped"
				res.Reason = "Active write in progress (Retrying...)"
				results = append(results, res)
				select {
				case <-vm.shutdownCtx.Done():
				case vm.retryQueue <- RetryItem{Namespace: namespace, Key: key, AttemptCount: currentAttempt + 1}:
				}
				continue
			} else {
				res.Status = "hot_key_waiting"
				res.Reason = fmt.Sprintf("Key remains hot after %d attempts. Deferred to background queue.", maxRetries)
				if config.Cfg.Logging.Level == "debug" {
					logging.PrintWarning(fmt.Sprintf("[VAL] Deferring hot key %s to background sweep.", id), 0)
				}
				vm.store.RegisterOutcome(ctx, namespace, res)
				results = append(results, res)
				continue
			}
		}

		src, okSrc := srcDocs[id]

		var idVal interface{}
		for _, e := range key {
			if e.Key == "_id" {
				idVal = e.Value
				break
			}
		}

		if idVal == nil {
			res.Status = "error"
			res.Reason = "Could not parse _id"
			vm.store.RegisterOutcome(ctx, namespace, res)
			results = append(results, res)
			continue
		}

		checkCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		count, errCount := targetColl.CountDocuments(checkCtx, bson.D{{Key: "_id", Value: idVal}})

		isGhost := (errCount == nil && count > 1)

		if okSrc {
			if isGhost {
				if config.Cfg.Logging.Level == "debug" {
					logging.PrintWarning(fmt.Sprintf("[VAL] Ghost detected for %s (Count: %d). Purging...", id, count), 0)
				}
				targetColl.DeleteMany(checkCtx, bson.D{{Key: "_id", Value: idVal}})
				_, errIns := targetColl.InsertOne(checkCtx, src)

				if errIns == nil {
					res.Status = "healed_pending_verify"
					res.Reason = "Auto-healed: Purged ghosts and re-inserted"
					if config.Cfg.Logging.Level == "debug" {
						logging.PrintSuccess(fmt.Sprintf("[VAL] Fixed ghosts for %s", id), 0)
					}
					vm.store.LogAudit(namespace, id, "ghost_fixed", fmt.Sprintf("Found %d copies, purged and re-inserted", count))
				} else {
					res.Status = "manual_validation_required"
					res.Reason = fmt.Sprintf("Repair failed: %v", errIns)
				}

			} else if count == 0 {
				_, errIns := targetColl.InsertOne(checkCtx, src)
				if errIns == nil {
					res.Status = "healed_pending_verify"
					res.Reason = "Auto-healed: Inserted missing doc"
					vm.store.LogAudit(namespace, id, "missing_target_fixed", "Document missing in destination, re-inserted")
				} else {
					res.Status = "manual_validation_required"
					res.Reason = fmt.Sprintf("Insert failed: %v", errIns)
				}

			} else {
				var actualDst bson.M
				errFind := targetColl.FindOne(checkCtx, bson.D{{Key: "_id", Value: idVal}}).Decode(&actualDst)
				if errFind == nil && reflect.DeepEqual(src, actualDst) {
					res.Status = "valid"
				} else {
					targetColl.DeleteMany(checkCtx, bson.D{{Key: "_id", Value: idVal}})
					targetColl.InsertOne(checkCtx, src)
					res.Status = "healed_pending_verify"
					res.Reason = "Auto-healed: Content mismatch fixed"
					if config.Cfg.Logging.Level == "debug" {
						logging.PrintSuccess(fmt.Sprintf("[VAL] Fixed content mismatch for %s", id), 0)
					}
					vm.store.LogAudit(namespace, id, "content_mismatch_fixed", "Field values differed, overwrote target")
				}
			}

		} else {
			if count > 0 {
				delRes, errDel := targetColl.DeleteOne(checkCtx, bson.D{{Key: "_id", Value: idVal}})
				if errDel == nil {
					res.Status = "healed_pending_verify"
					res.Reason = "Auto-healed: Deleted orphaned document"
					if config.Cfg.Logging.Level == "debug" {
						logging.PrintSuccess(fmt.Sprintf("[VAL] Deleted %d orphans for %s", delRes.DeletedCount, id), 0)
					}
					vm.store.LogAudit(namespace, id, "orphan_deleted", fmt.Sprintf("Deleted %d orphaned documents", delRes.DeletedCount))
				} else {
					res.Status = "manual_validation_required"
					res.Reason = fmt.Sprintf("Delete failed: %v", errDel)
				}
			} else {
				res.Status = "valid"
			}
		}
		cancel()
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
		grouped := make(map[string][]bson.D)
		for _, f := range failures {
			var filter bson.D
			if len(f.Keys) > 0 {
				filter = f.Keys
			} else {
				var idVal interface{} = f.DocID
				if oid, err := bson.ObjectIDFromHex(f.DocID); err == nil {
					idVal = oid
				} else if parsedUUID, err := uuid.Parse(f.DocID); err == nil {
					b := make([]byte, 16)
					copy(b, parsedUUID[:])
					idVal = bson.Binary{Data: b, Subtype: 4}
				}
				filter = bson.D{{Key: "_id", Value: idVal}}
			}
			grouped[f.Namespace] = append(grouped[f.Namespace], filter)
		}

		for ns, filters := range grouped {
			if vm.shutdownCtx.Err() != nil {
				return
			}
			chunkSize := 100
			for i := 0; i < len(filters); i += chunkSize {
				if vm.flowMgr != nil {
					vm.flowMgr.Wait()
				}
				end := i + chunkSize
				if end > len(filters) {
					end = len(filters)
				}
				vm.ValidateSync(vm.shutdownCtx, ns, filters[i:end])
			}
		}
	}()
	return len(failures), nil
}

func (vm *Manager) ReconcileStats(ctx context.Context) error {
	return vm.store.Reconcile(ctx)
}

func (vm *Manager) validateSingle(item RetryItem) {
	if vm.shutdownCtx.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(vm.shutdownCtx, 5*time.Second)
	defer cancel()
	vm.validateInternal(ctx, item.Namespace, []bson.D{item.Key}, item.AttemptCount)
}

func (vm *Manager) fetchDocsByIDOnly(ctx context.Context, client *mongo.Client, ns string, keys []bson.D) (map[string]bson.M, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	parts := strings.SplitN(ns, ".", 2)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid namespace: %s", ns)
	}
	dbName, collName := parts[0], parts[1]

	var orArray bson.A
	for _, k := range keys {
		for _, e := range k {
			if e.Key == "_id" {
				orArray = append(orArray, bson.D{{Key: "_id", Value: e.Value}})
				break
			}
		}
	}

	if len(orArray) == 0 {
		return make(map[string]bson.M), nil
	}

	filter := bson.D{{Key: "$or", Value: orArray}}
	cursor, err := client.Database(dbName).Collection(collName).Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	return processCursorResults(ctx, cursor)
}

func (vm *Manager) fetchDocsByFullKey(ctx context.Context, client *mongo.Client, ns string, keys []bson.D) (map[string]bson.M, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	parts := strings.SplitN(ns, ".", 2)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid namespace: %s", ns)
	}
	dbName, collName := parts[0], parts[1]

	filter := bson.D{{Key: "$or", Value: keys}}
	cursor, err := client.Database(dbName).Collection(collName).Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	return processCursorResults(ctx, cursor)
}

func processCursorResults(ctx context.Context, cursor *mongo.Cursor) (map[string]bson.M, error) {
	results := make(map[string]bson.M)
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		idVal := doc["_id"]
		var foundIDStr string

		switch v := idVal.(type) {
		case bson.ObjectID:
			foundIDStr = v.Hex()
		case bson.Binary:
			if v.Subtype == 4 {
				if u, err := uuid.FromBytes(v.Data); err == nil {
					foundIDStr = u.String()
				} else {
					foundIDStr = fmt.Sprintf("%v", idVal)
				}
			} else {
				foundIDStr = fmt.Sprintf("%v", idVal)
			}
		default:
			foundIDStr = fmt.Sprintf("%v", idVal)
		}

		results[foundIDStr] = doc
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (vm *Manager) QueueScan(ns, scanType string) {
	go func() {
		logging.PrintInfo(fmt.Sprintf("[VAL SCAN] Starting full validation scan for %s (Mode: %s)", ns, scanType), 0)

		ctx := context.Background() // Long-running background process
		parts := strings.SplitN(ns, ".", 2)

		var client *mongo.Client
		if scanType == "orphans" {
			client = vm.targetClient
		} else {
			client = vm.sourceClient
		}

		coll := client.Database(parts[0]).Collection(parts[1])
		opts := options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}})
		cursor, err := coll.Find(ctx, bson.D{}, opts)
		if err != nil {
			logging.PrintError(fmt.Sprintf("[VAL SCAN] Failed to query collection: %v", err), 0)
			return
		}
		defer cursor.Close(ctx)

		count := 0
		batch := make([]bson.D, 0, config.Cfg.Validation.BatchSize)

		for cursor.Next(ctx) {
			if vm.shutdownCtx.Err() != nil {
				logging.PrintInfo("[VAL SCAN] Scan aborted due to shutdown.", 0)
				return
			}

			if vm.flowMgr != nil {
				vm.flowMgr.Wait()
			}

			var result bson.D
			if err := cursor.Decode(&result); err != nil {
				continue
			}

			var idVal interface{}
			for _, e := range result {
				if e.Key == "_id" {
					idVal = e.Value
					break
				}
			}

			if idVal != nil {
				batch = append(batch, bson.D{{Key: "_id", Value: idVal}})
				count++
			}

			if len(batch) >= config.Cfg.Validation.BatchSize {
				vm.ValidateAsync(ns, batch)
				batch = make([]bson.D, 0, config.Cfg.Validation.BatchSize)
				time.Sleep(10 * time.Millisecond)
			}
		}

		if len(batch) > 0 {
			vm.ValidateAsync(ns, batch)
		}

		logging.PrintInfo(fmt.Sprintf("[VAL SCAN] Full scan queued %d documents for %s", count, ns), 0)
	}()
}
