package cdc

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/checkpoint"
	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"github.com/Percona-Lab/percona-docstreamer/internal/status"
	"github.com/Percona-Lab/percona-docstreamer/internal/validator"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type CDCManager struct {
	sourceClient *mongo.Client
	targetClient *mongo.Client
	eventQueue   chan *ChangeEvent
	// channel type carries Batch struct (Models + IDs)
	flushQueues        []chan map[string]*Batch
	bulkWriters        []*BulkWriter
	startAt            bson.Timestamp
	lastSuccessfulTS   bson.Timestamp
	checkpoint         *checkpoint.Manager
	statusManager      *status.Manager
	tracker            *validator.InFlightTracker
	store              *validator.Store
	validatorMgr       *validator.Manager // Access to validation manager
	shutdownWG         sync.WaitGroup
	workerWG           sync.WaitGroup
	totalEventsApplied atomic.Int64
	checkpointDocID    string
	// Maps for fast exclusion lookup
	excludeDBs   map[string]bool
	excludeColls map[string]bool
}

func NewManager(source, target *mongo.Client, checkpointDocID string, startAt bson.Timestamp, checkpoint *checkpoint.Manager, statusMgr *status.Manager, tracker *validator.InFlightTracker, store *validator.Store, valMgr *validator.Manager) *CDCManager {
	resumeTS, found := checkpoint.GetResumeTimestamp(context.Background(), checkpointDocID)

	if !found {
		logging.PrintWarning(fmt.Sprintf("[CDC %s] Checkpoint not found. Falling back to provided start time: %v", checkpointDocID, startAt), 0)
		resumeTS = startAt
	} else {
		logging.PrintInfo(fmt.Sprintf("[CDC %s] Loaded checkpoint. Resuming from %v", checkpointDocID, resumeTS), 0)
	}

	initialEvents := statusMgr.GetEventsApplied()
	if initialEvents > 0 {
		logging.PrintInfo(fmt.Sprintf("[CDC] Resuming event count from %d", initialEvents), 0)
	}

	workerCount := config.Cfg.CDC.MaxWriteWorkers
	if workerCount < 1 {
		workerCount = 1
	}

	queues := make([]chan map[string]*Batch, workerCount)
	writers := make([]*BulkWriter, workerCount)

	for i := 0; i < workerCount; i++ {
		queues[i] = make(chan map[string]*Batch, config.Cfg.Migration.MaxConcurrentWorkers)
		writers[i] = NewBulkWriter(target, config.Cfg.CDC.BatchSize)
	}

	// --- Initialize Exclusion Maps ---
	excludeDBs := make(map[string]bool)
	for _, db := range config.Cfg.Migration.ExcludeDBs {
		excludeDBs[db] = true
	}

	excludeColls := make(map[string]bool)
	for _, ns := range config.Cfg.Migration.ExcludeCollections {
		excludeColls[ns] = true
	}

	mgr := &CDCManager{
		sourceClient:     source,
		targetClient:     target,
		eventQueue:       make(chan *ChangeEvent, config.Cfg.CDC.BatchSize*2),
		flushQueues:      queues,
		bulkWriters:      writers,
		startAt:          resumeTS,
		lastSuccessfulTS: resumeTS,
		checkpoint:       checkpoint,
		statusManager:    statusMgr,
		tracker:          tracker,
		store:            store,
		validatorMgr:     valMgr,
		shutdownWG:       sync.WaitGroup{},
		workerWG:         sync.WaitGroup{},
		checkpointDocID:  checkpointDocID,
		excludeDBs:       excludeDBs,
		excludeColls:     excludeColls,
	}
	mgr.totalEventsApplied.Store(initialEvents)
	return mgr
}

// handleBulkWrite performs the write AND returns the max timestamp in the batch
func (m *CDCManager) handleBulkWrite(ctx context.Context, batchMap map[string]*Batch) (int64, []string, bson.Timestamp, error) {
	var totalOps int64
	var namespaces []string
	var firstErr error
	var batchMaxTS bson.Timestamp

	for ns, batch := range batchMap {
		if len(batch.Models) == 0 {
			continue
		}

		// Track max timestamp for this flush
		if batch.LastTS.T > batchMaxTS.T ||
			(batch.LastTS.T == batchMaxTS.T && batch.LastTS.I > batchMaxTS.I) {
			batchMaxTS = batch.LastTS
		}

		db, coll := splitNamespace(ns)
		if coll == "" {
			logging.PrintError(fmt.Sprintf("[%s] Invalid namespace, cannot split. Skipping batch.", ns), 0)
			continue
		}

		targetColl := m.targetClient.Database(db).Collection(coll)
		opts := options.BulkWrite().SetOrdered(true)

		result, err := targetColl.BulkWrite(ctx, batch.Models, opts)

		if err != nil {
			logging.PrintError(fmt.Sprintf("[%s] BulkWrite failed: %v", ns, err), 0)
			if wErr, ok := err.(mongo.BulkWriteException); ok {
				logging.PrintError(fmt.Sprintf("[%s] ... %d write errors", ns, len(wErr.WriteErrors)), 0)
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		totalOps += result.InsertedCount + result.ModifiedCount + result.UpsertedCount + result.DeletedCount
		namespaces = append(namespaces, ns)

		// --- EVENT-DRIVEN VALIDATION ---
		// The write succeeded. Now we queue these IDs for validation.
		if len(batch.IDs) > 0 {
			m.validatorMgr.ValidateAsync(ns, batch.IDs)
		}
	}

	return totalOps, namespaces, batchMaxTS, firstErr
}

func (m *CDCManager) startFlushWorkers() {
	workerCount := len(m.flushQueues)
	logging.PrintInfo(fmt.Sprintf("[CDC] Starting %d partition-aware write workers...", workerCount), 0)

	for i := 0; i < workerCount; i++ {
		m.workerWG.Add(1)
		go func(workerID int, queue <-chan map[string]*Batch) {
			defer m.workerWG.Done()

			for batch := range queue {
				start := time.Now()
				writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				// Capture max timestamp returned by handleBulkWrite
				flushedCount, namespaces, batchMaxTS, err := m.handleBulkWrite(writeCtx, batch)
				cancel()

				logging.LogCDCOp(start, flushedCount, namespaces, err)

				if err != nil {
					logging.PrintError(fmt.Sprintf("[CDC Worker %d] CRITICAL: Batch flush failed: %v", workerID, err), 0)
				} else {
					m.totalEventsApplied.Add(flushedCount)
					// Update status with the applied timestamp
					m.statusManager.UpdateAppliedStats(batchMaxTS)
				}
			}
		}(i, m.flushQueues[i])
	}
}

func (m *CDCManager) Start(ctx context.Context) error {
	logging.PrintInfo(fmt.Sprintf("Starting cluster-wide CDC... Resuming from checkpoint: %v", m.startAt), 0)

	// FIX: Reconcile stats on startup ---
	go func() {
		logging.PrintInfo("[CDC] Reconciling validation statistics...", 0)
		if err := m.validatorMgr.ReconcileStats(ctx); err != nil {
			logging.PrintWarning(fmt.Sprintf("[CDC] Stats reconciliation failed: %v", err), 0)
		} else {
			logging.PrintInfo("[CDC] Validation statistics reconciled.", 0)
		}
	}()

	m.startFlushWorkers()
	m.shutdownWG.Add(1)
	go m.processChanges(ctx)

	err := m.watchChanges(ctx)

	logging.PrintInfo("[CDC] Watcher stopped. Waiting for processor to finalize...", 0)
	m.shutdownWG.Wait()
	m.workerWG.Wait()

	if (m.lastSuccessfulTS != bson.Timestamp{}) {
		initialT0, _ := m.checkpoint.GetResumeTimestamp(context.Background(), m.checkpointDocID)
		if initialT0.T == 0 || m.lastSuccessfulTS.T > initialT0.T || (m.lastSuccessfulTS.T == initialT0.T && m.lastSuccessfulTS.I > initialT0.I) {
			saveCtx := context.Background()
			nextTS := bson.Timestamp{T: m.lastSuccessfulTS.T, I: m.lastSuccessfulTS.I + 1}
			m.checkpoint.SaveResumeTimestamp(saveCtx, m.checkpointDocID, nextTS)
		}
		m.statusManager.UpdateCDCStats(m.totalEventsApplied.Load(), m.lastSuccessfulTS)
		m.statusManager.Persist(context.Background())
	}
	return err
}

func (m *CDCManager) getWorkerIndex(docID interface{}) int {
	numWorkers := len(m.bulkWriters)
	if numWorkers == 1 {
		return 0
	}
	data, err := bson.Marshal(bson.D{{Key: "v", Value: docID}})
	if err != nil {
		data = []byte(fmt.Sprintf("%v", docID))
	}
	h := fnv.New32a()
	h.Write(data)
	return int(h.Sum32()) % numWorkers
}

// shouldSkip checks if the event belongs to an excluded database or collection
func (m *CDCManager) shouldSkip(event *ChangeEvent) bool {
	// 1. Check DB exclusion first (fastest check)
	if m.excludeDBs[event.Namespace.Database] {
		return true
	}

	// 2. Check Collection exclusion
	// We only format the string if the DB check passed, saving CPU.
	ns := fmt.Sprintf("%s.%s", event.Namespace.Database, event.Namespace.Collection)

	// Simply return the value from the map (true if excluded, false if not)
	return m.excludeColls[ns]
}

func (m *CDCManager) processChanges(ctx context.Context) {
	defer m.shutdownWG.Done()
	defer func() {
		for _, ch := range m.flushQueues {
			close(ch)
		}
	}()

	ticker := time.NewTicker(time.Duration(config.Cfg.CDC.BatchIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	// --- Auto-Retry Ticker (Every 60s) ---
	retryTicker := time.NewTicker(60 * time.Second)
	defer retryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			for i, writer := range m.bulkWriters {
				if finalBatch := writer.ExtractBatches(); len(finalBatch) > 0 {
					m.flushQueues[i] <- finalBatch
				}
			}
			return

		case event := <-m.eventQueue:
			// Filter Excluded Events
			m.lastSuccessfulTS = event.ClusterTime

			if m.shouldSkip(event) {
				continue
			}

			if m.isDDL(event) {
				m.handleDDL(ctx, event)
				m.totalEventsApplied.Add(1)
			} else {
				var docID interface{}
				if event.DocumentKey != nil {
					docID = event.DocumentKey["_id"]

					idStr := fmt.Sprintf("%v", docID)
					m.tracker.MarkDirty(idStr)

					// --- AUTO-HEAL ---
					// If record changes, invalidate previous validation status
					go func(ns, id string) {
						m.store.Invalidate(context.Background(), ns, id)
					}(event.Ns(), idStr)
				}

				workerIdx := m.getWorkerIndex(docID)
				if m.bulkWriters[workerIdx].AddEvent(event) {
					m.flushQueues[workerIdx] <- m.bulkWriters[workerIdx].ExtractBatches()
				}
			}

		case <-ticker.C:
			for i, writer := range m.bulkWriters {
				if batch := writer.ExtractBatches(); len(batch) > 0 {
					m.flushQueues[i] <- batch
				}
			}
			m.statusManager.UpdateCDCStats(m.totalEventsApplied.Load(), m.lastSuccessfulTS)
			// --- Persist status to DB regularly so external tools see "running"
			m.statusManager.Persist(context.Background())

		case <-retryTicker.C:
			// --- Auto-Retry Logic ---
			// If CDC is caught up (Lag ~ 0) and there are known failures, try to re-validate them.
			stats := m.statusManager.GetStats()
			if stats.CDCLagSeconds == 0 && stats.Validation.MismatchCount > 0 {
				logging.PrintInfo(fmt.Sprintf("[Auto-Retry] System is idle (Lag: 0s). Retrying %d known validation failures...", stats.Validation.MismatchCount), 0)
				go m.validatorMgr.RetryAllFailures(context.Background())
			}
		}
	}
}

func (m *CDCManager) watchChanges(ctx context.Context) error {
	streamOpts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetStartAtOperationTime(&m.startAt).
		SetMaxAwaitTime(time.Duration(config.Cfg.CDC.MaxAwaitTimeMS) * time.Millisecond)
	stream, err := m.sourceClient.Watch(ctx, mongo.Pipeline{}, streamOpts)
	if err != nil {
		return fmt.Errorf("failed to open change stream: %w", err)
	}
	defer stream.Close(ctx)
	for stream.Next(ctx) {
		var event ChangeEvent
		if err := stream.Decode(&event); err != nil {
			continue
		}
		m.eventQueue <- &event
	}
	return stream.Err()
}

func (m *CDCManager) isDDL(event *ChangeEvent) bool {
	switch event.OperationType {
	case Drop, Rename, DropDatabase, Create, CreateIndexes, DropIndexes:
		return true
	default:
		return false
	}
}

func (m *CDCManager) handleDDL(ctx context.Context, event *ChangeEvent) {
	ns := event.Ns()
	logging.PrintWarning(fmt.Sprintf("[%s] DDL Operation detected: %s. Flushing all partitions.", ns, event.OperationType), 0)
	saveCtx := context.Background()

	for i, writer := range m.bulkWriters {
		if batch := writer.ExtractBatches(); len(batch) > 0 {
			m.flushQueues[i] <- batch
		}
	}

	nextTS := bson.Timestamp{T: event.ClusterTime.T, I: event.ClusterTime.I + 1}
	m.checkpoint.SaveResumeTimestamp(saveCtx, m.checkpointDocID, nextTS)
	m.statusManager.UpdateCDCStats(m.totalEventsApplied.Load(), event.ClusterTime)
	m.statusManager.Persist(saveCtx)

	targetDB := m.targetClient.Database(event.Namespace.Database)
	targetColl := targetDB.Collection(event.Namespace.Collection)
	ddlCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch event.OperationType {
	case Drop:
		targetColl.Drop(ddlCtx)
	case DropDatabase:
		targetDB.Drop(ddlCtx)
	case Rename:
		renameCmd := bson.D{
			{Key: "renameCollection", Value: ns},
			{Key: "to", Value: fmt.Sprintf("%s.%s", event.To.Database, event.To.Collection)},
		}
		m.targetClient.Database("admin").RunCommand(ddlCtx, renameCmd)
	case Create:
		targetDB.CreateCollection(ddlCtx, event.Namespace.Collection)
	}
}
