package cdc

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Percona-Lab/docMongoStream/internal/checkpoint"
	"github.com/Percona-Lab/docMongoStream/internal/config"
	"github.com/Percona-Lab/docMongoStream/internal/logging"
	"github.com/Percona-Lab/docMongoStream/internal/status"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// CDCManager manages the change stream watcher and batch processor
type CDCManager struct {
	sourceClient *mongo.Client
	targetClient *mongo.Client
	eventQueue   chan *ChangeEvent
	// Key-Based Partitioning: We now have a slice of queues and writers (one pair per worker)
	flushQueues        []chan map[string][]mongo.WriteModel
	bulkWriters        []*BulkWriter
	startAt            bson.Timestamp
	lastSuccessfulTS   bson.Timestamp
	checkpoint         *checkpoint.Manager
	statusManager      *status.Manager
	shutdownWG         sync.WaitGroup
	workerWG           sync.WaitGroup // Separate WaitGroup for flush workers
	totalEventsApplied atomic.Int64   // Atomic to prevent race conditions
	checkpointDocID    string
}

// NewManager creates a new CDC manager
func NewManager(source, target *mongo.Client, checkpointDocID string, startAt bson.Timestamp, checkpoint *checkpoint.Manager, statusMgr *status.Manager) *CDCManager {
	// 1. Load the actual resume timestamp (T0) from the checkpoint manager
	resumeTS, found := checkpoint.GetResumeTimestamp(context.Background(), checkpointDocID)

	if !found {
		logging.PrintWarning(fmt.Sprintf("[CDC %s] Checkpoint not found. Falling back to provided start time: %v", checkpointDocID, startAt), 0)
		resumeTS = startAt
	} else {
		logging.PrintInfo(fmt.Sprintf("[CDC %s] Loaded checkpoint. Resuming from %v", checkpointDocID, resumeTS), 0)
	}

	// Read the event count from the status manager *after* LoadAndMerge
	initialEvents := statusMgr.GetEventsApplied()
	if initialEvents > 0 {
		logging.PrintInfo(fmt.Sprintf("[CDC] Resuming event count from %d", initialEvents), 0)
	}

	// Initialize Key-Based Partitioning structures
	workerCount := config.Cfg.CDC.MaxWriteWorkers
	if workerCount < 1 {
		workerCount = 1
	}

	queues := make([]chan map[string][]mongo.WriteModel, workerCount)
	writers := make([]*BulkWriter, workerCount)

	for i := 0; i < workerCount; i++ {
		// Each worker gets its own buffered channel to prevent blocking the processor
		queues[i] = make(chan map[string][]mongo.WriteModel, config.Cfg.Migration.MaxConcurrentWorkers)
		writers[i] = NewBulkWriter(target, config.Cfg.CDC.BatchSize)
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
		shutdownWG:       sync.WaitGroup{},
		workerWG:         sync.WaitGroup{},
		checkpointDocID:  checkpointDocID,
	}
	mgr.totalEventsApplied.Store(initialEvents)
	return mgr
}

// handleBulkWrite is the worker function executed concurrently
func (m *CDCManager) handleBulkWrite(ctx context.Context, batch map[string][]mongo.WriteModel) (int64, []string, error) {
	// We must process namespace by namespace
	var totalOps int64
	var namespaces []string
	var firstErr error

	for ns, models := range batch {
		if len(models) == 0 {
			continue
		}

		db, coll := splitNamespace(ns)
		if coll == "" {
			logging.PrintError(fmt.Sprintf("[%s] Invalid namespace, cannot split. Skipping batch.", ns), 0)
			if firstErr == nil {
				firstErr = fmt.Errorf("invalid namespace: %s", ns)
			}
			continue
		}

		targetColl := m.targetClient.Database(db).Collection(coll)

		// CRITICAL CHANGE for Partitioning: SetOrdered(true)
		// Since we are now partitioning by Key, a single worker might receive multiple
		// updates for the SAME document in one batch. We must apply them in order.
		opts := options.BulkWrite().SetOrdered(true)

		result, err := targetColl.BulkWrite(ctx, models, opts)

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
	}

	return totalOps, namespaces, firstErr
}

// startFlushWorkers launches the concurrent workers, each attached to a specific queue
func (m *CDCManager) startFlushWorkers() {
	workerCount := len(m.flushQueues)
	logging.PrintInfo(fmt.Sprintf("[CDC] Starting %d partition-aware write workers...", workerCount), 0)

	for i := 0; i < workerCount; i++ {
		m.workerWG.Add(1)
		// Launch worker specifically for queue [i]
		go func(workerID int, queue <-chan map[string][]mongo.WriteModel) {
			defer m.workerWG.Done()

			for batch := range queue {
				start := time.Now()
				writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				flushedCount, namespaces, err := m.handleBulkWrite(writeCtx, batch)
				cancel()

				logging.LogCDCOp(start, flushedCount, namespaces, err)

				if err != nil {
					logging.PrintError(fmt.Sprintf("[CDC Worker %d] CRITICAL: Batch flush failed: %v", workerID, err), 0)
				} else {
					m.totalEventsApplied.Add(flushedCount)
				}
			}
			logging.PrintInfo(fmt.Sprintf("[Worker %d] Shutting down.", workerID), 0)
		}(i, m.flushQueues[i])
	}
}

// Start begins the CDC process. This is a blocking call.
func (m *CDCManager) Start(ctx context.Context) error {
	logging.PrintInfo(fmt.Sprintf("Starting cluster-wide CDC... Resuming from checkpoint: %v", m.startAt), 0)

	m.startFlushWorkers()

	m.shutdownWG.Add(1)
	go m.processChanges(ctx)

	err := m.watchChanges(ctx)

	logging.PrintInfo("[CDC] Watcher stopped. Waiting for processor to finalize...", 0)
	m.shutdownWG.Wait()

	m.workerWG.Wait()

	// Save the checkpoint here, but only after all workers have successfully finished.
	if (m.lastSuccessfulTS != bson.Timestamp{}) {
		initialT0, _ := m.checkpoint.GetResumeTimestamp(context.Background(), m.checkpointDocID)
		if initialT0.T == 0 || m.lastSuccessfulTS.T > initialT0.T || (m.lastSuccessfulTS.T == initialT0.T && m.lastSuccessfulTS.I > initialT0.I) {
			logging.PrintInfo("[CDC] Saving final resume timestamp...", 0)
			saveCtx := context.Background()
			nextTS := bson.Timestamp{T: m.lastSuccessfulTS.T, I: m.lastSuccessfulTS.I + 1}
			m.checkpoint.SaveResumeTimestamp(saveCtx, m.checkpointDocID, nextTS)
		}
		m.statusManager.UpdateCDCStats(m.totalEventsApplied.Load(), m.lastSuccessfulTS)
		m.statusManager.Persist(context.Background())
	}

	logging.PrintInfo("[CDC] Processor finalized. Shutdown complete.", 0)
	return err
}

// getWorkerIndex calculates the consistent hash for a document ID
func (m *CDCManager) getWorkerIndex(docID interface{}) int {
	numWorkers := len(m.bulkWriters)
	if numWorkers == 1 {
		return 0
	}

	// Marshal the ID to bytes to ensure consistent representation (handles ObjectId, string, int, etc.)
	// We wrap it in a D struct to ensure valid BSON serialization
	data, err := bson.Marshal(bson.D{{"v", docID}})
	if err != nil {
		// Fallback for extreme edge cases
		logging.PrintWarning(fmt.Sprintf("[CDC] Failed to marshal _id for hashing: %v. Using string fallback.", err), 0)
		data = []byte(fmt.Sprintf("%v", docID))
	}

	h := fnv.New32a()
	h.Write(data)
	return int(h.Sum32()) % numWorkers
}

// processChanges reads from the eventQueue, routes to buffers, and flushes to workers
func (m *CDCManager) processChanges(ctx context.Context) {
	defer m.shutdownWG.Done()

	// Ensure ALL flushQueues are closed when this function exits
	defer func() {
		for _, ch := range m.flushQueues {
			close(ch)
		}
	}()

	ticker := time.NewTicker(time.Duration(config.Cfg.CDC.BatchIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logging.PrintInfo("[CDC] Processor shutting down. Flushing all partitions...", 0)
			// Flush all writers to their respective queues
			for i, writer := range m.bulkWriters {
				if finalBatch := writer.ExtractBatches(); len(finalBatch) > 0 {
					m.flushQueues[i] <- finalBatch
				}
			}
			return

		case event := <-m.eventQueue:
			m.lastSuccessfulTS = event.ClusterTime

			if m.isDDL(event) {
				m.handleDDL(ctx, event)
				m.totalEventsApplied.Add(1)
			} else {
				// ROUTING LOGIC: Determine which worker owns this document
				// If DocumentKey is nil (shouldn't happen for CRUD), default to 0
				var docID interface{}
				if event.DocumentKey != nil {
					docID = event.DocumentKey["_id"]
				}

				workerIdx := m.getWorkerIndex(docID)

				// Add to the assigned buffer
				if m.bulkWriters[workerIdx].AddEvent(event) {
					// If buffer is full, flush ONLY this worker's buffer to its queue
					m.flushQueues[workerIdx] <- m.bulkWriters[workerIdx].ExtractBatches()
				}
			}

		case <-ticker.C:
			// Periodic flush for ALL partitions to keep latency low
			for i, writer := range m.bulkWriters {
				if batch := writer.ExtractBatches(); len(batch) > 0 {
					m.flushQueues[i] <- batch
				}
			}
			// Update status periodically
			m.statusManager.UpdateCDCStats(m.totalEventsApplied.Load(), m.lastSuccessfulTS)
		}
	}
}

// watchChanges opens a change stream and feeds the eventQueue
func (m *CDCManager) watchChanges(ctx context.Context) error {
	logging.PrintStep("[CDC] Starting cluster-wide change stream watcher...", 0)
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
			logging.PrintWarning(fmt.Sprintf("[CDC] Failed to decode change event: %v", err), 0)
			continue
		}
		m.eventQueue <- &event
	}
	return stream.Err()
}

// isDDL checks if an event is a schema change
func (m *CDCManager) isDDL(event *ChangeEvent) bool {
	switch event.OperationType {
	case Drop, Rename, DropDatabase, Create, CreateIndexes, DropIndexes:
		return true
	default:
		return false
	}
}

// handleDDL flushes any pending writes and applies the DDL operation
func (m *CDCManager) handleDDL(ctx context.Context, event *ChangeEvent) {
	ns := event.Ns()
	logging.PrintWarning(fmt.Sprintf("[%s] DDL Operation detected: %s. Flushing all partitions before applying.", ns, event.OperationType), 0)
	saveCtx := context.Background()

	// Flush ALL pending data across ALL partitions
	for i, writer := range m.bulkWriters {
		if batch := writer.ExtractBatches(); len(batch) > 0 {
			m.flushQueues[i] <- batch
		}
	}

	nextTS := bson.Timestamp{T: event.ClusterTime.T, I: event.ClusterTime.I + 1}
	m.checkpoint.SaveResumeTimestamp(saveCtx, m.checkpointDocID, nextTS)
	m.statusManager.UpdateCDCStats(m.totalEventsApplied.Load(), event.ClusterTime)
	m.statusManager.Persist(saveCtx)

	// Apply the DDL operation
	targetDB := m.targetClient.Database(event.Namespace.Database)
	targetColl := targetDB.Collection(event.Namespace.Collection)
	ddlCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch event.OperationType {
	case Drop:
		logging.PrintStep(fmt.Sprintf("[%s] Applying DDL: Dropping collection", ns), 0)
		if err := targetColl.Drop(ddlCtx); err != nil {
			logging.PrintError(fmt.Sprintf("[%s] DDL Drop failed: %v", ns, err), 0)
		}
	case DropDatabase:
		logging.PrintStep(fmt.Sprintf("[%s] Applying DDL: Dropping database", event.Namespace.Database), 0)
		if err := targetDB.Drop(ddlCtx); err != nil {
			logging.PrintError(fmt.Sprintf("[%s] DDL DropDatabase failed: %v", event.Namespace.Database, err), 0)
		}
	case Rename:
		logging.PrintStep(fmt.Sprintf("[%s] Applying DDL: Renaming to %s.%s", ns, event.To.Database, event.To.Collection), 0)
		renameCmd := bson.D{
			{Key: "renameCollection", Value: ns},
			{Key: "to", Value: fmt.Sprintf("%s.%s", event.To.Database, event.To.Collection)},
		}
		if err := m.targetClient.Database("admin").RunCommand(ddlCtx, renameCmd).Err(); err != nil {
			logging.PrintError(fmt.Sprintf("[%s] DDL Rename failed: %v", ns, err), 0)
		}
	case Create:
		logging.PrintStep(fmt.Sprintf("[%s] Applying DDL: Creating collection", ns), 0)
		if err := targetDB.CreateCollection(ddlCtx, event.Namespace.Collection); err != nil {
			logging.PrintError(fmt.Sprintf("[%s] DDL Create failed: %v", ns, err), 0)
		}
	case CreateIndexes, DropIndexes:
		logging.PrintWarning(fmt.Sprintf("[%s] DDL operation '%s' is not yet supported, skipping.", ns, event.OperationType), 0)
	}
}
