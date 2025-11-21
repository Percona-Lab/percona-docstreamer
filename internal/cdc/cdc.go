package cdc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Percona-Lab/docMongoStream/internal/checkpoint"
	"github.com/Percona-Lab/docMongoStream/internal/config"
	"github.com/Percona-Lab/docMongoStream/internal/logging"
	"github.com/Percona-Lab/docMongoStream/internal/status"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CDCManager manages the change stream watcher and batch processor
type CDCManager struct {
	sourceClient       *mongo.Client
	targetClient       *mongo.Client
	eventQueue         chan *ChangeEvent
	flushQueue         chan map[string][]mongo.WriteModel // Channel for batches to be processed
	bulkWriter         *BulkWriter
	startAt            primitive.Timestamp // This holds the loaded T0
	lastSuccessfulTS   primitive.Timestamp
	checkpoint         *checkpoint.Manager
	statusManager      *status.Manager
	shutdownWG         sync.WaitGroup
	workerWG           sync.WaitGroup // Separate WaitGroup for flush workers
	totalEventsApplied atomic.Int64   // Atomic to prevent race conditions
	checkpointDocID    string
}

// NewManager creates a new CDC manager
func NewManager(source, target *mongo.Client, checkpointDocID string, startAt primitive.Timestamp, checkpoint *checkpoint.Manager, statusMgr *status.Manager) *CDCManager {
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
	mgr := &CDCManager{
		sourceClient:     source,
		targetClient:     target,
		eventQueue:       make(chan *ChangeEvent, config.Cfg.CDC.BatchSize*2),
		flushQueue:       make(chan map[string][]mongo.WriteModel, config.Cfg.Migration.MaxConcurrentWorkers), // Buffered channel
		bulkWriter:       NewBulkWriter(target, config.Cfg.CDC.BatchSize),
		startAt:          resumeTS, // Use the loaded checkpoint (T0)
		lastSuccessfulTS: resumeTS,
		checkpoint:       checkpoint,
		statusManager:    statusMgr,
		shutdownWG:       sync.WaitGroup{},
		workerWG:         sync.WaitGroup{}, // Initialize worker WG
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
		opts := options.BulkWrite().SetOrdered(false)
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

// startFlushWorkers launches the concurrent workers
func (m *CDCManager) startFlushWorkers() {
	workerCount := config.Cfg.CDC.MaxWriteWorkers
	logging.PrintInfo(fmt.Sprintf("[CDC] Starting %d concurrent write workers...", workerCount), 0)

	for i := 0; i < workerCount; i++ {
		m.workerWG.Add(1)
		go func(workerID int) {
			defer m.workerWG.Done()

			// Drain the channel until it is closed. Do NOT stop on ctx.Done().
			// This ensures all pending batches (especially the final one) are processed.
			for batch := range m.flushQueue {
				start := time.Now()

				// Use a Background context for the write.
				// The main 'ctx' might be cancelled already (shutdown), but we must finish writing.
				writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				flushedCount, namespaces, err := m.handleBulkWrite(writeCtx, batch)
				cancel()

				logging.LogCDCOp(start, flushedCount, namespaces, err)

				if err != nil {
					logging.PrintError(fmt.Sprintf("[CDC] CRITICAL: Worker batch flush failed: %v", err), 0)
				} else {
					// ATOMIC UPDATE
					m.totalEventsApplied.Add(flushedCount)
				}
			}
			logging.PrintInfo(fmt.Sprintf("[Worker %d] Shutting down.", workerID), 0)
		}(i)
	}
}

// Start begins the CDC process. This is a blocking call.
func (m *CDCManager) Start(ctx context.Context) error {
	logging.PrintInfo(fmt.Sprintf("Starting cluster-wide CDC... Resuming from checkpoint: %v", m.startAt), 0)

	m.startFlushWorkers() // Start the parallel workers (no ctx passed, they rely on channel close)

	m.shutdownWG.Add(1)
	go m.processChanges(ctx)

	err := m.watchChanges(ctx)

	logging.PrintInfo("[CDC] Watcher stopped. Waiting for processor to finalize...", 0)
	m.shutdownWG.Wait() // Wait for processChanges to finish flushing to channel

	// processChanges has closed the channel. Now wait for workers to drain it.
	m.workerWG.Wait()

	// Save the checkpoint HERE, only after all workers have successfully finished.
	if (m.lastSuccessfulTS != primitive.Timestamp{}) {
		initialT0, _ := m.checkpoint.GetResumeTimestamp(context.Background(), m.checkpointDocID)
		if initialT0.T == 0 || m.lastSuccessfulTS.T > initialT0.T || (m.lastSuccessfulTS.T == initialT0.T && m.lastSuccessfulTS.I > initialT0.I) {
			logging.PrintInfo("[CDC] Saving final resume timestamp...", 0)
			saveCtx := context.Background()
			nextTS := primitive.Timestamp{T: m.lastSuccessfulTS.T, I: m.lastSuccessfulTS.I + 1}
			m.checkpoint.SaveResumeTimestamp(saveCtx, m.checkpointDocID, nextTS)
		}
		m.statusManager.UpdateCDCStats(m.totalEventsApplied.Load(), m.lastSuccessfulTS)
		m.statusManager.Persist(context.Background())
	}

	logging.PrintInfo("[CDC] Processor finalized. Shutdown complete.", 0)
	return err
}

// processChanges reads from the eventQueue and applies changes to the target
func (m *CDCManager) processChanges(ctx context.Context) {
	defer m.shutdownWG.Done()

	// Ensure flushQueue is closed when this function exits, causing workers to stop.
	defer close(m.flushQueue)

	ticker := time.NewTicker(time.Duration(config.Cfg.CDC.BatchIntervalMS) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logging.PrintInfo("[CDC] Processor shutting down. Flushing final batch...", 0)
			// Flush any remaining events in the buffer
			if finalBatch := m.bulkWriter.ExtractBatches(); len(finalBatch) > 0 {
				m.flushQueue <- finalBatch
			}
			// DO NOT save checkpoint here. We must wait for workers (in Start)
			return
		case event := <-m.eventQueue:
			m.lastSuccessfulTS = event.ClusterTime
			if m.isDDL(event) {
				m.handleDDL(ctx, event)
				m.totalEventsApplied.Add(1)
			} else {
				if m.bulkWriter.AddEvent(event) {
					m.flushQueue <- m.bulkWriter.ExtractBatches()
				}
			}
		case <-ticker.C:
			if batch := m.bulkWriter.ExtractBatches(); len(batch) > 0 {
				m.flushQueue <- batch
			}
			// Update status periodically so metrics are live
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
	logging.PrintWarning(fmt.Sprintf("[%s] DDL Operation detected: %s. Flushing batch before applying.", ns, event.OperationType), 0)
	saveCtx := context.Background()

	// Flush pending data first
	if batch := m.bulkWriter.ExtractBatches(); len(batch) > 0 {
		m.flushQueue <- batch
	}

	// TODO: In a perfect world, we'd wait for workers to drain here too,
	// but DDL consistency is complex. For now, we save checkpoint immediately.
	nextTS := primitive.Timestamp{T: event.ClusterTime.T, I: event.ClusterTime.I + 1}
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
