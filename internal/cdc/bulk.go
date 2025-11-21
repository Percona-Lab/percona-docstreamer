package cdc

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Percona-Lab/docMongoStream/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// BulkWriter handles buffering and applying changes to the target
type BulkWriter struct {
	targetClient *mongo.Client
	batchSize    int
	models       map[string][]mongo.WriteModel
	// mutex to prevent race conditions when extracting batches
	lock sync.Mutex
}

// NewBulkWriter creates a new bulk writer
func NewBulkWriter(target *mongo.Client, batchSize int) *BulkWriter {
	return &BulkWriter{
		targetClient: target,
		batchSize:    batchSize,
		models:       make(map[string][]mongo.WriteModel),
		lock:         sync.Mutex{}, // Initialize the mutex
	}
}

// AddEvent adds a change event to the buffer.
// It returns true if the buffer is full (at batchSize)
func (b *BulkWriter) AddEvent(event *ChangeEvent) bool {
	var model mongo.WriteModel

	switch event.OperationType {
	case Insert:
		// Use ReplaceOne with Upsert instead of InsertOne.
		// Since CDC overlaps with Full Load, we will encounter duplicates.
		// Upserting ensures we handle them gracefully without errors.
		model = mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: event.DocumentKey["_id"]}}).
			SetReplacement(event.FullDocument).
			SetUpsert(true)
	case Update, Replace:
		model = mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: event.DocumentKey["_id"]}}).
			SetReplacement(event.FullDocument).
			SetUpsert(true)
	case Delete:
		model = mongo.NewDeleteOneModel().
			SetFilter(bson.D{{Key: "_id", Value: event.DocumentKey["_id"]}})
	default:
		// We should never get here if isDDL() is working
		logging.PrintWarning(fmt.Sprintf("Unsupported operation type: %s. Skipping.", event.OperationType), 0)
		return false
	}

	ns := event.Ns()
	b.models[ns] = append(b.models[ns], model)
	return len(b.models[ns]) >= b.batchSize
}

// ExtractBatches clears the buffer and returns all buffered batches
// indexed by namespace.
func (b *BulkWriter) ExtractBatches() map[string][]mongo.WriteModel {
	b.lock.Lock()
	defer b.lock.Unlock()

	// 1. Check if empty
	if len(b.models) == 0 {
		return nil
	}

	// 2. Grab the current map
	batches := b.models

	// 3. Reset the internal map (ready for new events)
	b.models = make(map[string][]mongo.WriteModel)

	return batches
}

// Flush applies all buffered operations to the target database
func (b *BulkWriter) Flush(ctx context.Context) (int64, []string, error) {
	if len(b.models) == 0 {
		return 0, nil, nil // Nothing to flush
	}

	var totalOps int64
	var namespaces []string
	var firstErr error // Track the first error

	// We must flush namespace by namespace
	for ns, models := range b.models {
		if len(models) == 0 {
			delete(b.models, ns) // Clean up empty/processed batch
			continue
		}

		db, coll := splitNamespace(ns)
		if coll == "" {
			logging.PrintError(fmt.Sprintf("[%s] Invalid namespace, cannot split. Skipping batch.", ns), 0)
			delete(b.models, ns) // Delete bad batch to prevent retry loop
			if firstErr == nil {
				firstErr = fmt.Errorf("invalid namespace: %s", ns)
			}
			continue
		}
		targetColl := b.targetClient.Database(db).Collection(coll)

		// Set Unordered to allow parallel processing
		opts := options.BulkWrite().SetOrdered(false)

		result, err := targetColl.BulkWrite(ctx, models, opts)
		if err != nil {
			// Log the error but try other namespaces
			logging.PrintError(fmt.Sprintf("[%s] BulkWrite failed: %v", ns, err), 0)
			// Check for specific "write results" if the main error is vague
			if wErr, ok := err.(mongo.BulkWriteException); ok {
				logging.PrintError(fmt.Sprintf("[%s] ... %d write errors", ns, len(wErr.WriteErrors)), 0)
			}

			// Capture the first error
			if firstErr == nil {
				firstErr = err
			}
			// DO NOT delete the batch, it will be retried on next flush
			continue
		}

		// This batch succeeded, clear it from the map
		delete(b.models, ns)

		// Calculate ops
		totalOps += result.InsertedCount + result.ModifiedCount + result.UpsertedCount + result.DeletedCount
		namespaces = append(namespaces, ns)
	}

	return totalOps, namespaces, firstErr
}

// splitNamespace helper
func splitNamespace(ns string) (string, string) {
	parts := strings.SplitN(ns, ".", 2)
	if len(parts) < 2 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}
