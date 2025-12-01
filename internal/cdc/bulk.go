package cdc

import (
	"fmt"
	"strings"
	"sync"

	"github.com/Percona-Lab/docMongoStream/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Batch holds the data for a flush operation
// It contains the WriteModels for Mongo AND the IDs for the Validator
type Batch struct {
	Models []mongo.WriteModel
	IDs    []string
	LastTS bson.Timestamp // Max timestamp in this batch
}

// BulkWriter handles buffering changes
type BulkWriter struct {
	targetClient *mongo.Client
	batchSize    int
	// Map namespace -> Batch
	batches map[string]*Batch
	lock    sync.Mutex
}

// NewBulkWriter creates a new bulk writer
func NewBulkWriter(target *mongo.Client, batchSize int) *BulkWriter {
	return &BulkWriter{
		targetClient: target,
		batchSize:    batchSize,
		batches:      make(map[string]*Batch),
		lock:         sync.Mutex{},
	}
}

// AddEvent adds a change event to the buffer.
func (b *BulkWriter) AddEvent(event *ChangeEvent) bool {
	var model mongo.WriteModel

	// Extract ID string safely for validation later
	var docID string
	if val, ok := event.DocumentKey["_id"]; ok {
		if oid, ok := val.(bson.ObjectID); ok {
			docID = oid.Hex()
		} else {
			docID = fmt.Sprintf("%v", val)
		}
	}

	switch event.OperationType {
	case Insert:
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
		logging.PrintWarning(fmt.Sprintf("Unsupported operation type: %s. Skipping.", event.OperationType), 0)
		return false
	}

	ns := event.Ns()
	b.lock.Lock()
	if b.batches[ns] == nil {
		b.batches[ns] = &Batch{
			Models: []mongo.WriteModel{},
			IDs:    []string{},
		}
	}

	b.batches[ns].Models = append(b.batches[ns].Models, model)
	if docID != "" {
		b.batches[ns].IDs = append(b.batches[ns].IDs, docID)
	}

	// --- Track the latest timestamp in this batch ---
	if event.ClusterTime.T > b.batches[ns].LastTS.T ||
		(event.ClusterTime.T == b.batches[ns].LastTS.T && event.ClusterTime.I > b.batches[ns].LastTS.I) {
		b.batches[ns].LastTS = event.ClusterTime
	}

	size := len(b.batches[ns].Models)
	b.lock.Unlock()

	return size >= b.batchSize
}

// ExtractBatches clears the buffer and returns all buffered batches
// This replaces the old Flush() method, moving the write logic to the Manager
func (b *BulkWriter) ExtractBatches() map[string]*Batch {
	b.lock.Lock()
	defer b.lock.Unlock()

	if len(b.batches) == 0 {
		return nil
	}

	currentBatches := b.batches
	b.batches = make(map[string]*Batch) // Reset

	return currentBatches
}

// splitNamespace helper
func splitNamespace(ns string) (string, string) {
	parts := strings.SplitN(ns, ".", 2)
	if len(parts) < 2 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}
