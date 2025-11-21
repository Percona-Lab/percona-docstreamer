package checkpoint

import (
	"context"
	"fmt"
	"time"

	"github.com/Percona-Lab/docMongoStream/internal/config"
	"github.com/Percona-Lab/docMongoStream/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// checkpointDoc is the structure we save in MongoDB
type checkpointDoc struct {
	ID          string         `bson:"_id"`
	Timestamp   bson.Timestamp `bson:"timestamp"`
	LastUpdated time.Time      `bson:"lastUpdated"`
}

// Manager saves and loads migration checkpoints
type Manager struct {
	coll *mongo.Collection
}

// NewManager creates a new checkpoint manager
func NewManager(targetClient *mongo.Client) *Manager {
	dbName := config.Cfg.Migration.MetadataDB
	collName := config.Cfg.Migration.CheckpointCollection

	coll := targetClient.Database(dbName).Collection(collName)
	logging.PrintInfo(fmt.Sprintf("Checkpoint manager initialized (collection: %s.%s)", dbName, collName), 0)

	return &Manager{
		coll: coll,
	}
}

// GetResumeTimestamp loads the last saved CDC timestamp from the database.
func (m *Manager) GetResumeTimestamp(ctx context.Context, docID string) (bson.Timestamp, bool) {
	var doc checkpointDoc
	err := m.coll.FindOne(ctx, bson.D{{Key: "_id", Value: docID}}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			logging.PrintInfo(fmt.Sprintf("[CDC %s] No resume timestamp found in checkpoint database.", docID), 0)
			return bson.Timestamp{}, false
		}
		return bson.Timestamp{}, false
	}

	logging.PrintInfo(fmt.Sprintf("[CDC %s] Found resume timestamp: %v", docID, doc.Timestamp), 0)
	return doc.Timestamp, true
}

// SaveResumeTimestamp saves the latest processed timestamp to the database.
func (m *Manager) SaveResumeTimestamp(ctx context.Context, docID string, ts bson.Timestamp) {
	doc := checkpointDoc{
		ID:          docID,
		Timestamp:   ts,
		LastUpdated: time.Now().UTC(),
	}

	opts := options.Replace().SetUpsert(true)
	_, err := m.coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: docID}}, doc, opts)
	if err != nil {
		logging.PrintError(fmt.Sprintf("[CDC %s] CRITICAL: Failed to save resume timestamp: %v", docID, err), 0)
	}
}

// SaveCollectionCheckpoint saves the final Full Load completion timestamp for a single collection.
// This uses the collection namespace (e.g., "dbname.collname") as the unique document ID.
func (m *Manager) SaveCollectionCheckpoint(ctx context.Context, ns string, ts bson.Timestamp) {
	doc := checkpointDoc{
		// Use the namespace as the document ID for per-collection tracking
		ID:          ns,
		Timestamp:   ts,
		LastUpdated: time.Now().UTC(),
	}

	opts := options.Replace().SetUpsert(true)
	_, err := m.coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: ns}}, doc, opts)
	if err != nil {
		logging.PrintError(fmt.Sprintf("[FULL LOAD %s] CRITICAL: Failed to save collection checkpoint: %v", ns, err), 0)
	} else {
		logging.PrintInfo(fmt.Sprintf("[FULL LOAD %s] Checkpoint saved: %v", ns, ts), 0)
	}
}

// GetLatestCollectionCheckpoint finds the latest completion timestamp across all collection checkpoints.
func (m *Manager) GetLatestCollectionCheckpoint(ctx context.Context) (bson.Timestamp, bool) {
	var latestTS bson.Timestamp

	// Exclude the global CDC resume timestamp itself from this calculation
	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$ne", Value: config.Cfg.Migration.CheckpointDocID}}}}

	opts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: -1}}).SetLimit(1)

	cursor, err := m.coll.Find(ctx, filter, opts)
	if err != nil {
		logging.PrintWarning(fmt.Sprintf("[FULL LOAD] Failed to list collection checkpoints: %v", err), 0)
		return bson.Timestamp{}, false
	}
	defer cursor.Close(ctx)

	if cursor.Next(ctx) {
		var doc checkpointDoc
		if err := cursor.Decode(&doc); err == nil {
			latestTS = doc.Timestamp
			return latestTS, true
		}
	}

	return bson.Timestamp{}, false
}
