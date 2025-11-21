package discover

import (
	"context"
	"fmt"

	"github.com/Percona-Lab/docMongoStream/internal/config"
	"github.com/Percona-Lab/docMongoStream/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// IndexInfo holds information about a single index
type IndexInfo struct {
	Name   string
	Key    bson.D
	Unique bool
}

// CollectionInfo holds metadata about a collection to be migrated
type CollectionInfo struct {
	Namespace string
	DB        string
	Coll      string
	Count     int64
	Indexes   []IndexInfo
	Size      int64 // Size in bytes
}

// indexDecoded is a helper to correctly decode index definitions,
// specifically ensuring compound index keys are decoded as bson.D
type indexDecoded struct {
	Name                    string      `bson:"name"`
	Key                     bson.D      `bson:"key"`
	Unique                  bool        `bson:"unique,omitempty"`
	ExpireAfterSeconds      *int32      `bson:"expireAfterSeconds,omitempty"`
	PartialFilterExpression interface{} `bson:"partialFilterExpression,omitempty"`
}

// DiscoverCollections scans the source and finds all collections to migrate
func DiscoverCollections(ctx context.Context, client *mongo.Client) ([]CollectionInfo, error) {
	logging.PrintStep("Discovering databases and collections...", 0)

	// Build the exclude map for fast lookups
	excludeDBs := make(map[string]bool)
	for _, db := range config.Cfg.Migration.ExcludeDBs {
		excludeDBs[db] = true
	}

	// --- Build the exclude map for collections ---
	excludeColls := make(map[string]bool)
	for _, ns := range config.Cfg.Migration.ExcludeCollections {
		excludeColls[ns] = true
	}

	dbNames, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}

	var collections []CollectionInfo
	var totalCollections int64

	for _, dbName := range dbNames {
		if _, ok := excludeDBs[dbName]; ok {
			logging.PrintInfo(fmt.Sprintf("Skipping DB: %s (excluded by configuration)", dbName), 0)
			continue // Skip excluded databases
		}

		logging.PrintStep(fmt.Sprintf("Scanning DB: %s", dbName), 0)
		db := client.Database(dbName)
		// --- Pass excludeColls map ---
		collInfos, err := discoverCollectionsInDB(ctx, db, excludeColls)
		if err != nil {
			logging.PrintWarning(fmt.Sprintf("Failed to scan DB %s: %v", dbName, err), 0)
			continue
		}
		totalCollections += int64(len(collInfos))
		collections = append(collections, collInfos...)
	}

	logging.PrintSuccess(fmt.Sprintf("Discovered %d total collections to migrate.", totalCollections), 0)
	return collections, nil
}

// --- Accept excludeColls map ---
// discoverCollectionsInDB lists all non-system collections in a given database
func discoverCollectionsInDB(ctx context.Context, db *mongo.Database, excludeColls map[string]bool) ([]CollectionInfo, error) {
	// Filter for only user collections
	filter := bson.D{{Key: "name", Value: bson.D{{Key: "$not", Value: bson.M{"$regex": "^system\\."}}}}}
	collNames, err := db.ListCollectionNames(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to list collections for %s: %w", db.Name(), err)
	}

	var collections []CollectionInfo
	for _, collName := range collNames {
		// Oplog is a special case. Here just in case, but documentDB doesn't have this
		if db.Name() == "local" && collName == "oplog.rs" {
			continue
		}

		// --- Check for collection exclusion ---
		namespace := fmt.Sprintf("%s.%s", db.Name(), collName)
		if _, ok := excludeColls[namespace]; ok {
			logging.PrintInfo(fmt.Sprintf("Skipping collection: %s (excluded by configuration)", namespace), 0)
			continue // Skip this collection
		}

		info, err := getCollectionInfo(ctx, db, collName)
		if err != nil {
			logging.PrintWarning(fmt.Sprintf("Failed to get info for %s.%s: %v", db.Name(), collName, err), 0)
			continue
		}

		logging.PrintInfo(fmt.Sprintf("- Found: %s (%d documents, %d indexes)",
			info.Namespace, info.Count, len(info.Indexes)), 0)
		collections = append(collections, info)
	}
	return collections, nil
}

// getCollectionInfo gets document count and index definitions for a collection
func getCollectionInfo(ctx context.Context, db *mongo.Database, collName string) (CollectionInfo, error) {
	coll := db.Collection(collName)

	// 1. Get Document Count
	count, err := coll.EstimatedDocumentCount(ctx)
	if err != nil {
		return CollectionInfo{}, fmt.Errorf("failed to count docs for %s: %w", collName, err)
	}

	// --- Get Collection Size ---
	var collStats struct {
		Size int64 `bson:"size"`
	}
	// Use RunCommand for collStats
	err = db.RunCommand(ctx, bson.D{{Key: "collStats", Value: collName}}).Decode(&collStats)
	if err != nil {
		// Fallback for certain environments, log warning and continue
		logging.PrintWarning(fmt.Sprintf("Could not get collStats for %s: %v. Size will be reported as 0.", collName, err), 0)
		collStats.Size = 0 // Set size to 0 and continue
	}

	// 2. Get Indexes
	var indexes []IndexInfo
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return CollectionInfo{}, fmt.Errorf("failed to list indexes for %s: %w", collName, err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		// Decode into a dedicated struct to ensure the 'key' is correctly interpreted as an ordered bson.D,
		// preserving the field order for compound indexes.
		var indexDoc indexDecoded
		if err := cursor.Decode(&indexDoc); err != nil {
			return CollectionInfo{}, fmt.Errorf("failed to decode index: %w", err)
		}

		// Don't copy the _id index
		if indexDoc.Name == "_id_" {
			continue
		}

		// Skip index if key is empty (should not happen for valid non-_id index)
		if len(indexDoc.Key) == 0 {
			logging.PrintWarning(fmt.Sprintf("Skipping index '%s' on %s.%s: index key is empty or could not be decoded as bson.D.", indexDoc.Name, db.Name(), collName), 0)
			continue
		}

		// Filter out TTL indexes
		if indexDoc.ExpireAfterSeconds != nil {
			logging.PrintWarning(fmt.Sprintf("Skipping TTL index '%s' on %s.%s", indexDoc.Name, db.Name(), collName), 0)
			continue
		}

		// Filter out partial indexes
		if indexDoc.PartialFilterExpression != nil {
			// A non-nil value means the partialFilterExpression field was present, indicating a partial index.
			logging.PrintWarning(fmt.Sprintf("Skipping partial index '%s' on %s.%s", indexDoc.Name, db.Name(), collName), 0)
			continue
		}

		// Filter out text indexes: text indexes have an explicit key value of "text"
		isTextIndex := false
		for _, e := range indexDoc.Key {
			if e.Value == "text" {
				isTextIndex = true
				break
			}
		}
		if isTextIndex {
			logging.PrintWarning(fmt.Sprintf("Skipping text index '%s' on %s.%s", indexDoc.Name, db.Name(), collName), 0)
			continue
		}

		indexes = append(indexes, IndexInfo{
			Name:   indexDoc.Name,
			Key:    indexDoc.Key,
			Unique: indexDoc.Unique,
		})
	}

	return CollectionInfo{
		Namespace: fmt.Sprintf("%s.%s", db.Name(), collName),
		DB:        db.Name(),
		Coll:      collName,
		Count:     count,
		Indexes:   indexes,
		Size:      collStats.Size,
	}, nil
}
