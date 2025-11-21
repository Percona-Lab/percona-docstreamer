package indexer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Percona-Lab/docMongoStream/internal/discover"
	"github.com/Percona-Lab/docMongoStream/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// CreateCollectionAndPreloadIndexes handles the initial setup of the target collection.
// It creates the collection and starts building indexes.
func CreateCollectionAndPreloadIndexes(ctx context.Context, targetDB *mongo.Database, collInfo discover.CollectionInfo, indexes []mongo.IndexModel) (*mongo.Collection, error) {
	ns := collInfo.Namespace

	logging.PrintInfo(fmt.Sprintf("[%s] Creating target collection...", ns), 0)
	if err := targetDB.CreateCollection(ctx, collInfo.Coll); err != nil {
		// Don't fail if it "already exists" (case where destroy=false)
		if !mongo.IsDuplicateKeyError(err) && !strings.Contains(err.Error(), "already exists") {
			return nil, fmt.Errorf("failed to create collection %s: %w", ns, err)
		}
	}

	targetColl := targetDB.Collection(collInfo.Coll)

	if len(indexes) > 0 {
		logging.PrintInfo(fmt.Sprintf("[%s] Starting creation of %d indexes...", ns, len(indexes)), 0)
		start := time.Now()
		names, err := targetColl.Indexes().CreateMany(ctx, indexes)
		if err != nil {
			// Just log a warning, we'll try again later
			logging.PrintWarning(fmt.Sprintf("[%s] Failed to create indexes (will retry post-load): %v", ns, err), 0)
		} else {
			elapsed := time.Since(start)
			logging.PrintInfo(fmt.Sprintf("[%s] Submitted %d indexes in %s: %v", ns, len(names), elapsed, names), 0)
		}
	}

	return targetColl, nil
}

// FinalizeIndexes builds any indexes that failed during pre-load.
// Accepts []discover.IndexInfo to access metadata (Name) which is hidden in v2 IndexModel builders.
func FinalizeIndexes(ctx context.Context, targetColl *mongo.Collection, indexes []discover.IndexInfo, ns string) error {
	if len(indexes) == 0 {
		return nil
	}

	logging.PrintInfo(fmt.Sprintf("[%s] Finalizing %d indexes...", ns, len(indexes)), 0)
	start := time.Now()

	// List existing indexes to see which ones we still need
	cursor, err := targetColl.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("failed to list existing indexes: %w", err)
	}

	existingIndexes := make(map[string]bool)
	for cursor.Next(ctx) {
		var index bson.M
		if err := cursor.Decode(&index); err != nil {
			return fmt.Errorf("failed to decode existing index: %w", err)
		}
		if name, ok := index["name"].(string); ok {
			existingIndexes[name] = true
		}
	}

	// Filter out any indexes that successfully built
	indexesToBuild := []mongo.IndexModel{}
	for _, idx := range indexes {
		if !existingIndexes[idx.Name] {
			// Reconstruct the model for the V2 driver
			model := mongo.IndexModel{
				Keys:    idx.Key,
				Options: options.Index().SetName(idx.Name).SetUnique(idx.Unique),
			}
			indexesToBuild = append(indexesToBuild, model)
		}
	}

	if len(indexesToBuild) == 0 {
		logging.PrintSuccess(fmt.Sprintf("[%s] All indexes confirmed.", ns), 0)
		return nil
	}

	logging.PrintInfo(fmt.Sprintf("[%s] Creating %d missing indexes...", ns, len(indexesToBuild)), 0)
	names, err := targetColl.Indexes().CreateMany(ctx, indexesToBuild)
	if err != nil {
		return fmt.Errorf("failed to create missing indexes: %w", err)
	}

	elapsed := time.Since(start)
	logging.PrintSuccess(fmt.Sprintf("[%s] Index finalization complete in %s. Created: %v", ns, elapsed, names), 0)
	return nil
}
