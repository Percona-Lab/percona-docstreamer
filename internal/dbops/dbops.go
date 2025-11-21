package dbops

import (
	"context"
	"fmt"

	"github.com/Percona-Lab/docMongoStream/internal/config"
	"github.com/Percona-Lab/docMongoStream/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// DropAllDatabases lists all databases on the target and drops them,
// *only* if they were also present on the source.
// It also explicitly drops the metadata database to ensure a clean start.
func DropAllDatabases(ctx context.Context, client *mongo.Client, dbsFromSource []string) error {
	// Build the exclude map for admin dbs
	excludeMap := make(map[string]bool)
	for _, db := range config.Cfg.Migration.ExcludeDBs {
		excludeMap[db] = true
	}

	// --- Build map of databases from source discovery ---
	sourceDBMap := make(map[string]bool)
	for _, db := range dbsFromSource {
		sourceDBMap[db] = true
	}

	logging.PrintStep("Discovering databases on target...", 0)
	dbNames, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to list target databases: %w", err)
	}

	dbsToDrop := []string{}
	for _, dbName := range dbNames {
		// If it's in the admin exclude list (admin, local, config), skip it.
		if _, ok := excludeMap[dbName]; ok {
			continue
		}
		// If it's our *own* metadata, skip it for now. We'll drop it last.
		if dbName == config.Cfg.Migration.MetadataDB {
			continue
		}

		// --- Only add if it was in the source discovery ---
		if _, ok := sourceDBMap[dbName]; ok {
			dbsToDrop = append(dbsToDrop, dbName)
		} else {
			logging.PrintInfo(fmt.Sprintf("Skipping drop for target database: %s (not in source)", dbName), 0)
		}
	}

	// Drop all the (now filtered) user databases
	if len(dbsToDrop) == 0 {
		logging.PrintStep("No source databases found on target to drop.", 0)
	}
	for _, dbName := range dbsToDrop {
		logging.PrintStep(fmt.Sprintf("Dropping target database: %s", dbName), 0)
		if err := client.Database(dbName).Drop(ctx); err != nil {
			logging.PrintWarning(fmt.Sprintf("Failed to drop database %s: %v", dbName, err), 0)
		}
	}

	// Finally, drop our own metadata databases to reset the migration
	logging.PrintStep(fmt.Sprintf("Dropping metadata database: %s", config.Cfg.Migration.MetadataDB), 0)
	if err := client.Database(config.Cfg.Migration.MetadataDB).Drop(ctx); err != nil {
		// Don't error out if it doesn't exist
		if err != mongo.ErrNoDocuments {
			logging.PrintWarning(fmt.Sprintf("Failed to drop metadata database: %v", err), 0)
		}
	}

	return nil
}
