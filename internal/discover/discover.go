package discover

import (
	"context"
	"fmt"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// IndexInfo holds information about a single index
type IndexInfo struct {
	Name   string
	Key    bson.D
	Unique bool
}

// CollectionInfo holds metadata about a collection to be migrated
type CollectionInfo struct {
	Namespace     string
	DB            string
	Coll          string
	Count         int64
	Indexes       []IndexInfo
	Size          int64 // Size in bytes
	UseLinearScan bool  // Flag to force linear scan if range queries are unsafe
}

// indexDecoded is a helper to correctly decode index definitions
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

	excludeDBs := make(map[string]bool)
	for _, db := range config.Cfg.Migration.ExcludeDBs {
		excludeDBs[db] = true
	}

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
			continue
		}

		logging.PrintStep(fmt.Sprintf("Scanning DB: %s", dbName), 0)
		db := client.Database(dbName)
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

func discoverCollectionsInDB(ctx context.Context, db *mongo.Database, excludeColls map[string]bool) ([]CollectionInfo, error) {
	filter := bson.D{{Key: "name", Value: bson.D{{Key: "$not", Value: bson.M{"$regex": "^system\\."}}}}}
	collNames, err := db.ListCollectionNames(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to list collections for %s: %w", db.Name(), err)
	}

	var collections []CollectionInfo
	for _, collName := range collNames {
		if db.Name() == "local" && collName == "oplog.rs" {
			continue
		}

		namespace := fmt.Sprintf("%s.%s", db.Name(), collName)
		if _, ok := excludeColls[namespace]; ok {
			logging.PrintInfo(fmt.Sprintf("Skipping collection: %s (excluded by configuration)", namespace), 0)
			continue
		}

		info, err := getCollectionInfo(ctx, db, collName)
		if err != nil {
			logging.PrintWarning(fmt.Sprintf("Failed to get info for %s.%s: %v", db.Name(), collName, err), 0)
			continue
		}

		scanType := "Range Scan"
		if info.UseLinearScan {
			scanType = "LINEAR SCAN (Safety Override)"
		}

		logging.PrintInfo(fmt.Sprintf("- Found: %s (%d documents, %d indexes) [%s]",
			info.Namespace, info.Count, len(info.Indexes), scanType), 0)
		collections = append(collections, info)
	}
	return collections, nil
}

// checkRangeQuerySafety verifies if range queries are safe by sampling Head, Tail, and Random Middle documents.
func checkRangeQuerySafety(ctx context.Context, coll *mongo.Collection, estCount int64) (bool, string) {
	if estCount < 10 {
		return false, ""
	}

	var observedTypes []bson.Type
	var firstID bson.RawValue

	// 1. HEAD SAMPLE: Fetch the first 1000 documents
	optsHead := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(1000)
	headCursor, err := coll.Find(ctx, bson.D{}, optsHead)
	if err != nil {
		return true, fmt.Sprintf("Head sample failed: %v", err)
	}
	defer headCursor.Close(ctx)

	rowCount := 0
	for headCursor.Next(ctx) {
		rowCount++
		id, err := headCursor.Current.LookupErr("_id")
		if err == nil {
			if rowCount == 1 {
				firstID = id
			}
			observedTypes = append(observedTypes, id.Type)
		}
	}

	if rowCount == 0 && estCount > 0 {
		return true, "Head Scan returned 0 documents (Invisible Data)"
	}

	// 2. TAIL SAMPLE: Fetch the last 1000 documents
	optsTail := options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(1000)
	tailCursor, err := coll.Find(ctx, bson.D{}, optsTail)
	if err != nil {
		return true, fmt.Sprintf("Tail sample failed: %v", err)
	}
	defer tailCursor.Close(ctx)

	for tailCursor.Next(ctx) {
		id, err := tailCursor.Current.LookupErr("_id")
		if err == nil {
			observedTypes = append(observedTypes, id.Type)
		}
	}

	// 3. RANDOM SAMPLE (Middle): Fetch 1000 random documents using $sample
	// This catches cases where head/tail are clean but the middle is messy.
	pipeline := mongo.Pipeline{
		{{Key: "$sample", Value: bson.D{{Key: "size", Value: 1000}}}},
	}
	sampleCursor, err := coll.Aggregate(ctx, pipeline)
	if err == nil { // If $sample fails (not supported?), we just skip this check
		defer sampleCursor.Close(ctx)
		for sampleCursor.Next(ctx) {
			id, err := sampleCursor.Current.LookupErr("_id")
			if err == nil {
				observedTypes = append(observedTypes, id.Type)
			}
		}
	}

	// 4. CONSISTENCY CHECK: Verify all collected types are identical
	if len(observedTypes) > 0 {
		baseType := firstID.Type
		for _, t := range observedTypes {
			if t != baseType {
				return true, fmt.Sprintf("Mixed Types detected in samples (%s vs %s)", baseType, t)
			}
		}
	}

	// 5. CONNECTIVITY CHECK: Verify range query finds at least one document
	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: firstID}}}}
	count, err := coll.CountDocuments(ctx, filter, options.Count().SetLimit(1))
	if err != nil {
		return true, fmt.Sprintf("Probe count failed: %v", err)
	}
	if count == 0 {
		return true, "Range Query ($gte: min) returned 0 documents"
	}

	return false, ""
}

func getCollectionInfo(ctx context.Context, db *mongo.Database, collName string) (CollectionInfo, error) {
	coll := db.Collection(collName)
	checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second) // Increased timeout for sampling
	defer cancel()

	count, err := coll.EstimatedDocumentCount(checkCtx)
	if err != nil {
		return CollectionInfo{}, fmt.Errorf("failed to count docs for %s: %w", collName, err)
	}

	var collStats struct {
		Size int64 `bson:"size"`
	}
	err = db.RunCommand(checkCtx, bson.D{{Key: "collStats", Value: collName}}).Decode(&collStats)
	if err != nil {
		logging.PrintWarning(fmt.Sprintf("Could not get collStats for %s: %v. Size will be reported as 0.", collName, err), 0)
		collStats.Size = 0
	}

	var indexes []IndexInfo
	cursor, err := coll.Indexes().List(checkCtx)
	if err != nil {
		return CollectionInfo{}, fmt.Errorf("failed to list indexes for %s: %w", collName, err)
	}
	defer cursor.Close(checkCtx)

	hasIDIndex := false

	for cursor.Next(checkCtx) {
		var indexDoc indexDecoded
		if err := cursor.Decode(&indexDoc); err != nil {
			return CollectionInfo{}, fmt.Errorf("failed to decode index: %w", err)
		}
		if indexDoc.Name == "_id_" {
			hasIDIndex = true
			continue
		}
		if len(indexDoc.Key) == 0 {
			continue
		}
		if indexDoc.ExpireAfterSeconds != nil {
			logging.PrintWarning(fmt.Sprintf("Skipping TTL index '%s' on %s.%s", indexDoc.Name, db.Name(), collName), 0)
			continue
		}
		if indexDoc.PartialFilterExpression != nil {
			logging.PrintWarning(fmt.Sprintf("Skipping partial index '%s' on %s.%s", indexDoc.Name, db.Name(), collName), 0)
			continue
		}
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

	useLinear := false
	reason := ""

	if !hasIDIndex {
		useLinear = true
		reason = "Missing _id index"
	} else {
		// Safety Check (Head + Tail + Random Samples)
		useLinear, reason = checkRangeQuerySafety(checkCtx, coll, count)
	}

	if useLinear {
		logging.PrintWarning(fmt.Sprintf("[%s.%s] Strategy Override: %s. Switching to Linear Scan.", db.Name(), collName, reason), 0)
	}

	return CollectionInfo{
		Namespace:     fmt.Sprintf("%s.%s", db.Name(), collName),
		DB:            db.Name(),
		Coll:          collName,
		Count:         count,
		Indexes:       indexes,
		Size:          collStats.Size,
		UseLinearScan: useLinear,
	}, nil
}
