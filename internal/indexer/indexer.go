package indexer

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/discover"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ShardInfo holds basic shard details
type ShardInfo struct {
	ID   string `bson:"_id"`
	Host string `bson:"host"`
}

// ChunkInfo holds metadata for moving chunks
type ChunkInfo struct {
	Min   bson.RawValue `bson:"min"`
	Max   bson.RawValue `bson:"max"`
	Shard string        `bson:"shard"`
}

// Balancer Management
func stopBalancer(ctx context.Context, client *mongo.Client) {
	logging.PrintInfo("Stopping Balancer to perform manual splits/moves...", 0)
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "balancerStop", Value: 1}}).Err()
	if err != nil {
		logging.PrintWarning(fmt.Sprintf("Failed to stop balancer: %v", err), 0)
	}
}

func startBalancer(ctx context.Context, client *mongo.Client) {
	logging.PrintInfo("Restarting Balancer...", 0)
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "balancerStart", Value: 1}}).Err()
	if err != nil {
		logging.PrintWarning(fmt.Sprintf("Failed to start balancer: %v", err), 0)
	}
}

// getShardList retrieves the list of shards from the target cluster
func getShardList(ctx context.Context, client *mongo.Client) ([]string, error) {
	var result struct {
		Shards []ShardInfo `bson:"shards"`
	}
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "listShards", Value: 1}}).Decode(&result)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(result.Shards))
	for i, s := range result.Shards {
		ids[i] = s.ID
	}
	return ids, nil
}

// getCollectionUUID retrieves the UUID for a namespace from the config database.
func getCollectionUUID(ctx context.Context, client *mongo.Client, ns string) (bson.RawValue, error) {
	var result struct {
		UUID bson.RawValue `bson:"uuid"`
	}
	err := client.Database("config").Collection("collections").FindOne(ctx, bson.D{{Key: "_id", Value: ns}}).Decode(&result)
	if err != nil {
		return bson.RawValue{}, err
	}
	return result.UUID, nil
}

// distributeChunks implements Phase 2: Distributing Chunks Round-Robin
func distributeChunks(ctx context.Context, targetClient *mongo.Client, ns string, shards []string) {
	if len(shards) < 2 {
		logging.PrintInfo(fmt.Sprintf("[%s] Skipped distribution (only %d shard available).", ns, len(shards)), 0)
		return
	}

	logging.PrintStep(fmt.Sprintf("[%s] Starting Phase 2: Chunk Distribution (Round-Robin across %d shards)...", ns, len(shards)), 0)

	collUUID, err := getCollectionUUID(ctx, targetClient, ns)
	if err != nil {
		logging.PrintWarning(fmt.Sprintf("[%s] Failed to get collection UUID: %v. Cannot distribute chunks.", ns, err), 0)
		return
	}

	filter := bson.D{{Key: "uuid", Value: collUUID}}
	opts := options.Find().SetSort(bson.D{{Key: "min", Value: 1}})

	cursor, err := targetClient.Database("config").Collection("chunks").Find(ctx, filter, opts)
	if err != nil {
		logging.PrintWarning(fmt.Sprintf("[%s] Failed to list chunks: %v", ns, err), 0)
		return
	}
	defer cursor.Close(ctx)

	var chunks []ChunkInfo
	if err := cursor.All(ctx, &chunks); err != nil {
		logging.PrintWarning(fmt.Sprintf("[%s] Failed to decode chunks: %v", ns, err), 0)
		return
	}

	logging.PrintInfo(fmt.Sprintf("[%s] Found %d chunks to distribute.", ns, len(chunks)), 0)

	moves := 0
	errors := 0

	for i, chunk := range chunks {
		targetShard := shards[i%len(shards)]

		if chunk.Shard != targetShard {
			cmd := bson.D{
				{Key: "moveChunk", Value: ns},
				{Key: "find", Value: chunk.Min},
				{Key: "to", Value: targetShard},
			}

			success := false
			for attempt := 1; attempt <= 3; attempt++ {
				err := targetClient.Database("admin").RunCommand(ctx, cmd).Err()
				if err == nil {
					success = true
					break
				}
				if strings.Contains(err.Error(), "already") {
					success = true
					break
				}
				time.Sleep(500 * time.Millisecond)
			}

			if success {
				moves++
				if moves%20 == 0 {
					logging.PrintInfo(fmt.Sprintf("[%s] Moved %d chunks...", ns, moves), 0)
				}
			} else {
				errors++
			}
		}
	}

	logging.PrintSuccess(fmt.Sprintf("[%s] Distribution complete. Moved: %d, Errors: %d", ns, moves, errors), 0)
}

// preSplitRangeSharding calculates split points by SAMPLING the source.
// OPTIMIZED: Uses a single cursor iteration instead of repeated Skip() calls.
func preSplitRangeSharding(ctx context.Context, targetClient *mongo.Client, sourceColl *mongo.Collection, ns string, keyDoc bson.D, collInfo discover.CollectionInfo) {
	const targetChunkSize = 128 * 1024 * 1024 // 128MB
	totalSize := collInfo.Size

	if totalSize == 0 && collInfo.Count > 0 {
		totalSize = collInfo.Count * 1024
	}
	if totalSize == 0 {
		return
	}

	numChunks := int(math.Ceil(float64(totalSize) / float64(targetChunkSize)))
	if numChunks <= 1 {
		logging.PrintInfo(fmt.Sprintf("[%s] Data size (%d bytes) fits in one chunk. Skipping pre-split.", ns, totalSize), 0)
		return
	}

	logging.PrintInfo(fmt.Sprintf("[%s] Phase 1: Pre-splitting. Estimated %d chunks for %d bytes.", ns, numChunks, totalSize), 0)

	docStep := collInfo.Count / int64(numChunks)
	if docStep == 0 {
		docStep = 1
	}

	// Project only shard keys to save bandwidth
	projection := bson.D{}
	for _, k := range keyDoc {
		projection = append(projection, bson.E{Key: k.Key, Value: 1})
	}

	// Use a large batch size for efficiency
	opts := options.Find().
		SetSort(keyDoc).
		SetProjection(projection).
		SetBatchSize(10000)

	logging.PrintStep(fmt.Sprintf("[%s] Scanning source for split points (Step: %d docs)...", ns, docStep), 0)

	// SINGLE PASS SCAN
	cursor, err := sourceColl.Find(ctx, bson.D{}, opts)
	if err != nil {
		logging.PrintWarning(fmt.Sprintf("[%s] Failed to start scan: %v", ns, err), 0)
		return
	}
	defer cursor.Close(ctx)

	counter := int64(0)
	splitCount := 0
	targetSplits := numChunks - 1

	for cursor.Next(ctx) {
		counter++

		// Check if we hit a split interval
		if counter%docStep == 0 && splitCount < targetSplits {
			var rawDoc bson.Raw
			rawDoc = cursor.Current

			// Extract split point from the raw document
			splitPoint := bson.D{}
			isValid := true
			for _, elem := range keyDoc {
				val, err := rawDoc.LookupErr(elem.Key)
				if err == nil {
					splitPoint = append(splitPoint, bson.E{Key: elem.Key, Value: val})
				} else {
					isValid = false
					break
				}
			}

			if isValid {
				// Execute Split
				splitCmd := bson.D{
					{Key: "split", Value: ns},
					{Key: "middle", Value: splitPoint},
				}
				// Ignore errors (e.g. duplicate split), just log warning if critical
				if err := targetClient.Database("admin").RunCommand(ctx, splitCmd).Err(); err == nil {
					splitCount++
				}
			}
		}
	}

	if err := cursor.Err(); err != nil {
		logging.PrintWarning(fmt.Sprintf("[%s] Scan cursor error: %v", ns, err), 0)
	}

	logging.PrintSuccess(fmt.Sprintf("[%s] Created %d splits (scanned %d docs).", ns, splitCount, counter), 0)
}

// isCollectionSharded checks if the collection is already sharded
func isCollectionSharded(ctx context.Context, client *mongo.Client, ns string) bool {
	filter := bson.D{{Key: "_id", Value: ns}}
	err := client.Database("config").Collection("collections").FindOne(ctx, filter).Err()
	return err == nil
}

// applySharding handles the full sharding setup process
func applySharding(ctx context.Context, targetClient *mongo.Client, sourceColl *mongo.Collection, dbName, ns string, collInfo discover.CollectionInfo) {
	var rule *config.ShardRule
	for _, r := range config.Cfg.Sharding {
		if r.Namespace == ns {
			rule = &r
			break
		}
	}

	if rule == nil {
		return
	}

	if isCollectionSharded(ctx, targetClient, ns) {
		logging.PrintInfo(fmt.Sprintf("[%s] Collection is already sharded. Skipping setup.", ns), 0)
		return
	}

	logging.PrintInfo(fmt.Sprintf("[%s] Sharding rule found: %s", ns, rule.ShardKey), 0)

	stopBalancer(ctx, targetClient)
	defer startBalancer(ctx, targetClient)

	// 1. Enable Sharding on DB
	targetClient.Database("admin").RunCommand(ctx, bson.D{{Key: "enableSharding", Value: dbName}})

	// 2. Parse Key
	keyDoc := bson.D{}
	isHashed := false
	parts := strings.Split(rule.ShardKey, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, ":") {
			subParts := strings.Split(part, ":")
			fieldName := strings.TrimSpace(subParts[0])
			fieldType := strings.ToLower(strings.TrimSpace(subParts[1]))
			if fieldType == "hashed" {
				keyDoc = append(keyDoc, bson.E{Key: fieldName, Value: "hashed"})
				isHashed = true
			} else if fieldType == "-1" {
				keyDoc = append(keyDoc, bson.E{Key: fieldName, Value: -1})
			} else {
				keyDoc = append(keyDoc, bson.E{Key: fieldName, Value: 1})
			}
		} else {
			keyDoc = append(keyDoc, bson.E{Key: part, Value: 1})
		}
	}

	// 3. Shard Collection
	cmd := bson.D{
		{Key: "shardCollection", Value: ns},
		{Key: "key", Value: keyDoc},
	}

	if isHashed && len(keyDoc) == 1 {
		const chunkSize = 128 * 1024 * 1024
		totalSize := collInfo.Size
		if totalSize == 0 {
			totalSize = collInfo.Count * 1024
		}

		numChunks := int64(math.Ceil(float64(totalSize) / float64(chunkSize)))
		if numChunks < 2 {
			numChunks = 2
		}
		cmd = append(cmd, bson.E{Key: "numInitialChunks", Value: numChunks})
		logging.PrintInfo(fmt.Sprintf("[%s] Hashed Sharding: Pre-allocating %d chunks.", ns, numChunks), 0)
	}

	if err := targetClient.Database("admin").RunCommand(ctx, cmd).Err(); err != nil {
		logging.PrintError(fmt.Sprintf("[%s] Failed to shard collection: %v", ns, err), 0)
		return
	}
	logging.PrintSuccess(fmt.Sprintf("[%s] Collection successfully sharded.", ns), 0)

	// 4. Pre-Split (Range) and Distribute (All)
	shards, err := getShardList(ctx, targetClient)
	if err == nil {
		if !isHashed {
			preSplitRangeSharding(ctx, targetClient, sourceColl, ns, keyDoc, collInfo)
		}
		distributeChunks(ctx, targetClient, ns, shards)
	} else {
		logging.PrintWarning(fmt.Sprintf("[%s] Could not list shards: %v. Skipping distribution.", ns, err), 0)
	}
}

// CreateCollectionAndPreloadIndexes handles initial setup
func CreateCollectionAndPreloadIndexes(ctx context.Context, targetDB *mongo.Database, sourceColl *mongo.Collection, collInfo discover.CollectionInfo, indexes []mongo.IndexModel) (*mongo.Collection, error) {
	ns := collInfo.Namespace

	logging.PrintInfo(fmt.Sprintf("[%s] Creating target collection...", ns), 0)
	if err := targetDB.CreateCollection(ctx, collInfo.Coll); err != nil {
		if !mongo.IsDuplicateKeyError(err) && !strings.Contains(err.Error(), "already exists") {
			return nil, fmt.Errorf("failed to create collection %s: %w", ns, err)
		}
	}

	targetColl := targetDB.Collection(collInfo.Coll)

	applySharding(ctx, targetDB.Client(), sourceColl, collInfo.DB, ns, collInfo)

	if len(indexes) > 0 {
		logging.PrintInfo(fmt.Sprintf("[%s] Starting creation of %d indexes...", ns, len(indexes)), 0)
		start := time.Now()
		names, err := targetColl.Indexes().CreateMany(ctx, indexes)
		if err != nil {
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
