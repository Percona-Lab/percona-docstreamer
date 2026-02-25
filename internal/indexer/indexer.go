package indexer

import (
	"context"
	"fmt"
	"strings"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/discover"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func isCollectionSharded(ctx context.Context, client *mongo.Client, ns string) bool {
	filter := bson.D{{Key: "_id", Value: ns}}
	err := client.Database("config").Collection("collections").FindOne(ctx, filter).Err()
	return err == nil
}

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
		logging.PrintInfo(fmt.Sprintf("[%s] Already sharded. Skipping setup.", ns), 0)
		return
	}

	logging.PrintInfo(fmt.Sprintf("[%s] Sharding setup start: %s", ns, rule.ShardKey), 0)

	// 2. Prepare Keys
	keyDoc := bson.D{}
	isHashed := false
	for _, part := range strings.Split(rule.ShardKey, ",") {
		kv := strings.Split(strings.TrimSpace(part), ":")
		if len(kv) < 1 {
			continue
		}
		k, v := kv[0], "1"
		if len(kv) > 1 {
			v = strings.ToLower(kv[1])
		}

		if v == "hashed" {
			keyDoc = append(keyDoc, bson.E{Key: k, Value: "hashed"})
			isHashed = true
		} else if v == "-1" {
			keyDoc = append(keyDoc, bson.E{Key: k, Value: -1})
		} else {
			keyDoc = append(keyDoc, bson.E{Key: k, Value: 1})
		}
	}

	isHashedPrefix := false
	if len(keyDoc) > 0 {
		if strVal, ok := keyDoc[0].Value.(string); ok && strVal == "hashed" {
			isHashedPrefix = true
		}
	}

	// 3. Create Index
	// Use keyDoc directly. Do NOT append _id here.
	indexKey := make(bson.D, len(keyDoc))
	copy(indexKey, keyDoc)

	targetClient.Database("admin").RunCommand(ctx, bson.D{{Key: "enableSharding", Value: dbName}})
	targetClient.Database(dbName).Collection(collInfo.Coll).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    indexKey,
		Options: options.Index().SetName("shard_key_index_auto").SetUnique(rule.Unique),
	})

	// 4. Shard Collection
	cmd := bson.D{{Key: "shardCollection", Value: ns}, {Key: "key", Value: keyDoc}}
	if rule.Unique {
		cmd = append(cmd, bson.E{Key: "unique", Value: true})
	}

	if isHashedPrefix || (isHashed && len(keyDoc) == 1) {
		if rule.PreSplitStrategy == "hashed_manual" {
			cmd = append(cmd, bson.E{Key: "numInitialChunks", Value: 1})
			logging.PrintInfo(fmt.Sprintf("[%s] Hashed Manual: Forcing 1 initial chunk.", ns), 0)
		} else {
			shards, _ := getShardList(ctx, targetClient)
			numChunks := int64(2)
			if len(shards) > 0 {
				numChunks = int64(len(shards) * 2)
			}
			if rule.NumInitialChunks > 0 {
				numChunks = int64(rule.NumInitialChunks)
			}
			cmd = append(cmd, bson.E{Key: "numInitialChunks", Value: numChunks})
		}
	}

	if err := targetClient.Database("admin").RunCommand(ctx, cmd).Err(); err != nil {
		logging.PrintError(fmt.Sprintf("[%s] Shard command failed: %v", ns, err), 0)
		return
	}

	// 5. Pre-Splitting & Distribution
	ApplyPreSplit(ctx, targetClient, ns, rule, collInfo)

	shards, _ := getShardList(ctx, targetClient)
	if len(shards) > 0 {
		distributeChunks(ctx, targetClient, ns, shards)
	}
}

func CreateCollectionAndPreloadIndexes(ctx context.Context, targetDB *mongo.Database, sourceColl *mongo.Collection, collInfo discover.CollectionInfo, indexes []mongo.IndexModel) (*mongo.Collection, error) {
	ns := collInfo.Namespace
	targetDB.CreateCollection(ctx, collInfo.Coll)
	targetColl := targetDB.Collection(collInfo.Coll)

	if len(indexes) > 0 {
		logging.PrintInfo(fmt.Sprintf("[%s] Pre-loading %d indexes...", ns, len(indexes)), 0)
		_, err := targetColl.Indexes().CreateMany(ctx, indexes)
		if err != nil {
			logging.PrintWarning(fmt.Sprintf("[%s] Non-fatal index creation warning: %v", ns, err), 0)
		}
	}

	applySharding(ctx, targetDB.Client(), sourceColl, collInfo.DB, ns, collInfo)
	return targetColl, nil
}

func FinalizeIndexes(ctx context.Context, targetColl *mongo.Collection, indexes []discover.IndexInfo, ns string) error {
	cursor, err := targetColl.Indexes().List(ctx)
	if err != nil {
		return nil
	}

	// We decode to []bson.D to preserve the exact order of the index keys
	var existingIndexes []bson.D
	if err := cursor.All(ctx, &existingIndexes); err != nil {
		return err
	}

	// Store existing index keys by their binary representation
	existingKeys := make(map[string]bool)
	for _, idxDoc := range existingIndexes {
		for _, elem := range idxDoc {
			if elem.Key == "key" {
				if keyD, ok := elem.Value.(bson.D); ok {
					keyBytes, _ := bson.Marshal(keyD)
					existingKeys[string(keyBytes)] = true
				}
			}
		}
	}

	var missing []mongo.IndexModel
	for _, idx := range indexes {
		// 1. Explicitly skip the _id index (MongoDB handles this natively)
		isId := false
		if len(idx.Key) == 1 && idx.Key[0].Key == "_id" {
			isId = true
		}
		if isId {
			continue
		}

		// 2. Check if the index key already exists on the target
		keyBytes, _ := bson.Marshal(idx.Key)
		if !existingKeys[string(keyBytes)] {
			missing = append(missing, mongo.IndexModel{
				Keys:    idx.Key,
				Options: options.Index().SetName(idx.Name).SetUnique(idx.Unique),
			})
		}
	}

	if len(missing) > 0 {
		logging.PrintInfo(fmt.Sprintf("[%s] Finalizing %d missing indexes...", ns, len(missing)), 0)
		_, err := targetColl.Indexes().CreateMany(ctx, missing)
		if err != nil {
			logging.PrintError(fmt.Sprintf("[%s] Failed to create final indexes: %v", ns, err), 0)
			return err
		}
		logging.PrintSuccess(fmt.Sprintf("[%s] Indexes finalized.", ns), 0)
	} else {
		logging.PrintSuccess(fmt.Sprintf("[%s] All required indexes already exist.", ns), 0)
	}
	return nil
}
