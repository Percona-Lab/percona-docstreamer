package indexer

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type ShardInfo struct {
	ID   string `bson:"_id"`
	Host string `bson:"host"`
}

type ChunkInfo struct {
	Min   bson.RawValue `bson:"min"`
	Max   bson.RawValue `bson:"max"`
	Shard string        `bson:"shard"`
	Jumbo bool          `bson:"jumbo,omitempty"`
}

func StopBalancer(ctx context.Context, client *mongo.Client) {
	logging.PrintInfo("Ensuring Balancer is STOPPED...", 0)
	client.Database("admin").RunCommand(ctx, bson.D{{Key: "balancerStop", Value: 1}})
}

func StartBalancer(ctx context.Context, client *mongo.Client) {
	logging.PrintInfo("Restarting Balancer...", 0)
	client.Database("admin").RunCommand(ctx, bson.D{{Key: "balancerStart", Value: 1}})
}

func getShardList(ctx context.Context, client *mongo.Client) ([]string, error) {
	var result struct {
		Shards []ShardInfo `bson:"shards"`
	}
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "listShards", Value: 1}}).Decode(&result); err != nil {
		return nil, err
	}
	ids := make([]string, len(result.Shards))
	for i, s := range result.Shards {
		ids[i] = s.ID
	}

	// Sort shard IDs to ensure deterministic round-robin distribution.
	sort.Strings(ids)

	return ids, nil
}

func getCollectionUUID(ctx context.Context, client *mongo.Client, ns string) (bson.RawValue, error) {
	var result struct {
		UUID bson.RawValue `bson:"uuid"`
	}
	err := client.Database("config").Collection("collections").FindOne(ctx, bson.D{{Key: "_id", Value: ns}}).Decode(&result)
	return result.UUID, err
}

// distributeChunks balances chunks round-robin concurrently
func distributeChunks(ctx context.Context, targetClient *mongo.Client, ns string, shards []string) {
	if len(shards) < 2 {
		return
	}
	logging.PrintStep(fmt.Sprintf("[%s] Starting Phase 2: Chunk Distribution (Round-Robin)...", ns), 0)

	collUUID, err := getCollectionUUID(ctx, targetClient, ns)
	if err != nil {
		logging.PrintWarning(fmt.Sprintf("[%s] Failed to get UUID: %v", ns, err), 0)
		return
	}

	filter := bson.D{{Key: "uuid", Value: collUUID}}
	opts := options.Find().SetSort(bson.D{{Key: "min", Value: 1}})

	chunksColl := targetClient.Database("config").Collection("chunks")
	cursor, err := chunksColl.Find(ctx, filter, opts)
	if err != nil {
		logging.PrintWarning(fmt.Sprintf("[%s] Failed to list chunks by UUID: %v", ns, err), 0)
		return
	}

	var chunks []ChunkInfo
	if err := cursor.All(ctx, &chunks); err != nil {
		logging.PrintWarning(fmt.Sprintf("[%s] Failed to decode chunks: %v", ns, err), 0)
		return
	}

	if len(chunks) == 0 {
		logging.PrintInfo(fmt.Sprintf("[%s] No chunks found by UUID. Retrying by namespace...", ns), 0)
		filterNS := bson.D{{Key: "ns", Value: ns}}
		cursorNS, err := chunksColl.Find(ctx, filterNS, opts)
		if err == nil {
			_ = cursorNS.All(ctx, &chunks)
		}
	}

	if len(chunks) == 0 {
		logging.PrintWarning(fmt.Sprintf("[%s] Found 0 chunks. Is the collection sharded?", ns), 0)
		return
	}

	logging.PrintInfo(fmt.Sprintf("[%s] Found %d chunks to distribute across %d shards.", ns, len(chunks), len(shards)), 0)

	var moves int32
	var errors int32

	var wg sync.WaitGroup
	sem := make(chan struct{}, 15) // Bounded concurrency limit for chunk moves

	for i, chunk := range chunks {
		targetShard := shards[i%len(shards)]

		if chunk.Jumbo {
			if chunk.Shard != targetShard {
				logging.PrintWarning(fmt.Sprintf("[%s] Skipping Jumbo chunk on %s (wanted %s). Manual intervention required.", ns, chunk.Shard, targetShard), 0)
			}
			continue
		}

		if chunk.Shard != targetShard {
			wg.Add(1)
			sem <- struct{}{} // Acquire concurrency token

			go func(c ChunkInfo, tShard string) {
				defer wg.Done()
				defer func() { <-sem }() // Release concurrency token

				cmd := bson.D{
					{Key: "moveChunk", Value: ns},
					{Key: "find", Value: c.Min},
					{Key: "to", Value: tShard},
				}

				success := false
				for attempt := 1; attempt <= 5; attempt++ {
					err := targetClient.Database("admin").RunCommand(ctx, cmd).Err()
					if err == nil {
						success = true
						break
					}

					errMsg := err.Error()
					if strings.Contains(errMsg, "lock") || strings.Contains(errMsg, "busy") || strings.Contains(errMsg, "ConflictingOperationInProgress") {
						time.Sleep(time.Duration(attempt) * 1000 * time.Millisecond)
					} else {
						time.Sleep(500 * time.Millisecond)
					}
				}

				if success {
					atomic.AddInt32(&moves, 1)
				} else {
					atomic.AddInt32(&errors, 1)
					if atomic.LoadInt32(&errors) <= 5 {
						logging.PrintWarning(fmt.Sprintf("[%s] Failed to move chunk to %s", ns, tShard), 0)
					}
				}
			}(chunk, targetShard)
		}
	}

	wg.Wait()
	logging.PrintSuccess(fmt.Sprintf("[%s] Distribution complete. Moved: %d, Errors: %d", ns, atomic.LoadInt32(&moves), atomic.LoadInt32(&errors)), 0)
}
