package topo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// helloResult struct to convert data from a serialized format (JSON, BSON, YAML, etc.) into a struct
type helloResult struct {
	OperationTime bson.Timestamp `bson:"operationTime"`
}

// ClusterTime fetches the current operation time from the MongoDB cluster.
// This is the most reliable way to get a "point in time" timestamp.
func ClusterTime(ctx context.Context, client *mongo.Client) (bson.Timestamp, error) {
	res := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}})
	if res.Err() != nil {
		return bson.Timestamp{}, fmt.Errorf("hello command failed: %w", res.Err())
	}

	var hello helloResult
	if err := res.Decode(&hello); err != nil {
		return bson.Timestamp{}, fmt.Errorf("failed to decode hello result: %w", err)
	}

	// Check zero value for bson.Timestamp
	if (hello.OperationTime == bson.Timestamp{}) {
		return bson.Timestamp{}, fmt.Errorf("operationTime not found in hello response")
	}

	return hello.OperationTime, nil
}
