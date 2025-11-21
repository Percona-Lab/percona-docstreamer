package topo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// helloResult struct to unmarshal the 'hello' command's response
type helloResult struct {
	OperationTime primitive.Timestamp `bson:"operationTime"`
}

// ClusterTime fetches the current operation time from the MongoDB cluster.
// This is the most reliable way to get a "point in time" timestamp.
func ClusterTime(ctx context.Context, client *mongo.Client) (primitive.Timestamp, error) {
	res := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}})
	if res.Err() != nil {
		return primitive.Timestamp{}, fmt.Errorf("hello command failed: %w", res.Err())
	}

	var hello helloResult
	if err := res.Decode(&hello); err != nil {
		return primitive.Timestamp{}, fmt.Errorf("failed to decode hello result: %w", err)
	}

	if (hello.OperationTime == primitive.Timestamp{}) {
		return primitive.Timestamp{}, fmt.Errorf("operationTime not found in hello response")
	}

	return hello.OperationTime, nil
}
