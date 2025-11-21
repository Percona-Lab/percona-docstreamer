package dbops

import (
	"context"
	"fmt"

	"github.com/Percona-Lab/docMongoStream/internal/logging"
	"go.mongodb.org/mongo-driver/mongo"
)

// changeStreamInfo is used to unmarshal the result from $listChangeStreams
type changeStreamInfo struct {
	Database   string `bson:"database"`
	Collection string `bson:"collection"`
}

// ValidateDocDBStreamConfig checks if the DocumentDB cluster has the cluster-wide
// change stream ("") enabled by running the $listChangeStreams command.
func ValidateDocDBStreamConfig(ctx context.Context, sourceClient *mongo.Client) (bool, error) {
	// The pipeline to run: [{$listChangeStreams: 1}]
	pipeline := mongo.Pipeline{
		{{Key: "$listChangeStreams", Value: 1}},
	}

	logging.PrintInfo("[VALIDATE] Running $listChangeStreams on admin DB...", 0)

	// Run the command against the 'admin' database
	cursor, err := sourceClient.Database("admin").Aggregate(ctx, pipeline)
	if err != nil {
		return false, fmt.Errorf("failed to run $listChangeStreams command: %w", err)
	}
	defer cursor.Close(ctx)

	foundClusterStream := false
	var enabledStreams []string // Log what we find for troubleshooting

	for cursor.Next(ctx) {
		var stream changeStreamInfo
		if err := cursor.Decode(&stream); err != nil {
			logging.PrintWarning(fmt.Sprintf("[VALIDATE] Failed to decode change stream info: %v", err), 0)
			continue
		}

		// Log the stream we found
		if stream.Database == "" && stream.Collection == "" {
			// This is the one we're looking for!
			foundClusterStream = true
			enabledStreams = append(enabledStreams, "CLUSTER_WIDE (*.*)")
		} else if stream.Collection == "" {
			enabledStreams = append(enabledStreams, fmt.Sprintf("%s.*", stream.Database))
		} else {
			enabledStreams = append(enabledStreams, fmt.Sprintf("%s.%s", stream.Database, stream.Collection))
		}
	}

	if err := cursor.Err(); err != nil {
		return false, fmt.Errorf("error iterating $listChangeStreams cursor: %w", err)
	}

	// Log all enabled streams for future troubleshooting
	logging.PrintInfo(fmt.Sprintf("[VALIDATE] Found %d enabled change streams:", len(enabledStreams)), 0)
	for _, s := range enabledStreams {
		logging.PrintInfo(fmt.Sprintf("[VALIDATE]   - %s", s), 0)
	}

	// Return whether we found the one that matters
	return foundClusterStream, nil
}
