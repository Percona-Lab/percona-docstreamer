package validator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Percona-Lab/docMongoStream/internal/config"
	"github.com/Percona-Lab/docMongoStream/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ValidationResult is the return type for the API
type ValidationResult struct {
	DocID  string `json:"docId"`
	Status string `json:"status"` // valid, mismatch, missing_source, missing_target
	Reason string `json:"reason,omitempty"`
}

// ValidationRecord represents ONLY a failure stored in DB
type ValidationRecord struct {
	Namespace  string    `bson:"namespace" json:"namespace"`
	DocID      string    `bson:"doc_id" json:"docId"`
	Reason     string    `bson:"reason,omitempty" json:"reason,omitempty"`
	DetectedAt time.Time `bson:"detected_at" json:"detectedAt"`
}

// ValidationStats holds the running totals per collection
type ValidationStats struct {
	Namespace  string `bson:"_id" json:"namespace"`
	Valid      int64  `bson:"valid_count" json:"validCount"`
	Mismatch   int64  `bson:"mismatch_count" json:"mismatchCount"`
	Total      int64  `bson:"total_checked" json:"totalChecked"`
	CapReached bool   `bson:"failure_cap_reached" json:"failureCapReached"` // <--- New Flag
}

type Store struct {
	client   *mongo.Client
	failures *mongo.Collection
	stats    *mongo.Collection
}

func NewStore(client *mongo.Client) *Store {
	dbName := config.Cfg.Migration.MetadataDB
	failuresColl := client.Database(dbName).Collection("validation_failures")
	statsColl := client.Database(dbName).Collection("validation_stats")

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := failuresColl.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "namespace", Value: 1}, {Key: "doc_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		})
		if err != nil {
			logging.PrintWarning(fmt.Sprintf("[VAL STORE] Failed to create index: %v", err), 0)
		}
	}()

	return &Store{
		client:   client,
		failures: failuresColl,
		stats:    statsColl,
	}
}

// RegisterOutcome updates the DB based on the validation result
func (s *Store) RegisterOutcome(ctx context.Context, ns string, res ValidationResult) {
	if ctx.Err() != nil {
		return
	}

	filter := bson.D{{Key: "namespace", Value: ns}, {Key: "doc_id", Value: res.DocID}}

	if res.Status == "valid" {
		// --- VALID CASE ---
		delRes, err := s.failures.DeleteOne(ctx, filter)
		if err == nil && delRes.DeletedCount > 0 {
			// Was a failure, now fixed. -1 Mismatch, +1 Valid
			// Note: We don't automatically unset CapReached here, complex to calculate.
			// User can reset via retry or manual intervention if needed.
			s.updateStats(ctx, ns, 1, -1, 0)
		} else {
			// New or already valid. +1 Valid, +1 Total
			s.updateStats(ctx, ns, 1, 0, 1)
		}
	} else {
		// --- MISMATCH CASE ---
		update := bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "reason", Value: res.Reason},
				{Key: "detected_at", Value: time.Now().UTC()},
			}},
		}
		updRes, err := s.failures.UpdateOne(ctx, filter, update)

		if err == nil && updRes.MatchedCount > 0 {
			// Record existed and was updated. Stats don't change.
			return
		}

		// It is a NEW failure. Check Safety Cap.
		maxSamples := int64(config.Cfg.Validation.MaxFailureSamples)
		if maxSamples <= 0 {
			maxSamples = 1000
		}

		// Check count.
		count, _ := s.failures.CountDocuments(ctx, bson.D{{Key: "namespace", Value: ns}}, options.Count().SetLimit(maxSamples))

		if count < maxSamples {
			// We have room. Insert the detailed failure record.
			opts := options.UpdateOne().SetUpsert(true)
			s.failures.UpdateOne(ctx, filter, update, opts)
			// Update stats normally
			s.updateStats(ctx, ns, 0, 1, 1)
		} else {
			// Cap reached. Do NOT insert record.
			// Update stats AND set cap flag to true.
			s.updateStats(ctx, ns, 0, 1, 1, true)
		}
	}
}

// updateStats updates the aggregate counters.
// Optional last arg: setCapReached (bool). If true, sets failure_cap_reached=true
func (s *Store) updateStats(ctx context.Context, ns string, validInc, mismatchInc, totalInc int64, setCapReached ...bool) {
	if validInc == 0 && mismatchInc == 0 && totalInc == 0 {
		return
	}
	if ctx.Err() != nil {
		return
	}

	opts := options.UpdateOne().SetUpsert(true)
	filter := bson.D{{Key: "_id", Value: ns}}

	setFields := bson.D{
		{Key: "last_updated", Value: time.Now().UTC()},
	}
	if len(setCapReached) > 0 && setCapReached[0] {
		setFields = append(setFields, bson.E{Key: "failure_cap_reached", Value: true})
	}

	update := bson.D{
		{Key: "$inc", Value: bson.D{
			{Key: "valid_count", Value: validInc},
			{Key: "mismatch_count", Value: mismatchInc},
			{Key: "total_checked", Value: totalInc},
		}},
		{Key: "$set", Value: setFields},
	}
	_, err := s.stats.UpdateOne(ctx, filter, update, opts)

	if err != nil && !errors.Is(err, context.Canceled) {
		logging.PrintError(fmt.Sprintf("[VAL STORE] Failed to update stats: %v", err), 0)
	}
}

// Invalidate removes a failure record if exists (used by CDC)
func (s *Store) Invalidate(ctx context.Context, ns, docID string) {
	if ctx.Err() != nil {
		return
	}
	filter := bson.D{{Key: "namespace", Value: ns}, {Key: "doc_id", Value: docID}}
	res, _ := s.failures.DeleteOne(ctx, filter)
	if res.DeletedCount > 0 {
		s.updateStats(ctx, ns, 0, -1, -1)
	}
}

// GetAllFailureIDs returns all document IDs that are currently marked as failures
func (s *Store) GetAllFailureIDs(ctx context.Context) ([]ValidationRecord, error) {
	cursor, err := s.failures.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var results []ValidationRecord
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}
	return results, nil
}

// GetStats returns the counters for all namespaces
func (s *Store) GetStats(ctx context.Context) ([]ValidationStats, error) {
	cursor, err := s.stats.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var stats []ValidationStats
	if err := cursor.All(ctx, &stats); err != nil {
		return nil, err
	}
	return stats, nil
}
