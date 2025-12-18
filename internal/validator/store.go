package validator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
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
	Namespace string `bson:"_id" json:"namespace"`
	Valid     int64  `bson:"valid_count" json:"validCount"`
	Mismatch  int64  `bson:"mismatch_count" json:"mismatchCount"`
	Total     int64  `bson:"total_checked" json:"totalChecked"`
}

type Store struct {
	client   *mongo.Client
	failures *mongo.Collection
	stats    *mongo.Collection
}

func NewStore(client *mongo.Client) *Store {
	dbName := config.Cfg.Migration.MetadataDB
	failuresColl := client.Database(dbName).Collection(config.Cfg.Migration.ValidationFailuresCollection)
	statsColl := client.Database(dbName).Collection(config.Cfg.Migration.ValidationStatsCollection)

	s := &Store{
		client:   client,
		failures: failuresColl,
		stats:    statsColl,
	}
	s.ensureIndexes()
	return s
}

func (s *Store) ensureIndexes() {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := s.failures.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "namespace", Value: 1}, {Key: "doc_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		})
		if err != nil {
			logging.PrintWarning(fmt.Sprintf("[VAL STORE] Failed to create index: %v", err), 0)
		}
	}()
}

// RegisterOutcome updates the DB based on the validation result
func (s *Store) RegisterOutcome(ctx context.Context, ns string, res ValidationResult) {
	if ctx.Err() != nil {
		return
	}

	filter := bson.D{{Key: "namespace", Value: ns}, {Key: "doc_id", Value: res.DocID}}

	if res.Status == "valid" {
		delRes, err := s.failures.DeleteOne(ctx, filter)
		if err == nil && delRes.DeletedCount > 0 {
			s.updateStats(ctx, ns, 1, -1, 0)
		} else {
			s.updateStats(ctx, ns, 1, 0, 1)
		}
	} else {
		update := bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "reason", Value: res.Reason},
				{Key: "detected_at", Value: time.Now().UTC()},
			}},
		}
		opts := options.UpdateOne().SetUpsert(true)
		updRes, err := s.failures.UpdateOne(ctx, filter, update, opts)

		if err == nil && updRes.MatchedCount == 0 {
			s.updateStats(ctx, ns, 0, 1, 1)
		}
	}
}

// updateStats updates the aggregate counters
func (s *Store) updateStats(ctx context.Context, ns string, validInc, mismatchInc, totalInc int64) {
	if validInc == 0 && mismatchInc == 0 && totalInc == 0 {
		return
	}
	if ctx.Err() != nil {
		return
	}

	opts := options.UpdateOne().SetUpsert(true)
	filter := bson.D{{Key: "_id", Value: ns}}
	update := bson.D{
		{Key: "$inc", Value: bson.D{
			{Key: "valid_count", Value: validInc},
			{Key: "mismatch_count", Value: mismatchInc},
			{Key: "total_checked", Value: totalInc},
		}},
		{Key: "$set", Value: bson.D{{Key: "last_updated", Value: time.Now().UTC()}}},
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

// Reset clears all validation data (Hard Reset)
func (s *Store) Reset(ctx context.Context) error {
	if err := s.failures.Drop(ctx); err != nil {
		return err
	}
	if err := s.stats.Drop(ctx); err != nil {
		return err
	}
	s.ensureIndexes()
	return nil
}

// Reconcile fixes "Ghost Mismatches" by synchronizing stats with actual failure records
func (s *Store) Reconcile(ctx context.Context) error {
	// 1. Get all namespaces from stats
	cursor, err := s.stats.Find(ctx, bson.D{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var stats []ValidationStats
	if err := cursor.All(ctx, &stats); err != nil {
		return err
	}

	// 2. For each namespace, count actual failures
	for _, stat := range stats {
		actualFailures, err := s.failures.CountDocuments(ctx, bson.D{{Key: "namespace", Value: stat.Namespace}})
		if err != nil {
			logging.PrintError(fmt.Sprintf("[VAL STORE] Failed to count failures for %s: %v", stat.Namespace, err), 0)
			continue
		}

		// 3. Calculate new Valid count.
		// Assume Total is correct (or close enough), adjust Valid to match Total - Failures
		newMismatch := actualFailures
		newValid := stat.Total - newMismatch

		if newValid < 0 {
			newValid = 0 // Should not happen unless DB is very messed up
		}

		// 4. Force Update
		filter := bson.D{{Key: "_id", Value: stat.Namespace}}
		update := bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "mismatch_count", Value: newMismatch},
				{Key: "valid_count", Value: newValid},
				// We keep total_checked as is, assuming it tracks "effort" correctly
			}},
		}
		s.stats.UpdateOne(ctx, filter, update)
	}
	return nil
}
