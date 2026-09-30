package indexer

import (
	"testing"

	"github.com/Percona-Lab/percona-docstreamer/internal/discover"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestPlanIndexUpdatesDefersTTLUntilFinalize(t *testing.T) {
	ttl := int32(3600)
	desired := []discover.IndexInfo{
		{Name: "_id_", Key: bson.D{{Key: "_id", Value: int32(1)}}},
		{Name: "status_1", Key: bson.D{{Key: "status", Value: int32(1)}}},
		{Name: "expireAt_1", Key: bson.D{{Key: "expireAt", Value: int32(1)}}, ExpireAfterSeconds: &ttl},
	}

	missing, repairs, skipped := planIndexUpdates(nil, desired, false)
	if len(repairs) != 0 {
		t.Fatalf("index pass repaired TTL indexes: %+v", repairs)
	}
	if len(skipped) != 1 || skipped[0] != "expireAt_1" {
		t.Fatalf("skipped TTL = %v", skipped)
	}
	if names := modelNames(t, missing); len(names) != 1 || names[0] != "status_1" {
		t.Fatalf("missing indexes = %v", names)
	}
	if opts := appliedOptions(t, missing[0]); opts.ExpireAfterSeconds != nil {
		t.Fatalf("non-TTL index was given expireAfterSeconds")
	}

	missing, repairs, skipped = planIndexUpdates(nil, desired, true)
	if len(skipped) != 0 || len(repairs) != 0 {
		t.Fatalf("finalize unexpectedly skipped %v or repaired %+v", skipped, repairs)
	}
	if names := modelNames(t, missing); len(names) != 2 {
		t.Fatalf("finalize missing indexes = %v", names)
	}
	var ttlModel *options.IndexOptions
	for _, model := range missing {
		opts := appliedOptions(t, model)
		if opts.Name != nil && *opts.Name == "expireAt_1" {
			ttlModel = &opts
		}
	}
	if ttlModel == nil || ttlModel.ExpireAfterSeconds == nil || *ttlModel.ExpireAfterSeconds != 3600 {
		t.Fatalf("finalize did not plan the TTL index: %+v", ttlModel)
	}
}

func TestPlanIndexUpdatesRepairsTTLShell(t *testing.T) {
	ttl := int32(3600)
	desired := []discover.IndexInfo{
		{Name: "status_1", Key: bson.D{{Key: "status", Value: int32(1)}}},
		{Name: "expireAt_1", Key: bson.D{{Key: "expireAt", Value: int32(1)}}, ExpireAfterSeconds: &ttl},
	}
	// Same key as the source, but the direction is int64 — the mismatch that
	// used to make finalize call createIndexes and hit "index already exists".
	shell := []listedIndex{
		{Name: "status_1", Key: bson.D{{Key: "status", Value: int64(1)}}},
		{Name: "expireAt_1", Key: bson.D{{Key: "expireAt", Value: int64(1)}}},
	}

	missing, repairs, skipped := planIndexUpdates(shell, desired, false)
	if len(missing) != 0 || len(repairs) != 0 || len(skipped) != 1 {
		t.Fatalf("index pass missing=%d repairs=%d skipped=%v", len(missing), len(repairs), skipped)
	}

	missing, repairs, skipped = planIndexUpdates(shell, desired, true)
	if len(missing) != 0 || len(skipped) != 0 {
		t.Fatalf("finalize tried to create indexes that already exist: missing=%d skipped=%v", len(missing), skipped)
	}
	if len(repairs) != 1 || repairs[0].desired.Name != "expireAt_1" || repairs[0].existing.Name != "expireAt_1" {
		t.Fatalf("finalize did not plan a TTL shell repair: %+v", repairs)
	}
}

func TestPlanIndexUpdatesLeavesMatchingTTL(t *testing.T) {
	ttl := int32(60)
	desired := []discover.IndexInfo{
		{Name: "expireAt_1", Key: bson.D{{Key: "expireAt", Value: int32(1)}}, ExpireAfterSeconds: &ttl},
	}
	rawType, raw, err := bson.MarshalValue(int32(60))
	if err != nil {
		t.Fatal(err)
	}
	existing := []listedIndex{{
		Name:               "expireAt_1",
		Key:                bson.D{{Key: "expireAt", Value: int32(1)}},
		ExpireAfterSeconds: bson.RawValue{Type: rawType, Value: raw},
	}}

	missing, repairs, skipped := planIndexUpdates(existing, desired, true)
	if len(missing) != 0 || len(repairs) != 0 || len(skipped) != 0 {
		t.Fatalf("matching TTL was not treated as complete: missing=%d repairs=%d skipped=%v", len(missing), len(repairs), skipped)
	}
}

func TestIndexKeyIDIgnoresNumericWidth(t *testing.T) {
	a := indexKeyID(bson.D{{Key: "expireAt", Value: int32(1)}})
	b := indexKeyID(bson.D{{Key: "expireAt", Value: int64(1)}})
	if a != b {
		t.Fatalf("key ids differ: %q vs %q", a, b)
	}
}

func modelNames(t *testing.T, models []mongo.IndexModel) []string {
	t.Helper()
	names := make([]string, 0, len(models))
	for _, model := range models {
		opts := appliedOptions(t, model)
		if opts.Name == nil {
			t.Fatal("index model has no name")
		}
		names = append(names, *opts.Name)
	}
	return names
}

func appliedOptions(t *testing.T, model mongo.IndexModel) options.IndexOptions {
	t.Helper()
	var opts options.IndexOptions
	if model.Options == nil {
		return opts
	}
	for _, set := range model.Options.List() {
		if err := set(&opts); err != nil {
			t.Fatal(err)
		}
	}
	return opts
}
