package cloner

import (
	"testing"

	"github.com/Percona-Lab/percona-docstreamer/internal/discover"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestConvertIndexesOmitsTTL(t *testing.T) {
	ttl := int32(3600)
	indexes := []discover.IndexInfo{
		{Name: "status_1", Key: bson.D{{Key: "status", Value: int32(1)}}},
		{
			Name:               "expireAt_1",
			Key:                bson.D{{Key: "expireAt", Value: int32(1)}},
			ExpireAfterSeconds: &ttl,
		},
		{Name: "userId_1", Key: bson.D{{Key: "userId", Value: int32(1)}}, Unique: true},
	}

	models := convertIndexes(indexes)
	if len(models) != 2 {
		t.Fatalf("preload index count = %d, want 2 (TTL omitted)", len(models))
	}
	for _, model := range models {
		opts := indexOptions(t, model)
		if opts.Name != nil && *opts.Name == "expireAt_1" {
			t.Fatal("TTL index was included in preload indexes")
		}
		if opts.ExpireAfterSeconds != nil {
			t.Fatalf("preload index %v has expireAfterSeconds", opts.Name)
		}
	}
}

func indexOptions(t *testing.T, model mongo.IndexModel) options.IndexOptions {
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
