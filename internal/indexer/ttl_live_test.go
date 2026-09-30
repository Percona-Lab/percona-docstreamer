package indexer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/discover"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const liveTTLDB = "docstreamer_ttl_test"

// TestLiveTTLFinalize creates a collection with a real TTL index on DocumentDB
// and checks both target shapes on MongoDB:
//   - the pre-fix shell (same index name, no expireAfterSeconds)
//   - a collection prepared the way the cloner does now (TTL omitted)
func TestLiveTTLFinalize(t *testing.T) {
	if os.Getenv("DOCSTREAMER_TTL_LIVE") != "1" {
		t.Skip("set DOCSTREAMER_TTL_LIVE=1 to run against the lab DocumentDB and MongoDB clusters")
	}

	docdbUser := os.Getenv("DOCSTREAMER_DOCDB_USER")
	docdbPass := os.Getenv("DOCSTREAMER_DOCDB_PASS")
	mongoUser := os.Getenv("DOCSTREAMER_MONGO_USER")
	mongoPass := os.Getenv("DOCSTREAMER_MONGO_PASS")
	if docdbUser == "" || docdbPass == "" || mongoUser == "" || mongoPass == "" {
		t.Fatal("DOCSTREAMER_DOCDB_USER, DOCSTREAMER_DOCDB_PASS, DOCSTREAMER_MONGO_USER, and DOCSTREAMER_MONGO_PASS are required")
	}

	caPath := os.Getenv("HOME") + "/Documents/global-bundle.pem"
	if _, err := os.Stat(caPath); err != nil {
		t.Fatalf("DocumentDB CA file: %v", err)
	}

	config.Cfg = &config.Config{
		Migration: config.MigrationConfig{
			ExcludeDBs:         []string{"admin", "local", "config"},
			DiscoveryTimeoutMS: 120000,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	docdbURI := (&url.URL{
		Scheme: "mongodb",
		User:   url.UserPassword(docdbUser, docdbPass),
		Host:   "localhost:7777",
		Path:   "/",
		RawQuery: "tls=true&tlsAllowInvalidHostnames=true&retryWrites=false&directConnection=true&authSource=admin&tlsCAFile=" +
			url.QueryEscape(caPath),
	}).String()
	mongoURI := (&url.URL{
		Scheme:   "mongodb",
		User:     url.UserPassword(mongoUser, mongoPass),
		Host:     "dan-ps-lab-mongos00.tp.int.percona.com:27017",
		Path:     "/",
		RawQuery: "authSource=admin&serverSelectionTimeoutMS=15000",
	}).String()

	docdb, err := mongo.Connect(options.Client().ApplyURI(docdbURI).SetTLSConfig(&tls.Config{InsecureSkipVerify: true}))
	if err != nil {
		t.Fatalf("connect DocumentDB: %v", err)
	}
	target, err := mongo.Connect(options.Client().ApplyURI(mongoURI))
	if err != nil {
		t.Fatalf("connect MongoDB: %v", err)
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		_ = docdb.Database(liveTTLDB).Drop(dropCtx)
		_ = target.Database(liveTTLDB).Drop(dropCtx)
		_ = docdb.Disconnect(dropCtx)
		_ = target.Disconnect(dropCtx)
	})

	if err := docdb.Database(liveTTLDB).Drop(ctx); err != nil {
		t.Fatalf("drop source db: %v", err)
	}
	if err := target.Database(liveTTLDB).Drop(ctx); err != nil {
		t.Fatalf("drop target db: %v", err)
	}

	source := docdb.Database(liveTTLDB).Collection("sessions")
	expireAt := time.Now().UTC().Add(48 * time.Hour)
	docs := []any{
		bson.D{{Key: "userId", Value: "u1"}, {Key: "status", Value: "open"}, {Key: "expireAt", Value: expireAt}},
		bson.D{{Key: "userId", Value: "u2"}, {Key: "status", Value: "closed"}, {Key: "expireAt", Value: expireAt}},
		bson.D{{Key: "userId", Value: "u3"}, {Key: "status", Value: "open"}, {Key: "expireAt", Value: expireAt}},
	}
	if _, err := source.InsertMany(ctx, docs); err != nil {
		t.Fatalf("insert sample documents: %v", err)
	}
	if _, err := source.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "status", Value: int32(1)}},
		Options: options.Index().SetName("status_1"),
	}); err != nil {
		t.Fatalf("create status index on source: %v", err)
	}
	if _, err := source.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "userId", Value: int32(1)}},
		Options: options.Index().SetName("userId_1").SetUnique(true),
	}); err != nil {
		t.Fatalf("create unique index on source: %v", err)
	}
	const ttlSeconds int32 = 3600
	if _, err := source.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "expireAt", Value: int32(1)}},
		Options: options.Index().SetName("expireAt_1").SetExpireAfterSeconds(ttlSeconds),
	}); err != nil {
		t.Fatalf("create TTL index on source: %v", err)
	}

	collections, err := discover.DiscoverCollections(ctx, docdb)
	if err != nil {
		t.Fatalf("discover source: %v", err)
	}
	var info discover.CollectionInfo
	found := false
	for _, coll := range collections {
		if coll.Namespace == liveTTLDB+".sessions" {
			info = coll
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("discovery did not return %s.sessions (saw %d collections)", liveTTLDB, len(collections))
	}

	var ttlIndex *discover.IndexInfo
	for i := range info.Indexes {
		idx := &info.Indexes[i]
		keyDesc := make([]string, 0, len(idx.Key))
		for _, elem := range idx.Key {
			keyDesc = append(keyDesc, fmt.Sprintf("%s:%T", elem.Key, elem.Value))
		}
		t.Logf("discovered %s key=[%s] ttl=%v unique=%v", idx.Name, strings.Join(keyDesc, ","), idx.ExpireAfterSeconds, idx.Unique)
		if idx.ExpireAfterSeconds != nil {
			ttlIndex = idx
		}
	}
	if ttlIndex == nil || *ttlIndex.ExpireAfterSeconds != ttlSeconds {
		t.Fatalf("source TTL index was not discovered: %+v", info.Indexes)
	}

	shell := target.Database(liveTTLDB).Collection("sessions_shell")
	preload := make([]mongo.IndexModel, 0, len(info.Indexes))
	for _, idx := range info.Indexes {
		preload = append(preload, mongo.IndexModel{
			Keys:    idx.Key,
			Options: options.Index().SetName(idx.Name).SetUnique(idx.Unique),
		})
	}
	if _, err := shell.Indexes().CreateMany(ctx, preload); err != nil {
		t.Fatalf("create pre-fix index shell: %v", err)
	}

	_, err = shell.Indexes().CreateOne(ctx, indexModel(*ttlIndex, true))
	if err == nil {
		t.Fatal("creating the TTL index on top of the shell succeeded; expected an already-exists error")
	}
	if !indexAlreadyExists(err) {
		t.Fatalf("shell conflict error = %v", err)
	}
	t.Logf("pre-fix finalize create failed as reported: %v", err)

	ns := liveTTLDB + ".sessions_shell"
	if err := FinalizeIndexes(ctx, shell, info.Indexes, ns, false); err != nil {
		t.Fatalf("index pass: %v", err)
	}
	assertTTL(t, ctx, shell, ttlIndex.Name, nil)

	if err := FinalizeIndexes(ctx, shell, info.Indexes, ns, true); err != nil {
		t.Fatalf("finalize shell: %v", err)
	}
	assertTTL(t, ctx, shell, ttlIndex.Name, ttlIndex.ExpireAfterSeconds)
	assertIndexPresent(t, ctx, shell, "status_1")
	assertIndexPresent(t, ctx, shell, "userId_1")

	if err := FinalizeIndexes(ctx, shell, info.Indexes, ns, true); err != nil {
		t.Fatalf("second finalize: %v", err)
	}
	assertTTL(t, ctx, shell, ttlIndex.Name, ttlIndex.ExpireAfterSeconds)

	deferred := target.Database(liveTTLDB).Collection("sessions_deferred")
	var nonTTL []mongo.IndexModel
	for _, idx := range info.Indexes {
		if idx.ExpireAfterSeconds != nil {
			continue
		}
		nonTTL = append(nonTTL, mongo.IndexModel{
			Keys:    idx.Key,
			Options: options.Index().SetName(idx.Name).SetUnique(idx.Unique),
		})
	}
	if _, err := deferred.Indexes().CreateMany(ctx, nonTTL); err != nil {
		t.Fatalf("create deferred-preload indexes: %v", err)
	}
	deferredNS := liveTTLDB + ".sessions_deferred"
	if err := FinalizeIndexes(ctx, deferred, info.Indexes, deferredNS, false); err != nil {
		t.Fatalf("index pass on deferred collection: %v", err)
	}
	assertIndexAbsent(t, ctx, deferred, ttlIndex.Name)

	if err := FinalizeIndexes(ctx, deferred, info.Indexes, deferredNS, true); err != nil {
		t.Fatalf("finalize deferred collection: %v", err)
	}
	assertTTL(t, ctx, deferred, ttlIndex.Name, ttlIndex.ExpireAfterSeconds)
}

func indexAlreadyExists(err error) bool {
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) && (cmdErr.HasErrorCode(68) || cmdErr.HasErrorCode(85) || cmdErr.HasErrorCode(86)) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "indexoptionsconflict")
}

func assertTTL(t *testing.T, ctx context.Context, coll *mongo.Collection, name string, want *int32) {
	t.Helper()
	got, ok := indexTTL(t, ctx, coll, name)
	if want == nil {
		if ok {
			t.Fatalf("index %s has expireAfterSeconds=%d, want none", name, got)
		}
		return
	}
	if !ok {
		t.Fatalf("index %s has no expireAfterSeconds, want %d", name, *want)
	}
	if got != int64(*want) {
		t.Fatalf("index %s expireAfterSeconds=%d, want %d", name, got, *want)
	}
}

func assertIndexPresent(t *testing.T, ctx context.Context, coll *mongo.Collection, name string) {
	t.Helper()
	if _, ok := findListedIndex(t, ctx, coll, name); !ok {
		t.Fatalf("index %s is missing", name)
	}
}

func assertIndexAbsent(t *testing.T, ctx context.Context, coll *mongo.Collection, name string) {
	t.Helper()
	if _, ok := findListedIndex(t, ctx, coll, name); ok {
		t.Fatalf("index %s exists, want it deferred", name)
	}
}

func indexTTL(t *testing.T, ctx context.Context, coll *mongo.Collection, name string) (int64, bool) {
	t.Helper()
	idx, ok := findListedIndex(t, ctx, coll, name)
	if !ok {
		t.Fatalf("index %s is missing", name)
	}
	return idx.ttlSeconds()
}

func findListedIndex(t *testing.T, ctx context.Context, coll *mongo.Collection, name string) (listedIndex, bool) {
	t.Helper()
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	var indexes []listedIndex
	if err := cursor.All(ctx, &indexes); err != nil {
		t.Fatalf("decode indexes: %v", err)
	}
	for _, idx := range indexes {
		if idx.Name == name {
			return idx, true
		}
	}
	return listedIndex{}, false
}
