package validator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/flow"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"github.com/Percona-Lab/percona-docstreamer/internal/status"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type ValidationResult struct {
	DocID  string `json:"docId"`
	Keys   bson.D `json:"keys,omitempty"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

type ValidationRecord struct {
	Namespace  string    `bson:"namespace" json:"namespace"`
	DocID      string    `bson:"doc_id" json:"docId"`
	Keys       bson.D    `bson:"keys,omitempty" json:"keys,omitempty"`
	Status     string    `bson:"status" json:"status"`
	Reason     string    `bson:"reason,omitempty" json:"reason,omitempty"`
	RetryCount int       `bson:"retry_count" json:"retryCount"`
	DetectedAt time.Time `bson:"detected_at" json:"detectedAt"`
}

type ValidationStats struct {
	Namespace     string `bson:"_id" json:"namespace"`
	TotalChecked  int64  `bson:"total_checked" json:"totalChecked"`
	MismatchFound int64  `bson:"mismatch_found" json:"mismatchFound"`
	MismatchFixed int64  `bson:"mismatch_fixed" json:"mismatchFixed"`
}

type Store struct {
	client    *mongo.Client
	failures  *mongo.Collection
	stats     *mongo.Collection
	audit     *mongo.Collection
	flowMgr   *flow.Manager
	statusMgr *status.Manager

	// BUFFERED CHANNELS:
	// Capacity set to exactly 1x BatchSize.
	// This ensures maximum memory usage is bounded to ~2 batches
	// (1 in channel + 1 in worker), preventing OOM on large records.
	auditChan   chan interface{}
	outcomeChan chan outcomeOp

	quit chan struct{}
	wg   sync.WaitGroup
}

type outcomeOp struct {
	opType string
	ns     string
	res    ValidationResult
}

func NewStore(client *mongo.Client, flowMgr *flow.Manager, statusMgr *status.Manager) *Store {
	dbName := config.Cfg.Migration.MetadataDB
	auditCollName := config.Cfg.Migration.ValidationAuditCollection
	if auditCollName == "" {
		auditCollName = "validation_audit"
	}

	batchSize := config.Cfg.Validation.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	s := &Store{
		client:      client,
		failures:    client.Database(dbName).Collection(config.Cfg.Migration.ValidationFailuresCollection),
		stats:       client.Database(dbName).Collection(config.Cfg.Migration.ValidationStatsCollection),
		audit:       client.Database(dbName).Collection(auditCollName),
		flowMgr:     flowMgr,
		statusMgr:   statusMgr,
		auditChan:   make(chan interface{}, batchSize),
		outcomeChan: make(chan outcomeOp, batchSize),
		quit:        make(chan struct{}),
	}
	s.ensureIndexes()
	s.startWorkers()
	return s
}

func (s *Store) Close() {
	close(s.quit)
	close(s.outcomeChan)
	close(s.auditChan)
	s.wg.Wait()
}

func (s *Store) ensureIndexes() {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.failures.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "namespace", Value: 1}, {Key: "doc_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		})
		s.failures.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{{Key: "status", Value: 1}, {Key: "retry_count", Value: 1}},
		})
		s.audit.Indexes().CreateMany(ctx, []mongo.IndexModel{
			{Keys: bson.D{{Key: "namespace", Value: 1}, {Key: "timestamp", Value: -1}}},
			{Keys: bson.D{{Key: "timestamp", Value: 1}}},
			{Keys: bson.D{{Key: "action", Value: 1}}},
		})
	}()
}

func (s *Store) startWorkers() {
	s.wg.Add(2)
	go s.runOutcomeWorker()
	go s.runAuditWorker()
}

func (s *Store) LogAudit(ns, docID, action, reason string) {
	doc := bson.M{
		"timestamp": time.Now().UTC(),
		"namespace": ns,
		"doc_id":    docID,
		"action":    action,
		"reason":    reason,
	}
	// Blocks if channel is full (Safety against OOM)
	select {
	case s.auditChan <- doc:
	case <-s.quit:
	}
}

func (s *Store) RegisterOutcome(ctx context.Context, ns string, res ValidationResult) {
	op := outcomeOp{opType: "upsert", ns: ns, res: res}
	if res.Status == "valid" {
		op.opType = "delete"
	}

	// This blocks if channel is full -> creates backpressure
	select {
	case s.outcomeChan <- op:
	case <-ctx.Done():
	case <-s.quit:
	}
}

func (s *Store) Invalidate(ctx context.Context, ns, docID string) {
	op := outcomeOp{opType: "delete", ns: ns, res: ValidationResult{DocID: docID}}
	select {
	case s.outcomeChan <- op:
	case <-ctx.Done():
	case <-s.quit:
	}
}

func (s *Store) runOutcomeWorker() {
	defer s.wg.Done()

	batchSize := config.Cfg.Validation.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	batch := make([]outcomeOp, 0, batchSize)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case op, ok := <-s.outcomeChan:
			if !ok {
				// Channel closed, flush remaining
				if len(batch) > 0 {
					s.writeChunk(batch)
				}
				return
			}
			batch = append(batch, op)
			if len(batch) >= batchSize {
				s.writeChunk(batch)
				batch = make([]outcomeOp, 0, batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				if s.flowMgr != nil {
					if isPaused, _, _ := s.flowMgr.GetStatus(); isPaused {
						continue
					}
				}
				s.writeChunk(batch)
				batch = make([]outcomeOp, 0, batchSize)
			}
		}
	}
}

func (s *Store) writeChunk(batch []outcomeOp) {
	batchesByNS := make(map[string][]mongo.WriteModel)
	type statDelta struct {
		total int64
	}
	deltas := make(map[string]*statDelta)
	var totalChecked int64

	for _, op := range batch {
		if _, ok := deltas[op.ns]; !ok {
			deltas[op.ns] = &statDelta{}
		}
		if op.res.Status != "" || op.res.Status == "valid" {
			deltas[op.ns].total++
			totalChecked++
		}

		if op.opType == "delete" {
			filter := bson.D{{Key: "namespace", Value: op.ns}, {Key: "doc_id", Value: op.res.DocID}}
			if op.res.Status == "" {
				filter = append(filter, bson.E{Key: "status", Value: bson.D{{Key: "$ne", Value: "safety_deferred_delete"}}})
			}
			batchesByNS[op.ns] = append(batchesByNS[op.ns], mongo.NewDeleteOneModel().SetFilter(filter))
		} else {
			setOnInsert := bson.D{
				{Key: "detected_at", Value: time.Now().UTC()},
				{Key: "keys", Value: op.res.Keys},
			}
			setFields := bson.D{
				{Key: "status", Value: op.res.Status},
				{Key: "reason", Value: op.res.Reason},
			}
			update := bson.D{{Key: "$setOnInsert", Value: setOnInsert}}

			if op.res.Status == "hot_key_waiting" || op.res.Status == "batch_failed" ||
				op.res.Status == "healed_pending_verify" || op.res.Status == "safety_deferred_delete" {
				setFields = append(setFields, bson.E{Key: "retry_count", Value: 0})
			} else {
				update = append(update, bson.E{Key: "$inc", Value: bson.D{{Key: "retry_count", Value: 1}}})
			}
			update = append(update, bson.E{Key: "$set", Value: setFields})

			filter := bson.D{{Key: "namespace", Value: op.ns}, {Key: "doc_id", Value: op.res.DocID}}
			batchesByNS[op.ns] = append(batchesByNS[op.ns], mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for ns, models := range batchesByNS {
		res, err := s.failures.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))

		if err != nil {
			logging.PrintWarning(fmt.Sprintf("[VAL STORE] DB congested. Re-queued %d items for %s.", len(models), ns), 0)

			// Re-queue items back into the channel using a goroutine to avoid deadlock
			// The channel might be full, but we must retry. This goroutine allows the
			// writeChunk function to return, freeing up the worker.
			go func(nsToRetry string) {
				for _, op := range batch {
					if op.ns == nsToRetry {
						select {
						case s.outcomeChan <- op:
						case <-s.quit:
							return
						}
					}
				}
			}(ns)

			time.Sleep(1 * time.Second)
		} else {
			if s.statusMgr != nil {
				s.statusMgr.UpdateValidationStats(totalChecked, res.UpsertedCount, res.DeletedCount, 0, 0)
			}
			s.updateStats(ctx, ns, deltas[ns].total, res.UpsertedCount, res.DeletedCount)
		}
	}
}

func (s *Store) runAuditWorker() {
	defer s.wg.Done()

	batchSize := config.Cfg.Validation.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	batch := make([]interface{}, 0, batchSize)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case doc, ok := <-s.auditChan:
			if !ok {
				if len(batch) > 0 {
					s.flushAuditBatch(batch)
				}
				return
			}
			batch = append(batch, doc)
			if len(batch) >= batchSize {
				s.flushAuditBatch(batch)
				batch = make([]interface{}, 0, batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				s.flushAuditBatch(batch)
				batch = make([]interface{}, 0, batchSize)
			}
		}
	}
}

func (s *Store) flushAuditBatch(batch []interface{}) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.audit.InsertMany(ctx, batch); err != nil {
		logging.PrintWarning(fmt.Sprintf("[VAL STORE] Audit log write failed: %v", err), 0)
	}
}

func (s *Store) updateStats(ctx context.Context, ns string, totalInc, foundInc, fixedInc int64) {
	if totalInc == 0 && foundInc == 0 && fixedInc == 0 {
		return
	}
	opts := options.UpdateOne().SetUpsert(true)
	filter := bson.D{{Key: "_id", Value: ns}}
	update := bson.D{
		{Key: "$inc", Value: bson.D{
			{Key: "total_checked", Value: totalInc},
			{Key: "mismatch_found", Value: foundInc},
			{Key: "mismatch_fixed", Value: fixedInc},
		}},
		{Key: "$set", Value: bson.D{{Key: "last_updated", Value: time.Now().UTC()}}},
	}
	go func() {
		localCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s.stats.UpdateOne(localCtx, filter, update, opts)
	}()
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

func (s *Store) GetEligibleFailures(ctx context.Context, maxRetries int) ([]ValidationRecord, error) {
	filter := getEligibleFilter(maxRetries)
	cursor, err := s.failures.Find(ctx, filter)
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

func (s *Store) HasEligibleFailures(ctx context.Context, maxRetries int) bool {
	filter := getEligibleFilter(maxRetries)
	count, err := s.failures.CountDocuments(ctx, filter, options.Count().SetLimit(1))
	if err != nil {
		return false
	}
	return count > 0
}

func getEligibleFilter(maxRetries int) bson.D {
	return bson.D{
		{Key: "status", Value: bson.D{{Key: "$ne", Value: "manual_validation_required"}}},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "retry_count", Value: bson.D{{Key: "$lt", Value: maxRetries}}}},
			bson.D{{Key: "retry_count", Value: bson.D{{Key: "$exists", Value: false}}}},
		}},
	}
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

func (s *Store) Reset(ctx context.Context) error {
	if err := s.failures.Drop(ctx); err != nil {
		return err
	}
	if err := s.stats.Drop(ctx); err != nil {
		return err
	}
	if err := s.audit.Drop(ctx); err != nil {
		return err
	}
	s.ensureIndexes()
	return nil
}

func (s *Store) Reconcile(ctx context.Context) error {
	logging.PrintInfo("[VAL STORE] Starting stats reconciliation...", 0)

	auditPipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "action", Value: bson.D{{Key: "$in", Value: bson.A{"ghost_fixed", "content_mismatch_fixed", "orphan_deleted", "missing_target_fixed"}}}}}}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$namespace"}, {Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
	}
	fixedCounts := make(map[string]int64)
	if cur, err := s.audit.Aggregate(ctx, auditPipeline); err == nil {
		defer cur.Close(ctx)
		for cur.Next(ctx) {
			var res struct {
				NS    string `bson:"_id"`
				Count int64  `bson:"count"`
			}
			if cur.Decode(&res) == nil {
				fixedCounts[res.NS] = res.Count
			}
		}
	}

	failPipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$namespace"}, {Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
	}
	pendingCounts := make(map[string]int64)
	if cur, err := s.failures.Aggregate(ctx, failPipeline); err == nil {
		defer cur.Close(ctx)
		for cur.Next(ctx) {
			var res struct {
				NS    string `bson:"_id"`
				Count int64  `bson:"count"`
			}
			if cur.Decode(&res) == nil {
				pendingCounts[res.NS] = res.Count
			}
		}
	}

	statsCursor, err := s.stats.Find(ctx, bson.D{})
	existingStats := make(map[string]bool)
	if err == nil {
		defer statsCursor.Close(ctx)
		for statsCursor.Next(ctx) {
			var res ValidationStats
			if statsCursor.Decode(&res) == nil {
				existingStats[res.Namespace] = true
			}
		}
	}

	allNS := make(map[string]bool)
	for ns := range fixedCounts {
		allNS[ns] = true
	}
	for ns := range pendingCounts {
		allNS[ns] = true
	}
	for ns := range existingStats {
		allNS[ns] = true
	}

	for ns := range allNS {
		fixed := fixedCounts[ns]
		pending := pendingCounts[ns]
		found := fixed + pending
		update := bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "mismatch_fixed", Value: fixed},
				{Key: "mismatch_found", Value: found},
				{Key: "last_updated", Value: time.Now().UTC()},
			}},
		}
		s.stats.UpdateOne(ctx, bson.D{{Key: "_id", Value: ns}}, update, options.UpdateOne().SetUpsert(true))
		logging.PrintInfo(fmt.Sprintf("[VAL STORE] Reconciled %s: Found=%d, Fixed=%d, Pending=%d", ns, found, fixed, pending), 0)
	}
	return nil
}
