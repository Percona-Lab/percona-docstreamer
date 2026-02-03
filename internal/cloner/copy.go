package cloner

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/checkpoint"
	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/discover"
	"github.com/Percona-Lab/percona-docstreamer/internal/flow"
	"github.com/Percona-Lab/percona-docstreamer/internal/indexer"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"github.com/Percona-Lab/percona-docstreamer/internal/status"
	"github.com/Percona-Lab/percona-docstreamer/internal/topo"
	"github.com/dustin/go-humanize"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Batch of documents read from the source
type docBatch struct {
	models []mongo.WriteModel
	size   int64
	first  bson.RawValue
	last   bson.RawValue
}

// keyRange defines a segment of a collection by its min and max _id
type keyRange struct {
	Min bson.RawValue
	Max bson.RawValue
}

// Segmenter splits a collection into multiple keyRanges
type Segmenter struct {
	mcoll        *mongo.Collection
	currentRange keyRange
	segmentSize  int64 // in documents
	lock         sync.Mutex
	initialMax   bson.RawValue
}

// Helper to create a RawValue for MinKey/MaxKey
func getRawBound(val interface{}) bson.RawValue {
	raw, _ := bson.Marshal(bson.D{{Key: "v", Value: val}})
	return bson.Raw(raw).Lookup("v")
}

// shouldRetry determines if an error is transient and should be retried
func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "too many cursors") ||
		strings.Contains(msg, "incomplete read") ||
		strings.Contains(msg, "connection") ||
		strings.Contains(msg, "network") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "cursor not found") ||
		strings.Contains(msg, "context canceled") {
		return true
	}
	return false
}

// NewSegmenter creates a segmenter for a collection
func NewSegmenter(ctx context.Context, mcoll *mongo.Collection, collInfo discover.CollectionInfo) (*Segmenter, error) {
	segSize := config.Cfg.Cloner.SegmentSizeDocs
	logging.PrintInfo(fmt.Sprintf("[%s] Initializing Segmenter. Configured Segment Size: %d", mcoll.Name(), segSize), 0)

	// Use Logical Boundaries (MinKey -> MaxKey)
	min := getRawBound(bson.MinKey{})
	max := getRawBound(bson.MaxKey{})

	return &Segmenter{
		mcoll: mcoll,
		currentRange: keyRange{
			Min: min,
			Max: max,
		},
		segmentSize: segSize,
		initialMax:  max,
	}, nil
}

// findSegmentMaxKey finds the _id of the document at the segmentSize offset
func (s *Segmenter) findSegmentMaxKey(ctx context.Context, minKey, maxKey bson.RawValue) (bson.RawValue, error) {
	var minVal interface{}
	if err := minKey.Unmarshal(&minVal); err != nil {
		return bson.RawValue{}, fmt.Errorf("failed to unmarshal minKey: %w", err)
	}

	filter := bson.D{}
	isMin := false
	switch minVal.(type) {
	case bson.MinKey, *bson.MinKey:
		isMin = true
	}

	if !isMin {
		filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: minKey}}}}
	}

	// Range Query Mode requires Sort order.
	opts := options.FindOne().
		SetSort(bson.D{{Key: "_id", Value: int32(1)}}).
		SetSkip(s.segmentSize).
		SetProjection(bson.D{{Key: "_id", Value: 1}})

	var res *mongo.SingleResult

	// Use Configured Retry Logic
	maxRetries := config.Cfg.Cloner.NumRetries
	retryInterval := time.Duration(config.Cfg.Cloner.RetryIntervalMS) * time.Millisecond

	for i := 1; i <= maxRetries; i++ {
		if ctx.Err() != nil {
			return bson.RawValue{}, ctx.Err()
		}
		res = s.mcoll.FindOne(ctx, filter, opts)
		if res.Err() == nil || res.Err() == mongo.ErrNoDocuments {
			break
		}
		if !shouldRetry(res.Err()) {
			break
		}
		logging.PrintWarning(fmt.Sprintf("[%s] Segmenter find error (attempt %d/%d): %v. Retrying...", s.mcoll.Name(), i, maxRetries, res.Err()), 0)
		time.Sleep(retryInterval)
	}

	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return s.initialMax, nil
		}
		return bson.RawValue{}, res.Err()
	}
	raw, err := res.Raw()
	if err != nil {
		return bson.RawValue{}, err
	}
	return raw.Lookup("_id"), nil
}

// Next returns the next segment to be processed.
func (s *Segmenter) Next(ctx context.Context) (keyRange, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.currentRange.Min.Value == nil {
		return keyRange{}, io.EOF
	}

	segmentMaxKey, err := s.findSegmentMaxKey(ctx, s.currentRange.Min, s.currentRange.Max)
	if err != nil {
		return keyRange{}, fmt.Errorf("findSegmentMaxKey failed: %w", err)
	}

	isLastSegment := segmentMaxKey.Equal(s.initialMax)

	segmentToProcess := keyRange{
		Min: s.currentRange.Min,
		Max: segmentMaxKey,
	}

	if isLastSegment {
		s.currentRange.Min = bson.RawValue{}
	} else {
		s.currentRange.Min = segmentMaxKey
	}

	return segmentToProcess, nil
}

func convertIndexes(indexes []discover.IndexInfo) []mongo.IndexModel {
	models := make([]mongo.IndexModel, len(indexes))
	for i, idx := range indexes {
		models[i] = mongo.IndexModel{
			Keys:    idx.Key,
			Options: options.Index().SetName(idx.Name).SetUnique(idx.Unique),
		}
	}
	return models
}

type CopyManager struct {
	sourceClient    *mongo.Client
	targetClient    *mongo.Client
	collInfo        discover.CollectionInfo
	statusMgr       *status.Manager
	checkpointMgr   *checkpoint.Manager
	checkpointDocID string
	initialMaxKey   bson.RawValue
	flowMgr         *flow.Manager
}

// Update: NewCopyManager now accepts flowMgr
func NewCopyManager(source, target *mongo.Client, collInfo discover.CollectionInfo, statusMgr *status.Manager, checkpointMgr *checkpoint.Manager, checkpointDocID string, flowMgr *flow.Manager) *CopyManager {
	return &CopyManager{
		sourceClient:    source,
		targetClient:    target,
		collInfo:        collInfo,
		statusMgr:       statusMgr,
		checkpointMgr:   checkpointMgr,
		checkpointDocID: checkpointDocID,
		flowMgr:         flowMgr,
	}
}

func (cm *CopyManager) Do(ctx context.Context) (int64, bson.Timestamp, error) {
	ns := cm.collInfo.Namespace
	db := cm.collInfo.DB
	coll := cm.collInfo.Coll
	sourceCount := cm.collInfo.Count
	emptyTS := bson.Timestamp{}

	logging.PrintInfo(fmt.Sprintf("[%s] Source collection has %d documents.", ns, sourceCount), 3)

	if sourceCount == 0 {
		logging.PrintInfo(fmt.Sprintf("[%s] Source collection is empty. Skipping.", ns), 3)
		ts, err := topo.ClusterTime(ctx, cm.sourceClient)
		if err != nil {
			return 0, emptyTS, fmt.Errorf("failed to get cluster time for empty coll: %w", err)
		}
		return 0, ts, nil
	}

	sourceColl := cm.sourceClient.Database(db).Collection(coll)
	targetDB := cm.targetClient.Database(db)

	initialTS := emptyTS
	existingTS, found := cm.checkpointMgr.GetResumeTimestamp(ctx, cm.checkpointDocID)

	if !found {
		newTS, err := topo.ClusterTime(ctx, cm.sourceClient)
		if err != nil {
			return 0, emptyTS, fmt.Errorf("failed to get initial cluster time (T0): %w", err)
		}
		initialTS = newTS
		logging.PrintStep(fmt.Sprintf("[%s] Using local start time (T0): %v", ns, initialTS), 3)
	} else {
		initialTS = existingTS
	}

	indexModels := convertIndexes(cm.collInfo.Indexes)

	// --- FIX: Pass sourceColl as the 3rd argument ---
	targetColl, err := indexer.CreateCollectionAndPreloadIndexes(ctx, targetDB, sourceColl, cm.collInfo, indexModels)
	// ------------------------------------------------
	if err != nil {
		return 0, emptyTS, fmt.Errorf("failed to create collection and indexes: %w", err)
	}

	logging.PrintStep(fmt.Sprintf("[%s] Starting parallel data load...", ns), 3)
	docsWritten, err := cm.runDataPipeline(ctx, sourceColl, targetColl)
	if err != nil {
		return 0, emptyTS, fmt.Errorf("data pipeline failed: %w", err)
	}

	// --- RECONCILIATION CHECK ---
	if docsWritten < sourceCount {
		diff := sourceCount - docsWritten
		logging.PrintWarning(fmt.Sprintf("[%s] MISMATCH DETECTED! Source: %d, Copied: %d. Difference: %d. (Potential data loss due to unsupported types or connection errors)", ns, sourceCount, docsWritten, diff), 0)
	} else if docsWritten > sourceCount {
		diff := docsWritten - sourceCount
		logging.PrintInfo(fmt.Sprintf("[%s] COPY SUCCESS (With Retries): Copied %d documents (Source: %d). %d duplicates processed safely.", ns, docsWritten, sourceCount, diff), 3)
	} else {
		logging.PrintSuccess(fmt.Sprintf("[%s] Full Load Verified: %d documents copied.", ns, docsWritten), 3)
	}

	finishTS, err := topo.ClusterTime(ctx, cm.sourceClient)
	if err != nil {
		return 0, emptyTS, fmt.Errorf("failed to get finish cluster time: %w", err)
	}
	logging.PrintSuccess(fmt.Sprintf("[%s] Data pipeline complete. Copied %d documents. Finish time: %v", ns, docsWritten, finishTS), 3)

	if cm.checkpointMgr != nil {
		cm.checkpointMgr.SaveCollectionCheckpoint(ctx, ns, initialTS)
	}

	if err := indexer.FinalizeIndexes(ctx, targetColl, cm.collInfo.Indexes, ns); err != nil {
		logging.PrintError(fmt.Sprintf("[%s] Index finalization failed: %v", ns, err), 3)
	}

	return docsWritten, initialTS, nil
}

func (cm *CopyManager) runDataPipeline(ctx context.Context, sourceColl, targetColl *mongo.Collection) (int64, error) {
	ns := cm.collInfo.Namespace
	numReadWorkers := config.Cfg.Cloner.NumReadWorkers

	// --- AUTO-DETECT LINEAR SCAN MODE ---
	// If discovery flagged this collection, OR if user forced 1 worker via flag.
	if cm.collInfo.UseLinearScan || numReadWorkers == 1 {
		if cm.collInfo.UseLinearScan {
			logging.PrintWarning(fmt.Sprintf("[%s] Auto-Switching to Linear Scan Mode (Unsafe Range Query Detected).", ns), 0)
		} else {
			logging.PrintWarning(fmt.Sprintf("[%s] Running in Linear Scan Mode (Forced by config).", ns), 0)
		}
		return cm.runLinearScan(ctx, sourceColl, targetColl)
	}

	segmentQueue := make(chan keyRange, numReadWorkers)
	docQueue := make(chan docBatch, config.Cfg.Cloner.NumInsertWorkers*2)

	var readWG, insertWG sync.WaitGroup
	errCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 1. Start Insert Workers
	var docsWritten int64
	insertWG.Add(config.Cfg.Cloner.NumInsertWorkers)
	for i := 0; i < config.Cfg.Cloner.NumInsertWorkers; i++ {
		go cm.insertWorker(errCtx, cancel, &insertWG, targetColl, docQueue, &docsWritten, ns, cm.statusMgr)
	}

	// 2. Start Read Workers
	readWG.Add(numReadWorkers)
	for i := 0; i < numReadWorkers; i++ {
		go cm.readWorker(errCtx, cancel, &readWG, sourceColl, segmentQueue, docQueue, ns, i)
	}

	// 3. Start pipeline manager
	go func() {
		readWG.Wait()
		close(docQueue)
	}()

	// 4. Start the Segmenter
	segmenter, err := NewSegmenter(errCtx, sourceColl, cm.collInfo)
	if err != nil {
		close(segmentQueue)
		close(docQueue)
		cancel()
		if err == io.EOF {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to create segmenter: %w", err)
	}

	cm.initialMaxKey = segmenter.initialMax

	go func() {
		defer close(segmentQueue)
		for {
			segment, err := segmenter.Next(errCtx)
			if err != nil {
				if err == io.EOF {
					break
				}
				logging.PrintError(fmt.Sprintf("[%s] Segmenter failed: %v", ns, err), 4)
				cancel()
				break
			}

			select {
			case segmentQueue <- segment:
			case <-errCtx.Done():
				return
			}
		}
	}()

	insertWG.Wait()

	if errCtx.Err() != nil {
		return 0, errCtx.Err()
	}

	return docsWritten, nil
}

// runLinearScan bypasses the segmenter and range queries entirely
func (cm *CopyManager) runLinearScan(ctx context.Context, sourceColl, targetColl *mongo.Collection) (int64, error) {
	ns := cm.collInfo.Namespace
	docQueue := make(chan docBatch, config.Cfg.Cloner.NumInsertWorkers*2)

	var insertWG sync.WaitGroup
	errCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var docsWritten int64
	insertWG.Add(config.Cfg.Cloner.NumInsertWorkers)
	for i := 0; i < config.Cfg.Cloner.NumInsertWorkers; i++ {
		go cm.insertWorker(errCtx, cancel, &insertWG, targetColl, docQueue, &docsWritten, ns, cm.statusMgr)
	}

	// Single linear read process
	go func() {
		defer close(docQueue)

		// LINEAR SCAN: Natural Order (No Sort)
		// This ensures the DB just streams documents in order.
		// It avoids memory sorts and "mixed type" index failures entirely.
		readOpts := options.Find().
			SetBatchSize(int32(config.Cfg.Cloner.ReadBatchSize))
			// NO SORT HERE. This is the fix for Linear Mode.

		cursor, err := sourceColl.Find(errCtx, bson.D{}, readOpts)
		if err != nil {
			logging.PrintError(fmt.Sprintf("[%s] Linear Scan Find failed: %v", ns, err), 0)
			cancel()
			return
		}
		defer cursor.Close(errCtx)

		batch := docBatch{
			models: make([]mongo.WriteModel, 0, config.Cfg.Cloner.InsertBatchSize),
		}
		var firstID, lastID bson.RawValue

		for cursor.Next(errCtx) {
			rawDoc := make(bson.Raw, len(cursor.Current))
			copy(rawDoc, cursor.Current)

			docID, err := rawDoc.LookupErr("_id")
			if err != nil {
				logging.PrintError(fmt.Sprintf("[%s] Failed to lookup _id in document. Skipping. Error: %v", ns, err), 0)
				continue
			}

			if len(batch.models) == 0 {
				firstID = docID
			}
			lastID = docID

			model := mongo.NewReplaceOneModel().
				SetFilter(bson.D{{Key: "_id", Value: docID}}).
				SetReplacement(rawDoc).
				SetUpsert(true)

			batch.models = append(batch.models, model)
			batch.size += int64(len(rawDoc))

			if len(batch.models) >= config.Cfg.Cloner.InsertBatchSize || batch.size >= config.Cfg.Cloner.InsertBatchBytes {
				batch.first = firstID
				batch.last = lastID
				select {
				case docQueue <- batch:
					batch = docBatch{
						models: make([]mongo.WriteModel, 0, config.Cfg.Cloner.InsertBatchSize),
					}
					firstID = bson.RawValue{}
				case <-errCtx.Done():
					return
				}
			}
		}

		if err := cursor.Err(); err != nil {
			logging.PrintError(fmt.Sprintf("[%s] Linear Scan Cursor failed: %v", ns, err), 0)
			cancel()
			return
		}

		if len(batch.models) > 0 {
			batch.first = firstID
			batch.last = lastID
			select {
			case docQueue <- batch:
			case <-errCtx.Done():
				return
			}
		}
	}()

	insertWG.Wait()

	if errCtx.Err() != nil {
		return 0, errCtx.Err()
	}

	return docsWritten, nil
}

func (cm *CopyManager) readWorker(
	ctx context.Context,
	cancel context.CancelFunc,
	wg *sync.WaitGroup,
	coll *mongo.Collection,
	segmentQueue <-chan keyRange,
	docQueue chan<- docBatch,
	ns string,
	workerID int,
) {
	defer wg.Done()
	logging.PrintStep(fmt.Sprintf("[%s] Read Worker %d started", ns, workerID), 4)

	for segment := range segmentQueue {
		// CHECK THROTTLE BEFORE READING
		if cm.flowMgr != nil {
			cm.flowMgr.Wait()
		}
		var minVal, maxVal interface{}
		if err := segment.Min.Unmarshal(&minVal); err != nil {
			logging.PrintError(fmt.Sprintf("Min unmarshal failed: %v", err), 0)
			cancel()
			return
		}
		if segment.Max.Value != nil {
			if err := segment.Max.Unmarshal(&maxVal); err != nil {
				logging.PrintError(fmt.Sprintf("Max unmarshal failed: %v", err), 0)
				cancel()
				return
			}
		}

		isMin := false
		switch minVal.(type) {
		case bson.MinKey, *bson.MinKey:
			isMin = true
		}

		isMax := false
		if segment.Max.Value != nil {
			switch maxVal.(type) {
			case bson.MaxKey, *bson.MaxKey:
				isMax = true
			}
		}

		idFilter := bson.D{}
		if !isMin {
			idFilter = append(idFilter, bson.E{Key: "$gte", Value: segment.Min})
		}
		if segment.Max.Value != nil && !isMax {
			idFilter = append(idFilter, bson.E{Key: "$lte", Value: segment.Max})
		}

		var filter bson.D
		if len(idFilter) == 0 {
			filter = bson.D{}
		} else {
			filter = bson.D{{Key: "_id", Value: idFilter}}
		}

		readOpts := options.Find().
			SetBatchSize(int32(config.Cfg.Cloner.ReadBatchSize)).
			SetSort(bson.D{{Key: "_id", Value: int32(1)}})

		success := false

		maxRetries := config.Cfg.Cloner.NumRetries
		retryInterval := time.Duration(config.Cfg.Cloner.RetryIntervalMS) * time.Millisecond

	RetryLoop:
		for i := 1; i <= maxRetries; i++ {
			if ctx.Err() != nil {
				return
			}

			cursor, err := coll.Find(ctx, filter, readOpts)
			if err != nil {
				if shouldRetry(err) {
					logging.PrintWarning(fmt.Sprintf("[%s] Read Worker %d Find error (attempt %d/%d): %v. Retrying...", ns, workerID, i, maxRetries, err), 0)
					time.Sleep(retryInterval)
					continue RetryLoop
				}
				logging.PrintError(fmt.Sprintf("[%s] Read Worker %d Find fatal error: %v", ns, workerID, err), 4)
				cancel()
				return
			}

			batch := docBatch{
				models: make([]mongo.WriteModel, 0, config.Cfg.Cloner.InsertBatchSize),
			}
			var firstID, lastID bson.RawValue

			for cursor.Next(ctx) {
				rawDoc := make(bson.Raw, len(cursor.Current))
				copy(rawDoc, cursor.Current)

				docID, err := rawDoc.LookupErr("_id")
				if err != nil {
					logging.PrintError(fmt.Sprintf("[%s] Failed to lookup _id in document. Skipping. Error: %v", ns, err), 0)
					continue
				}

				if len(batch.models) == 0 {
					firstID = docID
				}
				lastID = docID

				model := mongo.NewReplaceOneModel().
					SetFilter(bson.D{{Key: "_id", Value: docID}}).
					SetReplacement(rawDoc).
					SetUpsert(true)

				batch.models = append(batch.models, model)
				batch.size += int64(len(rawDoc))

				if len(batch.models) >= config.Cfg.Cloner.InsertBatchSize || batch.size >= config.Cfg.Cloner.InsertBatchBytes {
					batch.first = firstID
					batch.last = lastID
					select {
					case docQueue <- batch:
						batch = docBatch{
							models: make([]mongo.WriteModel, 0, config.Cfg.Cloner.InsertBatchSize),
							size:   0,
						}
						firstID = bson.RawValue{} // Reset
					case <-ctx.Done():
						cursor.Close(ctx)
						return
					}
				}
			}

			if err := cursor.Err(); err != nil {
				cursor.Close(ctx)
				if ctx.Err() != nil {
					return
				}
				if shouldRetry(err) {
					logging.PrintWarning(fmt.Sprintf("[%s] Read Worker %d cursor broken (attempt %d/%d): %v. Retrying segment...", ns, workerID, i, maxRetries, err), 0)
					time.Sleep(retryInterval)
					continue RetryLoop
				}
				logging.PrintError(fmt.Sprintf("[%s] Read Worker %d cursor fatal error: %v", ns, workerID, err), 4)
				cancel()
				return
			}

			if len(batch.models) > 0 {
				batch.first = firstID
				batch.last = lastID
				select {
				case docQueue <- batch:
				case <-ctx.Done():
					cursor.Close(ctx)
					return
				}
			}

			cursor.Close(ctx)
			success = true
			break RetryLoop
		}

		if !success {
			logging.PrintError(fmt.Sprintf("[%s] Read Worker %d failed segment after retries.", ns, workerID), 4)
			cancel()
			return
		}
	}
}

// insertWorker runs BulkWrite with ReplaceOneModels and Retry Logic
func (cm *CopyManager) insertWorker(
	ctx context.Context,
	cancel context.CancelFunc,
	wg *sync.WaitGroup,
	coll *mongo.Collection,
	queue <-chan docBatch,
	counter *int64,
	ns string,
	statusMgr *status.Manager,
) {
	defer wg.Done()

	bulkWriteOpts := options.BulkWrite().
		SetOrdered(false).
		SetBypassDocumentValidation(true)

	maxRetries := config.Cfg.Cloner.NumRetries
	retryInterval := time.Duration(config.Cfg.Cloner.RetryIntervalMS) * time.Millisecond
	writeTimeout := time.Duration(config.Cfg.Cloner.WriteTimeoutMS) * time.Millisecond

	for batch := range queue {
		if len(batch.models) == 0 {
			continue
		}

		var res *mongo.BulkWriteResult
		var err error
		var docCount int64

		// --- RETRY LOOP ---
		for i := 1; i <= maxRetries; i++ {
			if ctx.Err() != nil {
				return
			}

			start := time.Now()

			// Detach from parent cancel context for the specific write timeout
			// This ensures a network hang doesn't block indefinitely,
			// but we still respect overall app shutdown (handled by checking ctx.Err at top of loop)
			writeCtx, writeCancel := context.WithTimeout(context.Background(), writeTimeout)
			res, err = coll.BulkWrite(writeCtx, batch.models, bulkWriteOpts)
			writeCancel()

			// Capture count even on partial success/failure
			docCount = 0
			if res != nil {
				docCount = res.InsertedCount + res.UpsertedCount + res.MatchedCount
			}

			// Log the attempt duration
			logging.LogFullLoadBatchOp(start, ns, docCount, batch.size, err)

			if err == nil {
				break // Success!
			}

			// If it's a retryable error, wait and try again
			if shouldRetry(err) {
				logging.PrintWarning(fmt.Sprintf("[%s] BulkWrite error (attempt %d/%d): %v. Retrying...", ns, i, maxRetries, err), 0)
				time.Sleep(retryInterval)
				continue
			}

			// If it's not retryable (e.g. auth error, bad query), break immediately
			break
		}

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Fatal error after retries
			logging.PrintError(fmt.Sprintf("[%s] BulkWrite failed: %v", ns, err), 4)
			cancel()
			return
		}

		atomic.AddInt64(counter, docCount)

		if statusMgr != nil {
			statusMgr.AddClonedBytes(batch.size)
		}

		// Success Log
		elapsed := time.Duration(0)
		if config.Cfg.Logging.Level == "debug" {
			logging.PrintStep(fmt.Sprintf("[%s] Processed batch: %d inserted, %d replaced. (%s) in %s. Range: [%v - %v]",
				ns, res.InsertedCount+res.UpsertedCount, res.ModifiedCount, humanize.Bytes(uint64(batch.size)), elapsed, batch.first, batch.last), 4)
		} else {
			logging.PrintStep(fmt.Sprintf("[%s] Processed batch: %d inserted, %d replaced. (%s)",
				ns, res.InsertedCount+res.UpsertedCount, res.ModifiedCount, humanize.Bytes(uint64(batch.size))), 4)
		}
	}
}
