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

type docBatch struct {
	docs  []bson.Raw
	size  int64
	first bson.RawValue
	last  bson.RawValue
}

type keyRange struct {
	Min bson.RawValue
	Max bson.RawValue
}

type Segmenter struct {
	mcoll        *mongo.Collection
	currentRange keyRange
	segmentSize  int64 // number of documents
	lock         sync.Mutex
	initialMax   bson.RawValue
}

func getRawBound(val interface{}) bson.RawValue {
	raw, _ := bson.Marshal(bson.D{{Key: "v", Value: val}})
	return bson.Raw(raw).Lookup("v")
}

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

func NewSegmenter(ctx context.Context, mcoll *mongo.Collection, collInfo discover.CollectionInfo) (*Segmenter, error) {
	segSize := config.Cfg.Cloner.SegmentSizeDocs
	logging.PrintInfo(fmt.Sprintf("[%s] Initializing Segmenter. Configured Segment Size: %d", mcoll.Name(), segSize), 0)

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

type ShardKeyPath struct {
	Key  string
	Path []string
}

type CopyManager struct {
	sourceClient    *mongo.Client
	targetClient    *mongo.Client
	CollInfo        discover.CollectionInfo
	statusMgr       *status.Manager
	checkpointMgr   *checkpoint.Manager
	checkpointDocID string
	initialMaxKey   bson.RawValue
	flowMgr         *flow.Manager
	shardKeys       []string
	shardKeyPaths   []ShardKeyPath
	initialTS       bson.Timestamp
}

func NewCopyManager(source, target *mongo.Client, collInfo discover.CollectionInfo, statusMgr *status.Manager, checkpointMgr *checkpoint.Manager, checkpointDocID string, flowMgr *flow.Manager) *CopyManager {
	return &CopyManager{
		sourceClient:    source,
		targetClient:    target,
		CollInfo:        collInfo,
		statusMgr:       statusMgr,
		checkpointMgr:   checkpointMgr,
		checkpointDocID: checkpointDocID,
		flowMgr:         flowMgr,
		shardKeys:       []string{},
		shardKeyPaths:   []ShardKeyPath{},
	}
}

func (cm *CopyManager) Prepare(ctx context.Context) error {
	ns := cm.CollInfo.Namespace
	db := cm.CollInfo.DB
	coll := cm.CollInfo.Coll
	sourceCount := cm.CollInfo.Count
	emptyTS := bson.Timestamp{}

	logging.PrintInfo(fmt.Sprintf("[%s] Preparing collection. Source documents: %d", ns, sourceCount), 3)

	// 1. Get Cluster Time
	initialTS := emptyTS
	existingTS, found := cm.checkpointMgr.GetResumeTimestamp(ctx, cm.checkpointDocID)
	if !found {
		newTS, err := topo.ClusterTime(ctx, cm.sourceClient)
		if err != nil {
			return fmt.Errorf("failed to get T0: %w", err)
		}
		initialTS = newTS
	} else {
		initialTS = existingTS
	}
	cm.initialTS = initialTS

	sourceColl := cm.sourceClient.Database(db).Collection(coll)
	targetDB := cm.targetClient.Database(db)

	// 2. Create Collection, Shard, Split, Distribute (Balancer is STOPPED inside here)
	var indexModels []mongo.IndexModel

	if config.Cfg.Cloner.PostponeIndexCreation {
		logging.PrintInfo(fmt.Sprintf("[%s] Index creation POSTPONED, only shard key and _id default indexes will be created.", ns), 0)
		// Pass empty list. applySharding (inside the function below) will still create the required Shard Key index.
		indexModels = []mongo.IndexModel{}
	} else {
		// Standard behavior: Pre-load all indexes before copying
		indexModels = convertIndexes(cm.CollInfo.Indexes)
	}

	_, err := indexer.CreateCollectionAndPreloadIndexes(ctx, targetDB, sourceColl, cm.CollInfo, indexModels)
	if err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}

	// 3. Shard Key Discovery (for Copy Phase)
	foundInConfig := false
	for _, rule := range config.Cfg.Sharding {
		if rule.Namespace == ns {
			cm.shardKeys = rule.GetKeys()
			if len(cm.shardKeys) > 0 {
				foundInConfig = true
			}
			break
		}
	}
	if !foundInConfig {
		keys, err := discover.GetShardKey(ctx, cm.targetClient, ns)
		if err == nil && len(keys) > 0 {
			cm.shardKeys = keys
		}
	}

	// 4. Pre-calculate ShardKeyPaths for fast string splitting during Fallback processing
	cm.shardKeyPaths = make([]ShardKeyPath, 0, len(cm.shardKeys))
	for _, key := range cm.shardKeys {
		if key == "_id" {
			continue
		}
		cm.shardKeyPaths = append(cm.shardKeyPaths, ShardKeyPath{
			Key:  key,
			Path: strings.Split(key, "."),
		})
	}

	return nil
}

func (cm *CopyManager) Run(ctx context.Context) (int64, bson.Timestamp, error) {
	ns := cm.CollInfo.Namespace
	db := cm.CollInfo.DB
	coll := cm.CollInfo.Coll

	if cm.CollInfo.Count == 0 {
		return 0, cm.initialTS, nil
	}

	sourceColl := cm.sourceClient.Database(db).Collection(coll)
	targetColl := cm.targetClient.Database(db).Collection(coll)

	logging.PrintStep(fmt.Sprintf("[%s] Starting parallel data load...", ns), 3)
	docsWritten, err := cm.runDataPipeline(ctx, sourceColl, targetColl)
	if err != nil {
		return 0, cm.initialTS, fmt.Errorf("data pipeline failed: %w", err)
	}

	// --- RECONCILIATION CHECK ---
	if docsWritten < cm.CollInfo.Count {
		diff := cm.CollInfo.Count - docsWritten
		logging.PrintWarning(fmt.Sprintf("[%s] MISMATCH DETECTED! Source: %d, Copied: %d. Difference: %d.", ns, cm.CollInfo.Count, docsWritten, diff), 0)
	} else {
		logging.PrintSuccess(fmt.Sprintf("[%s] Full Load Verified: %d documents copied.", ns, docsWritten), 3)
	}

	logging.PrintSuccess(fmt.Sprintf("[%s] Data pipeline complete. Copied %d documents.", ns, docsWritten), 3)

	if cm.checkpointMgr != nil {
		cm.checkpointMgr.SaveCollectionCheckpoint(ctx, ns, cm.initialTS)
	}

	if config.Cfg.Cloner.PostponeIndexCreation {
		logging.PrintInfo(fmt.Sprintf("[%s] Index finalization POSTPONED. Run 'docStreamer index' or 'docStreamer finalize' to apply.", ns), 3)
	} else {
		if err := indexer.FinalizeIndexes(ctx, targetColl, cm.CollInfo.Indexes, ns); err != nil {
			logging.PrintError(fmt.Sprintf("[%s] Index finalization failed: %v", ns, err), 3)
		}
	}

	return docsWritten, cm.initialTS, nil
}

// buildFilter constructs the filter for a document based on Shard Keys and _id.
func (cm *CopyManager) buildFilter(rawDoc bson.Raw) (bson.D, error) {
	docID, err := rawDoc.LookupErr("_id")
	if err != nil {
		return nil, fmt.Errorf("missing _id")
	}

	// 1. Add Shard Keys first (Routing) using pre-calculated paths
	var filter bson.D
	if len(cm.shardKeyPaths) > 0 {
		filter = make(bson.D, 0, len(cm.shardKeyPaths)+1)
		for _, skp := range cm.shardKeyPaths {
			if val, err := rawDoc.LookupErr(skp.Path...); err == nil {
				filter = append(filter, bson.E{Key: skp.Key, Value: val})
			}
		}
	} else {
		filter = make(bson.D, 0, 1)
	}

	// 2. Append _id at the end (Targeting/Uniqueness)
	filter = append(filter, bson.E{Key: "_id", Value: docID})

	return filter, nil
}

func (cm *CopyManager) runDataPipeline(ctx context.Context, sourceColl, targetColl *mongo.Collection) (int64, error) {
	ns := cm.CollInfo.Namespace
	numReadWorkers := config.Cfg.Cloner.NumReadWorkers

	// --- AUTO-DETECT LINEAR SCAN MODE ---
	if cm.CollInfo.UseLinearScan || numReadWorkers == 1 {
		if cm.CollInfo.UseLinearScan {
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
		go cm.insertWorker(errCtx, cancel, &insertWG, targetColl, docQueue, &docsWritten, ns, cm.statusMgr, i)
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
	segmenter, err := NewSegmenter(errCtx, sourceColl, cm.CollInfo)
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
	ns := cm.CollInfo.Namespace
	docQueue := make(chan docBatch, config.Cfg.Cloner.NumInsertWorkers*2)

	var insertWG sync.WaitGroup
	errCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var docsWritten int64
	insertWG.Add(config.Cfg.Cloner.NumInsertWorkers)
	for i := 0; i < config.Cfg.Cloner.NumInsertWorkers; i++ {
		// Pass 'i' as workerID
		go cm.insertWorker(errCtx, cancel, &insertWG, targetColl, docQueue, &docsWritten, ns, cm.statusMgr, i)
	}

	// Single linear read process
	go func() {
		defer close(docQueue)

		readOpts := options.Find().SetBatchSize(int32(config.Cfg.Cloner.ReadBatchSize))

		cursor, err := sourceColl.Find(errCtx, bson.D{}, readOpts)
		if err != nil {
			logging.PrintError(fmt.Sprintf("[%s] Linear Scan Find failed: %v", ns, err), 0)
			cancel()
			return
		}
		defer cursor.Close(errCtx)

		batch := docBatch{
			docs: make([]bson.Raw, 0, config.Cfg.Cloner.InsertBatchSize),
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

			if len(batch.docs) == 0 {
				firstID = docID
			}
			lastID = docID

			batch.docs = append(batch.docs, rawDoc)
			batch.size += int64(len(rawDoc))

			if len(batch.docs) >= config.Cfg.Cloner.InsertBatchSize || batch.size >= config.Cfg.Cloner.InsertBatchBytes {
				batch.first = firstID
				batch.last = lastID
				select {
				case docQueue <- batch:
					batch = docBatch{
						docs: make([]bson.Raw, 0, config.Cfg.Cloner.InsertBatchSize),
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

		if len(batch.docs) > 0 {
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
			idFilter = append(idFilter, bson.E{Key: "$gt", Value: segment.Min})
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
				docs: make([]bson.Raw, 0, config.Cfg.Cloner.InsertBatchSize),
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

				if len(batch.docs) == 0 {
					firstID = docID
				}
				lastID = docID

				batch.docs = append(batch.docs, rawDoc)
				batch.size += int64(len(rawDoc))

				if len(batch.docs) >= config.Cfg.Cloner.InsertBatchSize || batch.size >= config.Cfg.Cloner.InsertBatchBytes {
					batch.first = firstID
					batch.last = lastID
					select {
					case docQueue <- batch:
						batch = docBatch{
							docs: make([]bson.Raw, 0, config.Cfg.Cloner.InsertBatchSize),
							size: 0,
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

			if len(batch.docs) > 0 {
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

// insertWorker runs InsertMany with Fallback to BulkWrite(Upsert)
func (cm *CopyManager) insertWorker(
	ctx context.Context,
	cancel context.CancelFunc,
	wg *sync.WaitGroup,
	coll *mongo.Collection,
	queue <-chan docBatch,
	counter *int64,
	ns string,
	statusMgr *status.Manager,
	workerID int,
) {
	defer wg.Done()

	insertOpts := options.InsertMany().SetOrdered(false).SetBypassDocumentValidation(true)
	bulkWriteOpts := options.BulkWrite().SetOrdered(false).SetBypassDocumentValidation(true)

	maxRetries := config.Cfg.Cloner.NumRetries
	retryInterval := time.Duration(config.Cfg.Cloner.RetryIntervalMS) * time.Millisecond
	writeTimeout := time.Duration(config.Cfg.Cloner.WriteTimeoutMS) * time.Millisecond

	for batch := range queue {
		if len(batch.docs) == 0 {
			continue
		}

		var docCount int64
		var err error

		docsInterface := make([]interface{}, len(batch.docs))
		for i, d := range batch.docs {
			docsInterface[i] = d
		}

		success := false
		for i := 1; i <= maxRetries; i++ {
			if ctx.Err() != nil {
				return
			}

			start := time.Now()
			writeCtx, writeCancel := context.WithTimeout(context.Background(), writeTimeout)
			res, insertErr := coll.InsertMany(writeCtx, docsInterface, insertOpts)
			writeCancel()

			if insertErr == nil {
				docCount = int64(len(res.InsertedIDs))

				logging.LogFullLoadBatchOp(start, ns, docCount, batch.size, nil, workerID)

				msg := fmt.Sprintf("[%s] Processed batch: %d inserted (InsertMany). (%s)", ns, docCount, humanize.Bytes(uint64(batch.size)))
				if config.Cfg.Logging.Level == "debug" {
					msg += fmt.Sprintf(" Range: [%v - %v]", batch.first, batch.last)
				}
				logging.PrintStep(msg, 4)

				success = true
				break
			}

			if shouldRetry(insertErr) {
				logging.PrintWarning(fmt.Sprintf("[%s] InsertMany network error (attempt %d/%d): %v. Retrying...", ns, i, maxRetries, insertErr), 0)
				time.Sleep(retryInterval)
				continue
			}

			if mongo.IsDuplicateKeyError(insertErr) || strings.Contains(insertErr.Error(), "E11000") || strings.Contains(insertErr.Error(), "write errors") {
				// We do NOT call LogFullLoadBatchOp with the error here because we don't want an ERROR entry in the JSON log.
				// We proceed to fallback mode which will log a successful Upsert batch later.
				logging.PrintFallback(fmt.Sprintf("[%s] Duplicate keys detected. Switching to Upsert mode for this batch.", ns), 0)
				break
			}

			logging.PrintError(fmt.Sprintf("[%s] InsertMany Fatal Error: %v", ns, insertErr), 4)
			cancel()
			return
		}

		if success {
			atomic.AddInt64(counter, docCount)
			if statusMgr != nil {
				statusMgr.AddClonedBytes(batch.size)
				statusMgr.AddClonedDocs(docCount)
			}
			continue
		}

		// --- FALLBACK: UPSERT LOOP ---
		models := make([]mongo.WriteModel, 0, len(batch.docs))
		for _, rawDoc := range batch.docs {
			filter, err := cm.buildFilter(rawDoc)
			if err != nil {
				logging.PrintError(fmt.Sprintf("[%s] Failed to build filter for fallback upsert: %v", ns, err), 0)
				continue
			}

			model := mongo.NewReplaceOneModel().
				SetFilter(filter).
				SetReplacement(rawDoc).
				SetUpsert(true)
			models = append(models, model)
		}

		for i := 1; i <= maxRetries; i++ {
			if ctx.Err() != nil {
				return
			}

			start := time.Now()
			writeCtx, writeCancel := context.WithTimeout(context.Background(), writeTimeout)
			res, upsertErr := coll.BulkWrite(writeCtx, models, bulkWriteOpts)
			writeCancel()

			docCount = 0
			if res != nil {
				docCount = res.InsertedCount + res.UpsertedCount + res.MatchedCount
			}

			// We log this as a success (err = nil) because even though it was a fallback,
			// the batch was successfully processed by the system's idempotent logic.
			logging.LogFullLoadBatchOp(start, ns, docCount, batch.size, nil, workerID)

			if upsertErr == nil {
				msg := fmt.Sprintf("[%s] Processed batch: %d docs (Fallback/Upsert). (%s)", ns, docCount, humanize.Bytes(uint64(batch.size)))
				logging.PrintStep(msg, 4)
				success = true
				break
			}

			if shouldRetry(upsertErr) {
				time.Sleep(retryInterval)
				continue
			}

			err = upsertErr
			break
		}

		if !success {
			logging.PrintError(fmt.Sprintf("[%s] BulkWrite Fallback failed: %v", ns, err), 4)
			cancel()
			return
		}

		atomic.AddInt64(counter, docCount)
		if statusMgr != nil {
			statusMgr.AddClonedBytes(batch.size)
			statusMgr.AddClonedDocs(docCount)
		}
	}
}
