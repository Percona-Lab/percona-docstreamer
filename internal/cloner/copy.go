package cloner

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Percona-Lab/docMongoStream/internal/checkpoint"
	"github.com/Percona-Lab/docMongoStream/internal/config"
	"github.com/Percona-Lab/docMongoStream/internal/discover"
	"github.com/Percona-Lab/docMongoStream/internal/indexer"
	"github.com/Percona-Lab/docMongoStream/internal/logging"
	"github.com/Percona-Lab/docMongoStream/internal/status"
	"github.com/Percona-Lab/docMongoStream/internal/topo"
	"github.com/dustin/go-humanize"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Batch of documents read from the source
type docBatch struct {
	models []mongo.WriteModel
	size   int64
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

// NewSegmenter creates a segmenter for a collection
func NewSegmenter(ctx context.Context, mcoll *mongo.Collection, collInfo discover.CollectionInfo) (*Segmenter, error) {
	min, max, err := getIDKeyRange(ctx, mcoll)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("could not get _id range: %w", err)
	}

	return &Segmenter{
		mcoll: mcoll,
		currentRange: keyRange{
			Min: min,
			Max: max,
		},
		segmentSize: config.Cfg.Cloner.SegmentSizeDocs,
		initialMax:  max,
	}, nil
}

// getIDKeyRange finds the min and max _id in a collection
func getIDKeyRange(ctx context.Context, mcoll *mongo.Collection) (min, max bson.RawValue, err error) {
	minRes := mcoll.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: 1}}).SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if minRes.Err() != nil {
		return bson.RawValue{}, bson.RawValue{}, minRes.Err()
	}
	minRaw, err := minRes.Raw()
	if err != nil {
		return bson.RawValue{}, bson.RawValue{}, fmt.Errorf("could not decode min _id: %w", err)
	}

	maxRes := mcoll.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}).SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if maxRes.Err() != nil {
		return bson.RawValue{}, bson.RawValue{}, maxRes.Err()
	}
	maxRaw, err := maxRes.Raw()
	if err != nil {
		return bson.RawValue{}, bson.RawValue{}, fmt.Errorf("could not decode max _id: %w", err)
	}

	return minRaw.Lookup("_id"), maxRaw.Lookup("_id"), nil
}

// findSegmentMaxKey finds the _id of the document at the segmentSize offset
func (s *Segmenter) findSegmentMaxKey(ctx context.Context, minKey, maxKey bson.RawValue) (bson.RawValue, error) {
	// Open-ended query: No $lte maxKey constraint
	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: minKey}}}}

	opts := options.FindOne().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetSkip(s.segmentSize).
		SetProjection(bson.D{{Key: "_id", Value: 1}})

	res := s.mcoll.FindOne(ctx, filter, opts)
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

// CopyManager manages the copy pipeline for a *single collection*.
type CopyManager struct {
	sourceClient    *mongo.Client
	targetClient    *mongo.Client
	collInfo        discover.CollectionInfo
	statusMgr       *status.Manager
	checkpointMgr   *checkpoint.Manager
	checkpointDocID string // Here to ensure consistent ID usage
	initialMaxKey   bson.RawValue
	readOpts        *options.FindOptions
	bulkWriteOpts   *options.BulkWriteOptions
}

// NewCopyManager creates a new manager for a single collection copy.
func NewCopyManager(source, target *mongo.Client, collInfo discover.CollectionInfo, statusMgr *status.Manager, checkpointMgr *checkpoint.Manager, checkpointDocID string) *CopyManager {
	return &CopyManager{
		sourceClient:    source,
		targetClient:    target,
		collInfo:        collInfo,
		statusMgr:       statusMgr,
		checkpointMgr:   checkpointMgr,
		checkpointDocID: checkpointDocID,
		readOpts:        options.Find().SetBatchSize(int32(config.Cfg.Cloner.ReadBatchSize)),
		bulkWriteOpts:   options.BulkWrite().SetOrdered(false).SetBypassDocumentValidation(true),
	}
}

// This function runs the full 3-phase copy operation for one collection
func (cm *CopyManager) Do(ctx context.Context) (int64, primitive.Timestamp, error) {
	ns := cm.collInfo.Namespace
	db := cm.collInfo.DB
	coll := cm.collInfo.Coll
	sourceCount := cm.collInfo.Count
	emptyTS := primitive.Timestamp{}

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

	// --- CONDITIONAL T0 SAVE ---
	// We assume main.go captured it, but this acts as a failsafe or for resuming
	initialTS := emptyTS
	existingTS, found := cm.checkpointMgr.GetResumeTimestamp(ctx, cm.checkpointDocID)

	if !found {
		// If not found, capture and save (Backup logic)
		newTS, err := topo.ClusterTime(ctx, cm.sourceClient)
		if err != nil {
			return 0, emptyTS, fmt.Errorf("failed to get initial cluster time (T0): %w", err)
		}
		initialTS = newTS
		if cm.checkpointMgr != nil {
			cm.checkpointMgr.SaveResumeTimestamp(ctx, cm.checkpointDocID, initialTS)
		}
		logging.PrintStep(fmt.Sprintf("[%s] Captured and saved initial cluster time (T0): %v", ns, initialTS), 3)
	} else {
		initialTS = existingTS
	}

	// --- PHASE 1: Create Collection and Pre-load Indexes ---
	indexModels := convertIndexes(cm.collInfo.Indexes)
	targetColl, err := indexer.CreateCollectionAndPreloadIndexes(ctx, targetDB, cm.collInfo, indexModels)
	if err != nil {
		return 0, emptyTS, fmt.Errorf("failed to create collection and indexes: %w", err)
	}

	// --- PHASE 2: Run Data Load Pipeline ---
	logging.PrintStep(fmt.Sprintf("[%s] Starting parallel data load...", ns), 3)
	docsWritten, err := cm.runDataPipeline(ctx, sourceColl, targetColl)
	if err != nil {
		return 0, emptyTS, fmt.Errorf("data pipeline failed: %w", err)
	}

	// --- Get Finish Timestamp ---
	finishTS, err := topo.ClusterTime(ctx, cm.sourceClient)
	if err != nil {
		return 0, emptyTS, fmt.Errorf("failed to get finish cluster time: %w", err)
	}
	logging.PrintSuccess(fmt.Sprintf("[%s] Data pipeline complete. Copied %d documents. Finish time: %v", ns, docsWritten, finishTS), 3)

	// --- Per-Collection Checkpoint ---
	if cm.checkpointMgr != nil {
		cm.checkpointMgr.SaveCollectionCheckpoint(ctx, ns, finishTS)
	}

	// --- PHASE 3: Finalize Indexes ---
	if err := indexer.FinalizeIndexes(ctx, targetColl, indexModels, ns); err != nil {
		logging.PrintError(fmt.Sprintf("[%s] Index finalization failed: %v", ns, err), 3)
	}

	// Return T0 so the coordinator knows the earliest start time
	return docsWritten, initialTS, nil
}

// runDataPipeline starts the segmenter, read workers, and insert workers
func (cm *CopyManager) runDataPipeline(ctx context.Context, sourceColl, targetColl *mongo.Collection) (int64, error) {
	ns := cm.collInfo.Namespace

	segmentQueue := make(chan keyRange, config.Cfg.Cloner.NumReadWorkers)
	docQueue := make(chan docBatch, config.Cfg.Cloner.NumInsertWorkers*2)

	var readWG, insertWG sync.WaitGroup
	errCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// --- 1. Start Insert Workers ---
	var docsWritten int64
	insertWG.Add(config.Cfg.Cloner.NumInsertWorkers)
	for i := 0; i < config.Cfg.Cloner.NumInsertWorkers; i++ {
		go cm.insertWorker(errCtx, cancel, &insertWG, targetColl, docQueue, &docsWritten, ns, cm.statusMgr)
	}

	// --- 2. Start Read Workers ---
	readWG.Add(config.Cfg.Cloner.NumReadWorkers)
	for i := 0; i < config.Cfg.Cloner.NumReadWorkers; i++ {
		go cm.readWorker(errCtx, &readWG, sourceColl, segmentQueue, docQueue, ns, i)
	}

	// --- 3. Start pipeline manager goroutine ---
	go func() {
		readWG.Wait()
		close(docQueue)
	}()

	// --- 4. Start the Segmenter ---
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

	// --- 5. Wait for Insert Workers (end of pipeline) ---
	insertWG.Wait()

	if errCtx.Err() != nil && errCtx.Err() != context.Canceled {
		return 0, fmt.Errorf("worker failed: %w", errCtx.Err())
	}

	return docsWritten, nil
}

// readWorker reads from segmentQueue, finds docs in that segment, and pushes to docQueue
func (cm *CopyManager) readWorker(
	ctx context.Context,
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
		if ctx.Err() != nil {
			return
		}

		// Open-ended query
		idFilter := bson.D{{Key: "$gte", Value: segment.Min}}
		if segment.Max.Value != nil && !segment.Max.Equal(cm.initialMaxKey) {
			idFilter = append(idFilter, bson.E{Key: "$lte", Value: segment.Max})
		}

		filter := bson.D{{Key: "_id", Value: idFilter}}

		cursor, err := coll.Find(ctx, filter, cm.readOpts.SetSort(bson.D{{Key: "_id", Value: 1}}))
		if err != nil {
			logging.PrintError(fmt.Sprintf("[%s] Read Worker %d Find failed: %v", ns, workerID, err), 4)
			return
		}

		batch := docBatch{
			models: make([]mongo.WriteModel, 0, config.Cfg.Cloner.InsertBatchSize),
		}

		for cursor.Next(ctx) {
			rawDoc := make(bson.Raw, len(cursor.Current))
			copy(rawDoc, cursor.Current)

			docID, err := rawDoc.LookupErr("_id")
			if err != nil {
				logging.PrintWarning(fmt.Sprintf("[%s] Read Worker %d found doc with no _id, skipping", ns, workerID), 4)
				continue
			}

			model := mongo.NewReplaceOneModel().
				SetFilter(bson.D{{Key: "_id", Value: docID}}).
				SetReplacement(rawDoc).
				SetUpsert(true)

			batch.models = append(batch.models, model)
			batch.size += int64(len(rawDoc))

			if len(batch.models) >= config.Cfg.Cloner.InsertBatchSize || batch.size >= config.Cfg.Cloner.InsertBatchBytes {
				select {
				case docQueue <- batch:
					batch = docBatch{
						models: make([]mongo.WriteModel, 0, config.Cfg.Cloner.InsertBatchSize),
						size:   0,
					}
				case <-ctx.Done():
					cursor.Close(ctx)
					return
				}
			}
		}

		if len(batch.models) > 0 {
			select {
			case docQueue <- batch:
			case <-ctx.Done():
				cursor.Close(ctx)
				return
			}
		}

		if err := cursor.Err(); err != nil {
			logging.PrintWarning(fmt.Sprintf("[%s] Read Worker %d cursor error: %v", ns, workerID, err), 4)
		}
		cursor.Close(ctx)
	}
}

// insertWorker runs BulkWrite with ReplaceOneModels
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

	for batch := range queue {
		if len(batch.models) == 0 {
			continue
		}
		if ctx.Err() != nil {
			return
		}

		start := time.Now()
		res, err := coll.BulkWrite(ctx, batch.models, cm.bulkWriteOpts)

		var docCount int64
		if res != nil {
			docCount = res.InsertedCount + res.UpsertedCount + res.ModifiedCount
		}
		logging.LogFullLoadBatchOp(start, ns, docCount, batch.size, err)

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logging.PrintError(fmt.Sprintf("[%s] BulkWrite failed: %v", ns, err), 4)
			cancel()
			return
		}

		atomic.AddInt64(counter, docCount)

		if statusMgr != nil {
			statusMgr.AddClonedBytes(batch.size)
		}

		elapsed := time.Since(start)

		logging.PrintStep(fmt.Sprintf("[%s] Processed batch: %d inserted, %d replaced. (%s) in %s",
			ns, res.InsertedCount+res.UpsertedCount, res.ModifiedCount, humanize.Bytes(uint64(batch.size)), elapsed), 4)
	}
}
