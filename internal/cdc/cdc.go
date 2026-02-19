package cdc

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/checkpoint"
	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/discover"
	"github.com/Percona-Lab/percona-docstreamer/internal/flow"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"github.com/Percona-Lab/percona-docstreamer/internal/status"
	"github.com/Percona-Lab/percona-docstreamer/internal/validator"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var eventPool = sync.Pool{
	New: func() interface{} {
		return &ChangeEvent{}
	},
}

type Watermark struct {
	sync.Mutex
	workerLastTS   []bson.Timestamp
	workerPending  []int64
	lastReceivedTS bson.Timestamp
}

type CDCManager struct {
	sourceClient       *mongo.Client
	targetClient       *mongo.Client
	eventQueue         chan *ChangeEvent
	flushQueues        []chan map[string]*Batch
	bulkWriters        []*BulkWriter
	startAt            bson.Timestamp
	watermark          *Watermark
	checkpoint         *checkpoint.Manager
	statusManager      *status.Manager
	tracker            *validator.InFlightTracker
	store              *validator.Store
	validatorMgr       *validator.Manager
	shutdownWG         sync.WaitGroup
	workerWG           sync.WaitGroup
	totalEventsApplied atomic.Int64
	totalInserted      atomic.Int64
	totalUpdated       atomic.Int64
	totalDeleted       atomic.Int64
	checkpointDocID    string
	excludeDBs         map[string]bool
	excludeColls       map[string]bool
	fatalErrorChan     chan error
	flowMgr            *flow.Manager
	shardKeyCache      map[string][]string
	cacheLock          sync.RWMutex
	running            atomic.Bool
	stopChan           chan struct{}
}

func isFatalError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "authentication failed") ||
		strings.Contains(msg, "sasl conversation error") ||
		strings.Contains(msg, "mechanism mismatch") ||
		strings.Contains(msg, "unauthorized")
}

func formatID(id interface{}) string {
	switch v := id.(type) {
	case bson.ObjectID:
		return v.Hex()
	case bson.Binary:
		if v.Subtype == 4 {
			if u, err := uuid.FromBytes(v.Data); err == nil {
				return u.String()
			}
		}
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func describeModel(wm mongo.WriteModel) string {
	switch m := wm.(type) {
	case *mongo.InsertOneModel:
		return fmt.Sprintf("InsertOne(Doc: %v)", m.Document)
	case *mongo.DeleteOneModel:
		return fmt.Sprintf("DeleteOne(Filter: %v)", m.Filter)
	case *mongo.DeleteManyModel:
		return fmt.Sprintf("DeleteMany(Filter: %v)", m.Filter)
	case *mongo.UpdateOneModel:
		return fmt.Sprintf("UpdateOne(Filter: %v, Update: %v)", m.Filter, m.Update)
	case *mongo.UpdateManyModel:
		return fmt.Sprintf("UpdateMany(Filter: %v, Update: %v)", m.Filter, m.Update)
	case *mongo.ReplaceOneModel:
		return fmt.Sprintf("ReplaceOne(Filter: %v, Replacement: %v)", m.Filter, m.Replacement)
	default:
		return fmt.Sprintf("UnknownModel(%T)", m)
	}
}

func (m *CDCManager) getShardKeys(ctx context.Context, ns string) ([]string, error) {
	m.cacheLock.RLock()
	if keys, ok := m.shardKeyCache[ns]; ok {
		m.cacheLock.RUnlock()
		return keys, nil
	}
	m.cacheLock.RUnlock()

	var keys []string
	for _, rule := range config.Cfg.Sharding {
		if rule.Namespace == ns {
			rawKeys := rule.GetKeys()
			for _, rk := range rawKeys {
				parts := strings.Split(rk, ":")
				keys = append(keys, parts[0])
			}
			break
		}
	}

	if len(keys) == 0 {
		var err error
		keys, err = discover.GetShardKey(ctx, m.targetClient, ns)
		if err != nil {
			return nil, fmt.Errorf("failed to discover shard keys for %s: %w", ns, err)
		}
	}

	m.cacheLock.Lock()
	m.shardKeyCache[ns] = keys
	m.cacheLock.Unlock()

	return keys, nil
}

func NewManager(source, target *mongo.Client, checkpointDocID string, startAt bson.Timestamp, checkpoint *checkpoint.Manager, statusMgr *status.Manager, tracker *validator.InFlightTracker, store *validator.Store, valMgr *validator.Manager, flowMgr *flow.Manager) *CDCManager {
	resumeTS, found := checkpoint.GetResumeTimestamp(context.Background(), checkpointDocID)

	if !found {
		logging.PrintWarning(fmt.Sprintf("[CDC %s] Checkpoint not found. Resuming from provided start time: %v", checkpointDocID, startAt), 0)
		resumeTS = startAt
	} else {
		logging.PrintInfo(fmt.Sprintf("[CDC %s] Loaded checkpoint. Resuming from %v", checkpointDocID, resumeTS), 0)
	}

	workerCount := config.Cfg.CDC.MaxWriteWorkers
	if workerCount < 1 {
		workerCount = 1
	}

	queues := make([]chan map[string]*Batch, workerCount)
	writers := make([]*BulkWriter, workerCount)

	wm := &Watermark{
		workerLastTS:   make([]bson.Timestamp, workerCount),
		workerPending:  make([]int64, workerCount),
		lastReceivedTS: resumeTS,
	}
	for i := 0; i < workerCount; i++ {
		wm.workerLastTS[i] = resumeTS
	}

	excludeDBs := make(map[string]bool)
	for _, db := range config.Cfg.Migration.ExcludeDBs {
		excludeDBs[db] = true
	}

	excludeColls := make(map[string]bool)
	for _, ns := range config.Cfg.Migration.ExcludeCollections {
		excludeColls[ns] = true
	}

	mgr := &CDCManager{
		sourceClient:    source,
		targetClient:    target,
		eventQueue:      make(chan *ChangeEvent, config.Cfg.CDC.BatchSize*2),
		flushQueues:     queues,
		startAt:         resumeTS,
		watermark:       wm,
		checkpoint:      checkpoint,
		statusManager:   statusMgr,
		tracker:         tracker,
		store:           store,
		validatorMgr:    valMgr,
		checkpointDocID: checkpointDocID,
		excludeDBs:      excludeDBs,
		excludeColls:    excludeColls,
		fatalErrorChan:  make(chan error, workerCount+1),
		flowMgr:         flowMgr,
		shardKeyCache:   make(map[string][]string),
		stopChan:        make(chan struct{}),
	}

	for i := 0; i < workerCount; i++ {
		queues[i] = make(chan map[string]*Batch, config.Cfg.Migration.MaxConcurrentWorkers)
		writers[i] = NewBulkWriter(target, config.Cfg.CDC.BatchSize, mgr.getShardKeys)
	}
	mgr.bulkWriters = writers

	mgr.totalEventsApplied.Store(statusMgr.GetEventsApplied())
	mgr.totalInserted.Store(statusMgr.GetInsertedDocs())
	mgr.totalUpdated.Store(statusMgr.GetUpdatedDocs())
	mgr.totalDeleted.Store(statusMgr.GetDeletedDocs())

	return mgr
}

func (m *CDCManager) getSafeCheckpointTS() bson.Timestamp {
	m.watermark.Lock()
	defer m.watermark.Unlock()

	minTS := bson.Timestamp{T: 0xFFFFFFFF, I: 0xFFFFFFFF}

	for i := 0; i < len(m.watermark.workerLastTS); i++ {
		effectiveTS := m.watermark.workerLastTS[i]
		if m.watermark.workerPending[i] == 0 {
			effectiveTS = m.watermark.lastReceivedTS
		}
		if effectiveTS.T < minTS.T || (effectiveTS.T == minTS.T && effectiveTS.I < minTS.I) {
			minTS = effectiveTS
		}
	}
	return minTS
}

func (m *CDCManager) handleBulkWrite(ctx context.Context, batchMap map[string]*Batch) (int64, int64, int64, int64, []string, bson.Timestamp, error) {
	var totalOps, totalIns, totalUpd, totalDel int64
	var namespaces []string
	var batchMaxTS bson.Timestamp

	for ns, batch := range batchMap {
		if len(batch.Models) == 0 {
			continue
		}

		if batch.LastTS.T > batchMaxTS.T || (batch.LastTS.T == batchMaxTS.T && batch.LastTS.I > batchMaxTS.I) {
			batchMaxTS = batch.LastTS
		}

		m.hydrateAndFixOperations(ctx, ns, batch)

		ops, ins, upd, del, err := m.executeBatch(ctx, ns, batch)
		if err == nil {
			totalOps += ops
			totalIns += ins
			totalUpd += upd
			totalDel += del
			namespaces = append(namespaces, ns)
			if len(batch.Keys) > 0 {
				m.validatorMgr.ValidateAsync(ns, batch.Keys)
			}
			continue
		}

		if isFatalError(err) {
			return 0, 0, 0, 0, namespaces, batchMaxTS, err
		}

		m.statusManager.IncrementSerialBatches()
		if config.Cfg.Logging.Level == "debug" {
			logging.PrintWarning(fmt.Sprintf("[CDC %s] Batch failed (%v). Switching to Adaptive Serial Mode.", ns, err), 0)
		}

		sOps, sIns, sUpd, sDel, sErr := m.executeSerially(ctx, ns, batch)
		if sErr != nil {
			return 0, 0, 0, 0, namespaces, batchMaxTS, sErr
		}

		totalOps += sOps
		totalIns += sIns
		totalUpd += sUpd
		totalDel += sDel
		namespaces = append(namespaces, ns)
	}

	return totalOps, totalIns, totalUpd, totalDel, namespaces, batchMaxTS, nil
}

// applyUpdate applies a $set/$unset update map to a source document (bson.M)
// and returns the modified document. Simple in-memory application.
func applyUpdate(doc bson.M, update interface{}) bson.M {
	newDoc := make(bson.M)
	for k, v := range doc {
		newDoc[k] = v
	}

	var setMap bson.M

	if uD, ok := update.(bson.D); ok {
		for _, e := range uD {
			if e.Key == "$set" {
				if valD, ok := e.Value.(bson.D); ok {
					setMap = make(bson.M)
					for _, ve := range valD {
						setMap[ve.Key] = ve.Value
					}
				} else if valM, ok := e.Value.(bson.M); ok {
					setMap = valM
				}
			}
		}
	} else if uM, ok := update.(bson.M); ok {
		if val, ok := uM["$set"]; ok {
			if valM, ok := val.(bson.M); ok {
				setMap = valM
			} else if valD, ok := val.(bson.D); ok {
				setMap = make(bson.M)
				for _, ve := range valD {
					setMap[ve.Key] = ve.Value
				}
			}
		}
	}

	if setMap != nil {
		for k, v := range setMap {
			if !strings.Contains(k, ".") {
				newDoc[k] = v
			}
		}
	}
	return newDoc
}

func (m *CDCManager) hydrateAndFixOperations(ctx context.Context, ns string, batch *Batch) {
	shardKeys, err := m.getShardKeys(ctx, ns)

	if err != nil {
		if config.Cfg.Logging.Level == "debug" {
			logging.PrintWarning(fmt.Sprintf("[CDC %s] Discovery failed: %v. Downgrading to Broadcast.", ns, err), 0)
		}
		m.downgradeAllToBroadcast(batch)
		return
	}

	if len(shardKeys) == 0 {
		return
	}

	dbName, collName := splitNamespace(ns)
	targetColl := m.targetClient.Database(dbName).Collection(collName)

	var indicesToFix []int
	var idsToLookup []interface{}

	for i, model := range batch.Models {
		var filter bson.D
		isCandidate := false

		switch model.(type) {
		case *mongo.DeleteOneModel:
			filter = batch.Keys[i]
			isCandidate = true
		case *mongo.UpdateOneModel:
			filter = batch.Keys[i]
			isCandidate = true
		case *mongo.ReplaceOneModel:
			filter = batch.Keys[i]
			isCandidate = true
		}

		if isCandidate {
			for _, e := range filter {
				if e.Key == "_id" {
					indicesToFix = append(indicesToFix, i)
					idsToLookup = append(idsToLookup, e.Value)
					break
				}
			}
		}
	}

	if len(indicesToFix) == 0 {
		return
	}

	lookupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cursor, err := targetColl.Find(lookupCtx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: idsToLookup}}}})

	foundDocs := make(map[string]bson.M)
	if err == nil {
		defer cursor.Close(ctx)
		for cursor.Next(ctx) {
			var doc bson.M
			if err := cursor.Decode(&doc); err == nil {
				idStr := formatID(doc["_id"])
				foundDocs[idStr] = doc
			}
		}
	}

	newModels := make([]mongo.WriteModel, 0, len(batch.Models))
	newKeys := make([]bson.D, 0, len(batch.Keys))

	fixMap := make(map[int]bool)
	for _, idx := range indicesToFix {
		fixMap[idx] = true
	}

	for i, model := range batch.Models {
		if !fixMap[i] {
			newModels = append(newModels, model)
			newKeys = append(newKeys, batch.Keys[i])
			continue
		}

		var idVal interface{}
		for _, e := range batch.Keys[i] {
			if e.Key == "_id" {
				idVal = e.Value
				break
			}
		}
		idStr := formatID(idVal)
		doc, exists := foundDocs[idStr]

		if !exists {
			var fallbackModel mongo.WriteModel
			switch mod := model.(type) {
			case *mongo.DeleteOneModel:
				fallbackModel = mongo.NewDeleteManyModel().SetFilter(mod.Filter)
			case *mongo.UpdateOneModel:
				safeUpdate := stripShardKeysFromUpdate(mod.Update, shardKeys)

				if u, ok := safeUpdate.(bson.D); ok && len(u) == 0 {
					if config.Cfg.Logging.Level == "debug" {
						logging.PrintWarning(fmt.Sprintf("[CDC] Skipping empty update for non-existent document %s", idStr), 0)
					}
					continue
				}

				fallbackModel = mongo.NewUpdateManyModel().SetFilter(mod.Filter).SetUpdate(safeUpdate)
			case *mongo.ReplaceOneModel:
				if mod.Upsert != nil && *mod.Upsert {
					if replacement, ok := mod.Replacement.(bson.M); ok {
						fallbackModel = mongo.NewInsertOneModel().SetDocument(replacement)
					} else {
						fallbackModel = mod
					}
				} else {
					continue
				}
			}
			newModels = append(newModels, fallbackModel)
			newKeys = append(newKeys, batch.Keys[i])
			continue
		}

		currentKeys := bson.D{}
		hasKeys := true
		for _, key := range shardKeys {
			path := strings.Split(key, ".")
			if val, ok := GetNestedValue(doc, path); ok {
				currentKeys = append(currentKeys, bson.E{Key: key, Value: val})
			} else {
				hasKeys = false
				break
			}
		}

		if !hasKeys {
			newModels = append(newModels, model)
			newKeys = append(newKeys, batch.Keys[i])
			continue
		}

		hydratedFilter := bson.D{{Key: "_id", Value: idVal}}
		hydratedFilter = append(hydratedFilter, currentKeys...)

		wasSplit := false

		if replaceMod, ok := model.(*mongo.ReplaceOneModel); ok {
			if replacement, ok := replaceMod.Replacement.(bson.M); ok {
				keysChanged := false
				for _, sk := range shardKeys {
					path := strings.Split(sk, ".")
					currentVal, _ := GetNestedValue(doc, path)
					newVal, _ := GetNestedValue(replacement, path)
					if fmt.Sprintf("%v", currentVal) != fmt.Sprintf("%v", newVal) {
						keysChanged = true
						break
					}
				}

				if keysChanged {
					del := mongo.NewDeleteOneModel().SetFilter(hydratedFilter)
					ins := mongo.NewInsertOneModel().SetDocument(replacement)
					newModels = append(newModels, del, ins)
					newKeys = append(newKeys, hydratedFilter, hydratedFilter)
					wasSplit = true
				}
			}
		} else if updateMod, ok := model.(*mongo.UpdateOneModel); ok {
			newDoc := applyUpdate(doc, updateMod.Update)

			keysChanged := false
			for _, sk := range shardKeys {
				path := strings.Split(sk, ".")
				currentVal, _ := GetNestedValue(doc, path)
				newVal, _ := GetNestedValue(newDoc, path)
				if fmt.Sprintf("%v", currentVal) != fmt.Sprintf("%v", newVal) {
					keysChanged = true
					break
				}
			}

			if keysChanged {
				del := mongo.NewDeleteOneModel().SetFilter(hydratedFilter)
				ins := mongo.NewInsertOneModel().SetDocument(newDoc)
				newModels = append(newModels, del, ins)
				newKeys = append(newKeys, hydratedFilter, hydratedFilter)
				wasSplit = true
			}
		}

		if !wasSplit {
			switch mod := model.(type) {
			case *mongo.DeleteOneModel:
				mod.SetFilter(hydratedFilter)
				newModels = append(newModels, mod)
			case *mongo.ReplaceOneModel:
				mod.SetFilter(hydratedFilter)
				newModels = append(newModels, mod)
			case *mongo.UpdateOneModel:
				mod.SetFilter(hydratedFilter)
				newModels = append(newModels, mod)
			default:
				newModels = append(newModels, model)
			}
			newKeys = append(newKeys, hydratedFilter)
		}
	}

	batch.Models = newModels
	batch.Keys = newKeys
}

func stripShardKeysFromUpdate(update interface{}, shardKeys []string) interface{} {
	uDoc, ok := update.(bson.D)
	if !ok {
		return update
	}

	newUpdate := bson.D{}
	for _, elem := range uDoc {
		if elem.Key == "$set" || elem.Key == "$unset" {
			inner, ok := elem.Value.(bson.D)
			if !ok {
				if m, ok := elem.Value.(bson.M); ok {
					inner = bson.D{}
					for k, v := range m {
						inner = append(inner, bson.E{Key: k, Value: v})
					}
				}
			}

			newInner := bson.D{}
			for _, field := range inner {
				isShardKey := false
				for _, sk := range shardKeys {
					if field.Key == sk || strings.HasPrefix(field.Key, sk+".") {
						isShardKey = true
						break
					}
				}
				if !isShardKey {
					newInner = append(newInner, field)
				}
			}

			if len(newInner) > 0 {
				newUpdate = append(newUpdate, bson.E{Key: elem.Key, Value: newInner})
			}
		} else {
			newUpdate = append(newUpdate, elem)
		}
	}
	return newUpdate
}

func (m *CDCManager) downgradeAllToBroadcast(batch *Batch) {
	for i, model := range batch.Models {
		switch mod := model.(type) {
		case *mongo.DeleteOneModel:
			batch.Models[i] = mongo.NewDeleteManyModel().SetFilter(mod.Filter)
		case *mongo.UpdateOneModel:
			batch.Models[i] = mongo.NewUpdateManyModel().SetFilter(mod.Filter).SetUpdate(mod.Update)
		}
	}
}

func (m *CDCManager) executeBatch(ctx context.Context, ns string, batch *Batch) (int64, int64, int64, int64, error) {
	if len(batch.Models) == 0 {
		return 0, 0, 0, 0, nil
	}

	db, coll := splitNamespace(ns)
	targetColl := m.targetClient.Database(db).Collection(coll)
	opts := options.BulkWrite().SetOrdered(false)

	writeTimeout := time.Duration(config.Cfg.CDC.WriteTimeoutMS) * time.Millisecond
	if writeTimeout == 0 {
		writeTimeout = 60 * time.Second
	}

	attemptCtx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	res, err := targetColl.BulkWrite(attemptCtx, batch.Models, opts)
	if err != nil {
		// Existing error handling
		if mongo.IsDuplicateKeyError(err) || strings.Contains(err.Error(), "E11000") {
			return 0, 0, 0, 0, nil
		}
		return 0, 0, 0, 0, err
	}

	var ins, upd, del, ops int64
	if res != nil {
		ins = res.InsertedCount
		upd = res.MatchedCount + res.UpsertedCount
		del = res.DeletedCount
		ops = ins + upd + del
	}
	return ops, ins, upd, del, nil
}

func (m *CDCManager) executeSerially(ctx context.Context, ns string, batch *Batch) (int64, int64, int64, int64, error) {
	var totalOps, totalIns, totalUpd, totalDel int64

	for i, model := range batch.Models {
		singleBatch := &Batch{
			Models: []mongo.WriteModel{model},
			Keys:   []bson.D{batch.Keys[i]},
		}

		for {
			if ctx.Err() != nil {
				return 0, 0, 0, 0, ctx.Err()
			}

			ops, ins, upd, del, err := m.executeBatch(ctx, ns, singleBatch)
			if err == nil {
				totalOps += ops
				totalIns += ins
				totalUpd += upd
				totalDel += del
				if i < len(batch.Keys) {
					m.validatorMgr.ValidateAsync(ns, []bson.D{batch.Keys[i]})
				}
				break
			}

			if isFatalError(err) {
				return 0, 0, 0, 0, err
			}

			// Prevent infinite loop on invalid update documents
			if strings.Contains(err.Error(), "update document must have at least one element") {
				if config.Cfg.Logging.Level == "debug" {
					logging.PrintError(fmt.Sprintf("[CDC %s] Skipping invalid operation: %v. ID: %s", ns, err, formatID(batch.Keys[i])), 0)
				}
				break // Stop retrying this specific op and move to the next in the batch
			}

			if config.Cfg.Logging.Level == "debug" {
				logging.PrintWarning(fmt.Sprintf("[CDC %s] Serial op failed: %v. Model: %s. Throttling...", ns, err, describeModel(model)), 0)
			}

			select {
			case <-ctx.Done():
				return 0, 0, 0, 0, ctx.Err()
			case <-time.After(2 * time.Second):
				continue
			}
		}
	}

	return totalOps, totalIns, totalUpd, totalDel, nil
}

func (m *CDCManager) startFlushWorkers(ctx context.Context) {
	workerCount := len(m.flushQueues)

	for i := 0; i < workerCount; i++ {
		m.workerWG.Add(1)
		go func(workerID int, queue <-chan map[string]*Batch) {
			defer m.workerWG.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case batchMap, ok := <-queue:
					if !ok {
						return
					}

					var eventCount int64
					for _, batch := range batchMap {
						eventCount += batch.EventCount
					}

					start := time.Now()
					flushedCount, ins, upd, del, namespaces, batchMaxTS, err := m.handleBulkWrite(ctx, batchMap)

					m.watermark.Lock()
					m.watermark.workerLastTS[workerID] = batchMaxTS
					m.watermark.workerPending[workerID] -= eventCount
					m.watermark.Unlock()

					logging.LogCDCOp(start, ins, upd, del, namespaces, err, workerID)
					if err != nil {
						select {
						case m.fatalErrorChan <- err:
						default:
						}
						return
					}

					m.totalEventsApplied.Add(flushedCount)
					m.totalInserted.Add(ins)
					m.totalUpdated.Add(upd)
					m.totalDeleted.Add(del)

				}
			}
		}(i, m.flushQueues[i])
	}
}

func (m *CDCManager) Start(ctx context.Context) error {
	if m.running.Swap(true) {
		return nil
	}

	logging.PrintInfo(fmt.Sprintf("Starting CDC... Resuming from: %v", m.startAt), 0)

	go func() {
		if err := m.validatorMgr.ReconcileStats(ctx); err != nil {
			logging.PrintWarning(fmt.Sprintf("[CDC] Stats reconciliation failed: %v", err), 0)
		}
	}()

	cdcCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case err := <-m.fatalErrorChan:
			logging.PrintError(fmt.Sprintf("FATAL CDC ERROR: %v", err), 0)
			m.statusManager.SetError(err.Error())
			cancel()
		case <-cdcCtx.Done():
		case <-m.stopChan:
			cancel()
		}
	}()

	m.startFlushWorkers(cdcCtx)
	m.shutdownWG.Add(1)
	go m.processChanges(cdcCtx)

	go func() {
		defer cancel()
		for m.running.Load() {
			if err := m.runChangeStream(cdcCtx); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				logging.PrintError(fmt.Sprintf("[CDC] Error in change stream: %v. Retrying in 5s...", err), 0)
				m.statusManager.SetError(fmt.Sprintf("CDC Stalled: %v", err))

				select {
				case <-m.stopChan:
					return
				case <-cdcCtx.Done():
					return
				case <-time.After(5 * time.Second):
				}
			}
		}
	}()

	select {
	case <-m.stopChan:
		cancel()
	case <-cdcCtx.Done():
	case <-ctx.Done():
		cancel()
	}

	m.shutdownWG.Wait()
	m.workerWG.Wait()

	safeTS := m.getSafeCheckpointTS()
	if safeTS != (bson.Timestamp{}) {
		m.checkpoint.SaveResumeTimestamp(context.Background(), m.checkpointDocID, safeTS)
		logging.PrintInfo(fmt.Sprintf("[CDC] Clean shutdown. Saved safe checkpoint: %d.%d", safeTS.T, safeTS.I), 0)
	}

	return nil
}

func (m *CDCManager) Stop() {
	if !m.running.Swap(false) {
		return
	}
	close(m.stopChan)
	logging.PrintInfo("[CDC] Stop signal sent.", 0)
}

func (m *CDCManager) waitForFlow(ctx context.Context) {
	if m.flowMgr == nil {
		return
	}

	for {
		isPaused, _, _ := m.flowMgr.GetStatus()
		if !isPaused {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (m *CDCManager) runChangeStream(ctx context.Context) error {
	pipeline := mongo.Pipeline{}

	batchSize := int32(config.Cfg.CDC.BatchSize)
	if batchSize == 0 {
		batchSize = 100 // Fallback safety
	}

	maxAwaitTime := time.Duration(config.Cfg.CDC.MaxAwaitTimeMS) * time.Millisecond
	if maxAwaitTime == 0 {
		maxAwaitTime = 1000 * time.Millisecond // Fallback safety
	}

	opts := options.ChangeStream().
		SetBatchSize(batchSize).
		SetFullDocument(options.UpdateLookup).
		SetMaxAwaitTime(maxAwaitTime)

	safeStart := m.getSafeCheckpointTS()
	if safeStart.T == 0xFFFFFFFF {
		safeStart = m.startAt
	}

	if safeStart.T > 0 {
		opts.SetStartAtOperationTime(&safeStart)
		logging.PrintInfo(fmt.Sprintf("[CDC] Starting stream from SAFE timestamp: %d.%d", safeStart.T, safeStart.I), 0)
	} else {
		opts.SetStartAtOperationTime(&m.startAt)
	}

	m.statusManager.SetState("running", "Change Data Capture")

	stream, err := m.sourceClient.Watch(ctx, pipeline, opts)
	if err != nil {
		return fmt.Errorf("watch start failed: %w", err)
	}
	defer stream.Close(ctx)

	logging.PrintInfo("[CDC] Change stream active, waiting for events...", 0)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.stopChan:
			return nil
		default:
			if m.flowMgr != nil {
				m.flowMgr.WaitIfPaused()
			}

			if stream.Next(ctx) {
				event := eventPool.Get().(*ChangeEvent)
				if err := stream.Decode(event); err != nil {
					eventPool.Put(event)
					logging.PrintError(fmt.Sprintf("[CDC] Failed to decode event: %v", err), 0)
					continue
				}

				select {
				case m.eventQueue <- event:
				case <-ctx.Done():
					return ctx.Err()
				case <-m.stopChan:
					return nil
				}
			} else {
				if err := stream.Err(); err != nil {
					return fmt.Errorf("stream error: %w", err)
				}
			}
		}
	}
}

func (m *CDCManager) getWorkerIndex(docID interface{}) int {
	numWorkers := len(m.bulkWriters)
	h := fnv.New32a()
	strID := fmt.Sprintf("%v", docID)
	h.Write([]byte(strID))
	return int(h.Sum32()) % numWorkers
}

func (m *CDCManager) shouldSkip(event *ChangeEvent) bool {
	if m.excludeDBs[event.Namespace.Database] {
		return true
	}
	ns := fmt.Sprintf("%s.%s", event.Namespace.Database, event.Namespace.Collection)
	return m.excludeColls[ns]
}

func (m *CDCManager) processChanges(ctx context.Context) {
	defer m.shutdownWG.Done()
	defer func() {
		for _, ch := range m.flushQueues {
			close(ch)
		}
	}()

	ticker := time.NewTicker(time.Duration(config.Cfg.CDC.BatchIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		m.waitForFlow(ctx)

		select {
		case <-ctx.Done():
			return

		case event := <-m.eventQueue:
			if m.shouldSkip(event) {
				continue
			}

			workerIdx := m.getWorkerIndex(event.DocumentKey["_id"])
			m.watermark.Lock()
			m.watermark.lastReceivedTS = event.ClusterTime
			m.watermark.workerPending[workerIdx]++
			m.watermark.Unlock()

			if m.isDDL(event) {
				m.handleDDL(ctx, event)
			} else {
				docID := event.DocumentKey["_id"]
				idStr := formatID(docID)
				m.tracker.MarkDirty(idStr)
				go m.store.Invalidate(context.Background(), event.Ns(), idStr)

				for {
					queued, full, err := m.bulkWriters[workerIdx].AddEvent(event)
					if err == nil {
						if !queued {
							m.watermark.Lock()
							m.watermark.workerPending[workerIdx]--
							m.watermark.Unlock()
						} else if full {
							batches := m.bulkWriters[workerIdx].ExtractBatches()
							select {
							case m.flushQueues[workerIdx] <- batches:
							case <-ctx.Done():
								return
							}
						}
						break
					}

					logging.PrintError(fmt.Sprintf("[CDC] Failed to process event for %s: %v. Retrying in 2s...", event.Ns(), err), 0)
					select {
					case <-ctx.Done():
						return
					case <-time.After(2 * time.Second):
					}
				}
			}

		case <-ticker.C:
			for i, writer := range m.bulkWriters {
				if b := writer.ExtractBatches(); len(b) > 0 {
					select {
					case m.flushQueues[i] <- b:
					case <-ctx.Done():
						return
					}
				}
			}

			applied := m.totalEventsApplied.Load()
			inserted := m.totalInserted.Load()
			updated := m.totalUpdated.Load()
			deleted := m.totalDeleted.Load()

			m.watermark.Lock()
			headTS := m.watermark.lastReceivedTS
			m.watermark.Unlock()

			safeTS := m.getSafeCheckpointTS()
			if safeTS.T == 0xFFFFFFFF {
				safeTS = m.startAt
			}

			m.statusManager.UpdateCDCStats(applied, inserted, updated, deleted, headTS)
			m.statusManager.UpdateAppliedStats(safeTS)

			go func(ts bson.Timestamp) {
				persistCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				m.statusManager.Persist(persistCtx)

				if ts.T > 0 {
					m.checkpoint.SaveResumeTimestamp(persistCtx, m.checkpointDocID, ts)
				}
			}(safeTS)
		}
	}
}

func (m *CDCManager) handleDDL(ctx context.Context, event *ChangeEvent) {
	ns := event.Ns()
	logging.PrintWarning(fmt.Sprintf("[%s] DDL Operation detected: %s. Flushing all partitions.", ns, event.OperationType), 0)
	saveCtx := context.Background()

	for i, writer := range m.bulkWriters {
		if batch := writer.ExtractBatches(); len(batch) > 0 {
			select {
			case m.flushQueues[i] <- batch:
			default:
			}
		}
	}

	nextTS := bson.Timestamp{T: event.ClusterTime.T, I: event.ClusterTime.I + 1}
	m.checkpoint.SaveResumeTimestamp(saveCtx, m.checkpointDocID, nextTS)
	m.statusManager.UpdateCDCStats(
		m.totalEventsApplied.Load(),
		m.totalInserted.Load(),
		m.totalUpdated.Load(),
		m.totalDeleted.Load(),
		event.ClusterTime,
	)
	m.statusManager.Persist(saveCtx)

	targetDB := m.targetClient.Database(event.Namespace.Database)
	targetColl := targetDB.Collection(event.Namespace.Collection)
	ddlCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch event.OperationType {
	case Drop:
		targetColl.Drop(ddlCtx)
	case DropDatabase:
		targetDB.Drop(ddlCtx)
	case Rename:
		renameCmd := bson.D{
			{Key: "renameCollection", Value: ns},
			{Key: "to", Value: fmt.Sprintf("%s.%s", event.To.Database, event.To.Collection)},
		}
		m.targetClient.Database("admin").RunCommand(ddlCtx, renameCmd)
	case Create:
		targetDB.CreateCollection(ddlCtx, event.Namespace.Collection)
	}
}

func (m *CDCManager) isDDL(event *ChangeEvent) bool {
	switch event.OperationType {
	case Drop, Rename, DropDatabase, Create, CreateIndexes, DropIndexes:
		return true
	default:
		return false
	}
}

func splitNamespace(ns string) (string, string) {
	parts := strings.SplitN(ns, ".", 2)
	if len(parts) < 2 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}
