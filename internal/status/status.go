package status

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type StatusOutput struct {
	OK    bool   `json:"ok"`
	State string `json:"state"`
	Info  string `json:"info"`

	FlowControl *FCStatus `json:"flowControl,omitempty"`

	// CDC Metrics
	TimeSinceLastEventSeconds float64 `json:"timeSinceLastEventSeconds"`
	CDCLagSeconds             float64 `json:"cdcLagSeconds"`
	TotalEventsApplied        int64   `json:"totalEventsApplied"`
	AdaptiveSerialBatches     int64   `json:"adaptiveSerialBatches"`
	InsertedDocs              int64   `json:"insertedDocs"`
	UpdatedDocs               int64   `json:"updatedDocs"`
	DeletedDocs               int64   `json:"deletedDocs"`

	// Sub-sections
	Validation           ValidationStats `json:"validation"`
	LastSourceEventTime  TSTime          `json:"lastSourceEventTime"`
	LastAppliedEventTime TSTime          `json:"lastAppliedEventTime"`
	LastBatchAppliedAt   string          `json:"lastBatchAppliedAt"`
	InitialSync          SyncStatus      `json:"initialSync"`
}

type FCStatus struct {
	Enabled             bool   `json:"enabled"`
	IsPaused            bool   `json:"isPaused"`
	PauseReason         string `json:"pauseReason,omitempty"`
	CurrentQueuedOps    int    `json:"currentQueuedOps"`
	CurrentWriterQueue  int    `json:"currentWriterQueue"`
	NativeLagged        bool   `json:"isReplicationLagged"`
	NativeSustainerRate int    `json:"assignedRateLimit"`
}

type ValidationStats struct {
	QueuedBatches  int64   `json:"queuedBatches"`
	TotalChecked   int64   `json:"totalChecked"`
	MismatchFound  int64   `json:"mismatchFound"`
	MismatchFixed  int64   `json:"mismatchFixed"`
	Pending        int64   `json:"pendingMismatches"`
	HotKeysWaiting int64   `json:"hotKeysWaiting"`
	SyncPercent    float64 `json:"syncPercent"`
	LastValidated  string  `json:"lastValidatedAt"`
}

type TSTime struct {
	TS      string `json:"ts"`
	ISODate string `json:"isoDate"`
}

type SyncStatus struct {
	Completed               bool    `json:"completed"`
	ProgressPercent         float64 `json:"progressPercent"`
	ClonedDocs              int64   `json:"clonedDocs"`
	EstimatedTotalDocs      int64   `json:"estimatedTotalDocs"`
	ClonedBytes             int64   `json:"clonedBytes"`
	EstimatedTotalBytes     int64   `json:"estimatedTotalBytes"`
	ClonedSizeHuman         string  `json:"clonedSizeHuman"`
	EstimatedCloneSizeHuman string  `json:"estimatedCloneSizeHuman"`
	StartedAt               string  `json:"startedAt"`
	EndedAt                 string  `json:"endedAt"`
	Duration                string  `json:"duration"`
	CompletionLagSeconds    float64 `json:"completionLagSeconds"`
}

type Manager struct {
	client           *mongo.Client
	collection       *mongo.Collection
	docID            string
	state            string
	info             string
	mu               sync.RWMutex
	startTime        time.Time
	initialSyncStart time.Time
	initialSyncEnd   time.Time

	// Atomic Counters (CDC)
	eventsApplied atomic.Int64
	insertedDocs  atomic.Int64
	updatedDocs   atomic.Int64
	deletedDocs   atomic.Int64
	serialBatches atomic.Int64

	// Atomic Counters (Initial Sync)
	estimatedTotalBytes atomic.Int64
	estimatedTotalDocs  atomic.Int64
	clonedBytes         atomic.Int64
	clonedDocs          atomic.Int64

	// Timestamps (Stored as Unix Seconds)
	lastSourceEventTime  atomic.Int64
	lastAppliedEventTime atomic.Int64
	lastBatchAppliedAt   atomic.Int64

	// Flags
	cloneCompleted       atomic.Bool
	initialSyncCompleted atomic.Bool

	// Flow Control State (In-Memory)
	fcIsPaused        atomic.Bool
	fcReason          atomic.Value
	fcQueuedOps       atomic.Int64
	fcWriterQueue     atomic.Int64
	fcNativeLagged    atomic.Bool
	fcNativeSustainer atomic.Int64

	// Validation Stats (In-Memory)
	valTotalChecked  atomic.Int64
	valMismatchFound atomic.Int64
	valMismatchFixed atomic.Int64
	valPending       atomic.Int64
	valHotKeys       atomic.Int64
	valLastCheck     atomic.Int64
	valQueueSize     atomic.Int64
}

func NewManager(client *mongo.Client, isSource bool) *Manager {
	dbName := config.Cfg.Migration.MetadataDB
	collName := config.Cfg.Migration.StatusCollection
	docID := config.Cfg.Migration.StatusDocID

	m := &Manager{
		client:     client,
		collection: client.Database(dbName).Collection(collName),
		docID:      docID,
		state:      "initializing",
		startTime:  time.Now().UTC(),
	}
	m.fcReason.Store("")
	return m
}

func (m *Manager) UpdateFlowControl(paused bool, reason string, queuedOps, writerQueue int, nativeLagged bool, nativeSustainer int) {
	m.fcIsPaused.Store(paused)
	m.fcReason.Store(reason)
	m.fcQueuedOps.Store(int64(queuedOps))
	m.fcWriterQueue.Store(int64(writerQueue))
	m.fcNativeLagged.Store(nativeLagged)
	m.fcNativeSustainer.Store(int64(nativeSustainer))

	go m.Persist(context.Background())
}

func (m *Manager) UpdateValidationStats(checked, found, fixed, pending, hotKeys int64) {
	if checked > 0 {
		m.valTotalChecked.Add(checked)
	}
	if found > 0 {
		m.valMismatchFound.Add(found)
	}
	if fixed > 0 {
		m.valMismatchFixed.Add(fixed)
	}
	m.valPending.Store(pending)
	m.valHotKeys.Store(hotKeys)
	m.valLastCheck.Store(time.Now().UTC().Unix())
}

func (m *Manager) SetValidationQueueSize(size int) {
	m.valQueueSize.Store(int64(size))
}

func (m *Manager) SetState(state, info string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
	m.info = info
}

func (m *Manager) SetError(err string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = "error"
	m.info = err
}

func (m *Manager) UpdateCDCStats(applied, inserted, updated, deleted int64, lastSourceTS bson.Timestamp) {
	m.eventsApplied.Store(applied)
	m.insertedDocs.Store(inserted)
	m.updatedDocs.Store(updated)
	m.deletedDocs.Store(deleted)
	if lastSourceTS.T > 0 {
		m.lastSourceEventTime.Store(int64(lastSourceTS.T))
	}
}

func (m *Manager) UpdateAppliedStats(lastAppliedTS bson.Timestamp) {
	if lastAppliedTS.T > 0 {
		m.lastAppliedEventTime.Store(int64(lastAppliedTS.T))
		m.lastBatchAppliedAt.Store(time.Now().UTC().Unix())
	}
}

func (m *Manager) IncrementSerialBatches() {
	m.serialBatches.Add(1)
}

func (m *Manager) GetEventsApplied() int64 {
	return m.eventsApplied.Load()
}
func (m *Manager) GetInsertedDocs() int64 {
	return m.insertedDocs.Load()
}
func (m *Manager) GetUpdatedDocs() int64 {
	return m.updatedDocs.Load()
}
func (m *Manager) GetDeletedDocs() int64 {
	return m.deletedDocs.Load()
}

func (m *Manager) AddEstimatedBytes(n int64) {
	m.estimatedTotalBytes.Add(n)
}
func (m *Manager) AddEstimatedDocs(n int64) {
	m.estimatedTotalDocs.Add(n)
}
func (m *Manager) AddClonedBytes(n int64) {
	m.clonedBytes.Add(n)
}
func (m *Manager) AddClonedDocs(n int64) {
	m.clonedDocs.Add(n)
}
func (m *Manager) ResetProgress() {
	m.estimatedTotalBytes.Store(0)
	m.estimatedTotalDocs.Store(0)
	m.clonedBytes.Store(0)
	m.clonedDocs.Store(0)
}
func (m *Manager) SetInitialSyncStart(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initialSyncStart = t
}
func (m *Manager) SetInitialSyncEnd(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initialSyncEnd = t
}

func (m *Manager) IsInitialSyncCompleted() bool {
	return m.initialSyncCompleted.Load()
}
func (m *Manager) SetInitialSyncCompleted(lagSeconds float64) {
	m.initialSyncCompleted.Store(true)
}

func (m *Manager) IsCloneCompleted() bool {
	return m.cloneCompleted.Load()
}

func (m *Manager) SetCloneCompleted() {
	m.cloneCompleted.Store(true)
}

func (m *Manager) LoadAndMerge(ctx context.Context) error {
	var doc bson.M
	err := m.collection.FindOne(ctx, bson.D{{Key: "_id", Value: m.docID}}).Decode(&doc)
	if err != nil {
		return err
	}

	if val, ok := doc["lastSourceEventTime"]; ok {
		m.lastSourceEventTime.Store(parseInt64(val))
	}
	if val, ok := doc["lastAppliedEventTime"]; ok {
		m.lastAppliedEventTime.Store(parseInt64(val))
	}

	if val, ok := doc["lastBatchAppliedAt"]; ok {
		if t, isTime := val.(time.Time); isTime {
			m.lastBatchAppliedAt.Store(t.Unix())
		} else {
			m.lastBatchAppliedAt.Store(parseInt64(val))
		}
	}

	if val, ok := doc["totalEventsApplied"]; ok {
		m.eventsApplied.Store(parseInt64(val))
	}
	if val, ok := doc["insertedDocs"]; ok {
		m.insertedDocs.Store(parseInt64(val))
	}
	if val, ok := doc["updatedDocs"]; ok {
		m.updatedDocs.Store(parseInt64(val))
	}
	if val, ok := doc["deletedDocs"]; ok {
		m.deletedDocs.Store(parseInt64(val))
	}

	if val, ok := doc["estimatedTotalBytes"]; ok {
		m.estimatedTotalBytes.Store(parseInt64(val))
	}
	if val, ok := doc["estimatedTotalDocs"]; ok {
		m.estimatedTotalDocs.Store(parseInt64(val))
	}
	if val, ok := doc["clonedBytes"]; ok {
		m.clonedBytes.Store(parseInt64(val))
	}
	if val, ok := doc["clonedDocs"]; ok {
		m.clonedDocs.Store(parseInt64(val))
	}

	if val, ok := doc["cloneCompleted"].(bool); ok {
		m.cloneCompleted.Store(val)
	}
	if val, ok := doc["initialSyncCompleted"].(bool); ok {
		m.initialSyncCompleted.Store(val)
	}

	m.mu.Lock()
	if val, ok := doc["initialSyncStart"].(time.Time); ok {
		m.initialSyncStart = val
	}
	if val, ok := doc["initialSyncEnd"].(time.Time); ok {
		m.initialSyncEnd = val
	}
	m.mu.Unlock()

	//Safe Validation Stats Loading (handles int32/int64/float64 transparently)
	if valStatsRaw, ok := doc["validation"]; ok {
		if valStats, isM := valStatsRaw.(bson.M); isM {
			m.valTotalChecked.Store(parseInt64(valStats["totalChecked"]))
			m.valMismatchFound.Store(parseInt64(valStats["mismatchFound"]))
			m.valMismatchFixed.Store(parseInt64(valStats["mismatchFixed"]))
		} else if valStatsD, isD := valStatsRaw.(bson.D); isD {
			for _, e := range valStatsD {
				switch e.Key {
				case "totalChecked":
					m.valTotalChecked.Store(parseInt64(e.Value))
				case "mismatchFound":
					m.valMismatchFound.Store(parseInt64(e.Value))
				case "mismatchFixed":
					m.valMismatchFixed.Store(parseInt64(e.Value))
				}
			}
		}
	}

	if fc, ok := doc["flowControl"].(bson.M); ok {
		if val, ok := fc["isPaused"].(bool); ok {
			m.fcIsPaused.Store(val)
		}
		if val, ok := fc["pauseReason"].(string); ok {
			m.fcReason.Store(val)
		}
		if val, ok := fc["currentQueuedOps"]; ok {
			m.fcQueuedOps.Store(parseInt64(val))
		}
		if val, ok := fc["currentWriterQueue"]; ok {
			m.fcWriterQueue.Store(parseInt64(val))
		}
		if val, ok := fc["isReplicationLagged"].(bool); ok {
			m.fcNativeLagged.Store(val)
		}
		if val, ok := fc["assignedRateLimit"]; ok {
			m.fcNativeSustainer.Store(parseInt64(val))
		}
	}

	return nil
}

func (m *Manager) Persist(ctx context.Context) {
	m.mu.RLock()
	state := m.state
	info := m.info
	start := m.initialSyncStart
	end := m.initialSyncEnd
	m.mu.RUnlock()

	fcStatus := bson.M{
		"enabled":             config.Cfg.FlowControl.Enabled,
		"isPaused":            m.fcIsPaused.Load(),
		"pauseReason":         m.fcReason.Load().(string),
		"currentQueuedOps":    m.fcQueuedOps.Load(),
		"currentWriterQueue":  m.fcWriterQueue.Load(),
		"isReplicationLagged": m.fcNativeLagged.Load(),
		"assignedRateLimit":   m.fcNativeSustainer.Load(),
	}

	valStats := bson.M{
		"totalChecked":    m.valTotalChecked.Load(),
		"mismatchFound":   m.valMismatchFound.Load(),
		"mismatchFixed":   m.valMismatchFixed.Load(),
		"lastValidatedAt": time.Unix(m.valLastCheck.Load(), 0).UTC(),
	}

	update := bson.M{
		"$set": bson.M{
			"state":                 state,
			"info":                  info,
			"lastUpdated":           time.Now().UTC(),
			"totalEventsApplied":    m.eventsApplied.Load(),
			"insertedDocs":          m.insertedDocs.Load(),
			"updatedDocs":           m.updatedDocs.Load(),
			"deletedDocs":           m.deletedDocs.Load(),
			"estimatedTotalBytes":   m.estimatedTotalBytes.Load(),
			"estimatedTotalDocs":    m.estimatedTotalDocs.Load(),
			"clonedBytes":           m.clonedBytes.Load(),
			"clonedDocs":            m.clonedDocs.Load(),
			"adaptiveSerialBatches": m.serialBatches.Load(),
			"cloneCompleted":        m.cloneCompleted.Load(),
			"initialSyncCompleted":  m.initialSyncCompleted.Load(),
			"initialSyncStart":      start,
			"initialSyncEnd":        end,
			"flowControl":           fcStatus,
			"validation":            valStats,
			"lastSourceEventTime":   m.lastSourceEventTime.Load(),
			"lastAppliedEventTime":  m.lastAppliedEventTime.Load(),
			"lastBatchAppliedAt":    m.lastBatchAppliedAt.Load(),
		},
	}

	opts := options.UpdateOne().SetUpsert(true)
	shortCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if _, err := m.collection.UpdateOne(shortCtx, bson.D{{Key: "_id", Value: m.docID}}, update, opts); err != nil {
		logging.PrintWarning(fmt.Sprintf("[STATUS] Failed to persist status (Transient): %v", err), 0)
	}
}

func (m *Manager) GetCDCIdleMetrics() (float64, float64) {
	lastEvent := m.lastSourceEventTime.Load()
	if lastEvent == 0 {
		return 0, 0
	}
	now := time.Now().UTC().Unix()
	idle := float64(now - lastEvent)

	lastApplied := m.lastAppliedEventTime.Load()
	lag := 0.0
	if lastApplied > 0 {
		lag = float64(lastEvent - lastApplied)
	}
	return idle, lag
}

func (m *Manager) GetStats() StatusOutput {
	// Internal helper to get raw stats without HTTP wrapping if needed
	return m.buildStatusOutput()
}

func (m *Manager) StatusHandler(w http.ResponseWriter, r *http.Request) {
	out := m.buildStatusOutput()
	w.Header().Set("Content-Type", "application/json")
	if b, err := json.MarshalIndent(out, "", "    "); err == nil {
		w.Write(b)
	} else {
		json.NewEncoder(w).Encode(out)
	}
}

// ByteToHuman converts bytes to human readable string
func ByteToHuman(bytes int64) string {
	const (
		kb = 1024
		mb = kb * 1024
		gb = mb * 1024
	)
	f := float64(bytes)
	if bytes >= gb {
		return fmt.Sprintf("%.1f GB", f/gb)
	}
	if bytes >= mb {
		return fmt.Sprintf("%.1f MB", f/mb)
	}
	if bytes >= kb {
		return fmt.Sprintf("%.0f KB", f/kb)
	}
	return fmt.Sprintf("%d B", bytes)
}

func (m *Manager) buildStatusOutput() StatusOutput {
	m.mu.RLock()
	state := m.state
	info := m.info
	start := m.initialSyncStart
	end := m.initialSyncEnd
	m.mu.RUnlock()

	lastSourceUnix := m.lastSourceEventTime.Load()
	lastAppliedUnix := m.lastAppliedEventTime.Load()
	lastBatchUnix := m.lastBatchAppliedAt.Load()

	idleTime := 0.0
	cdcLag := 0.0

	if lastSourceUnix > 0 {
		eventTime := time.Unix(lastSourceUnix, 0)
		idleTime = time.Since(eventTime).Seconds()
		if idleTime < 0 {
			idleTime = 0.0
		}

		if lastAppliedUnix > 0 {
			cdcLag = float64(lastSourceUnix - lastAppliedUnix)
			if cdcLag < 0 {
				cdcLag = 0.0
			}
		}
	}

	totalBytes := m.estimatedTotalBytes.Load()
	clonedBytes := m.clonedBytes.Load()
	var progress float64 = 0
	if totalBytes > 0 {
		progress = (float64(clonedBytes) / float64(totalBytes)) * 100.0
	}
	if m.cloneCompleted.Load() {
		progress = 100.0
	}

	vChecked := m.valTotalChecked.Load()
	vFound := m.valMismatchFound.Load()
	vFixed := m.valMismatchFixed.Load()
	vPending := m.valPending.Load()
	vLast := m.valLastCheck.Load()

	vPercent := 0.0
	if vChecked > 0 {
		validOrFixedCount := vChecked - vPending
		if validOrFixedCount < 0 {
			validOrFixedCount = 0
		}
		vPercent = (float64(validOrFixedCount) / float64(vChecked)) * 100.0
	}

	vLastStr := ""
	if vLast > 0 {
		vLastStr = time.Unix(vLast, 0).UTC().Format(time.RFC3339)
	}

	duration := "N/A"
	if !start.IsZero() {
		if !end.IsZero() {
			duration = end.Sub(start).String()
		} else {
			duration = time.Since(start).String()
		}
	}

	lastSourceTS := TSTime{}
	if lastSourceUnix > 0 {
		lastSourceTS.TS = fmt.Sprintf("%d", lastSourceUnix)
		lastSourceTS.ISODate = time.Unix(lastSourceUnix, 0).UTC().Format(time.RFC3339)
	}

	lastAppliedTS := TSTime{}
	if lastAppliedUnix > 0 {
		lastAppliedTS.TS = fmt.Sprintf("%d", lastAppliedUnix)
		lastAppliedTS.ISODate = time.Unix(lastAppliedUnix, 0).UTC().Format(time.RFC3339)
	}

	lastBatchStr := ""
	if lastBatchUnix > 0 {
		lastBatchStr = time.Unix(lastBatchUnix, 0).UTC().Format(time.RFC3339)
	}

	startStr := ""
	if !start.IsZero() {
		startStr = start.Format(time.RFC3339)
	}
	endStr := ""
	if !end.IsZero() {
		endStr = end.Format(time.RFC3339)
	}

	output := StatusOutput{
		OK:                        state != "error",
		State:                     state,
		Info:                      info,
		TimeSinceLastEventSeconds: idleTime,
		CDCLagSeconds:             cdcLag,
		TotalEventsApplied:        m.eventsApplied.Load(),
		AdaptiveSerialBatches:     m.serialBatches.Load(),
		InsertedDocs:              m.insertedDocs.Load(),
		UpdatedDocs:               m.updatedDocs.Load(),
		DeletedDocs:               m.deletedDocs.Load(),
		Validation: ValidationStats{
			QueuedBatches:  m.valQueueSize.Load(),
			TotalChecked:   vChecked,
			MismatchFound:  vFound,
			MismatchFixed:  vFixed,
			Pending:        vPending,
			HotKeysWaiting: m.valHotKeys.Load(),
			SyncPercent:    vPercent,
			LastValidated:  vLastStr,
		},
		LastSourceEventTime:  lastSourceTS,
		LastAppliedEventTime: lastAppliedTS,
		LastBatchAppliedAt:   lastBatchStr,
		InitialSync: SyncStatus{
			Completed:               m.initialSyncCompleted.Load(),
			ProgressPercent:         progress,
			ClonedDocs:              m.clonedDocs.Load(),
			EstimatedTotalDocs:      m.estimatedTotalDocs.Load(),
			ClonedBytes:             clonedBytes,
			EstimatedTotalBytes:     totalBytes,
			ClonedSizeHuman:         ByteToHuman(clonedBytes),
			EstimatedCloneSizeHuman: ByteToHuman(totalBytes),
			StartedAt:               startStr,
			EndedAt:                 endStr,
			Duration:                duration,
			CompletionLagSeconds:    0,
		},
	}

	if config.Cfg.FlowControl.Enabled {
		output.FlowControl = &FCStatus{
			Enabled:             true,
			IsPaused:            m.fcIsPaused.Load(),
			PauseReason:         m.fcReason.Load().(string),
			CurrentQueuedOps:    int(m.fcQueuedOps.Load()),
			CurrentWriterQueue:  int(m.fcWriterQueue.Load()),
			NativeLagged:        m.fcNativeLagged.Load(),
			NativeSustainerRate: int(m.fcNativeSustainer.Load()),
		}
	}

	return output
}

func parseInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case float64:
		return int64(val)
	default:
		return 0
	}
}
