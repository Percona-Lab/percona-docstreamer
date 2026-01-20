package status

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type FlowControlStatus struct {
	Enabled          bool   `json:"enabled"`
	IsPaused         bool   `json:"isPaused"`
	PauseReason      string `json:"pauseReason,omitempty"`
	TargetQueuedOps  int    `json:"targetQueuedOps"`
	TargetResidentMB int    `json:"targetResidentMB"`
}

// ValidationInfo shows the sync progress
type ValidationInfo struct {
	TotalChecked    int64   `json:"totalChecked"`
	ValidCount      int64   `json:"validCount"`
	MismatchCount   int64   `json:"mismatchCount"`
	SyncPercent     float64 `json:"syncPercent"`
	LastValidatedAt string  `json:"lastValidatedAt"`
}

// Status represents the current state of the migration
type Status struct {
	State                string         `json:"state"`
	LastStateChange      time.Time      `json:"lastStateChange"`
	Error                string         `json:"error,omitempty"`
	CloneCompleted       bool           `json:"cloneCompleted"`
	InitialSyncCompleted bool           `json:"initialSyncCompleted"`
	Lag                  int64          `json:"lag,omitempty"`
	EstimatedBytes       int64          `json:"estimatedBytes"`
	ClonedBytes          int64          `json:"clonedBytes"`
	EventsApplied        int64          `json:"eventsApplied"`
	LastEventTimestamp   bson.Timestamp `json:"lastEventTimestamp,omitempty"`
	LastAppliedTimestamp bson.Timestamp `json:"lastAppliedTimestamp,omitempty"`
	LastBatchAppliedAt   time.Time      `json:"lastBatchAppliedAt,omitempty"`
	FlowQueuedOps        int            `bson:"flowQueuedOps"`
	FlowResidentMB       int            `bson:"flowResidentMB"`
}

type OpTimeInfo struct {
	TS      string `json:"ts"`
	ISODate string `json:"isoDate"`
}

type InitialSync struct {
	Completed               bool   `json:"completed"`
	CompletionLagSeconds    int64  `json:"completionLagSeconds"`
	CloneCompleted          bool   `json:"cloneCompleted"`
	EstimatedCloneSizeBytes int64  `json:"estimatedCloneSizeBytes"`
	ClonedSizeBytes         int64  `json:"clonedSizeBytes"`
	EstimatedCloneSizeHuman string `json:"estimatedCloneSizeHuman"`
	ClonedSizeHuman         string `json:"clonedSizeHuman"`
}

type StatusOutput struct {
	OK                        bool               `json:"ok"`
	State                     string             `json:"state"`
	Info                      string             `json:"info"`
	FlowControl               *FlowControlStatus `json:"flowControl,omitempty"`
	TimeSinceLastEventSeconds float64            `json:"timeSinceLastEventSeconds"`
	CDCLagSeconds             float64            `json:"cdcLagSeconds"`
	EventsApplied             int64              `json:"totalEventsApplied"`
	Validation                ValidationInfo     `json:"validation"`
	LastSourceEventTime       OpTimeInfo         `json:"lastSourceEventTime"`
	LastAppliedEventTime      OpTimeInfo         `json:"lastAppliedEventTime"`
	LastBatchAppliedAt        string             `json:"lastBatchAppliedAt"`
	InitialSync               InitialSync        `json:"initialSync"`
}

type Manager struct {
	state                string
	lastStateChange      time.Time
	error                string
	cloneCompleted       bool
	initialSyncCompleted bool
	lag                  int64
	estimatedBytes       int64
	clonedBytes          int64
	eventsApplied        int64
	lastEventTimestamp   bson.Timestamp
	lastAppliedTimestamp bson.Timestamp
	lastBatchAppliedAt   time.Time
	flowStatus           FlowControlStatus
	targetClient         *mongo.Client
	isPersisted          bool
	lock                 sync.RWMutex
}

func NewManager(targetClient *mongo.Client, isPersisted bool) *Manager {
	logging.PrintInfo(fmt.Sprintf("Status manager initialized (collection: %s.%s)",
		config.Cfg.Migration.MetadataDB, config.Cfg.Migration.StatusCollection), 0)

	mgr := &Manager{
		state:           "starting",
		lastStateChange: time.Now().UTC(),
		targetClient:    targetClient,
		isPersisted:     isPersisted,
	}

	// Initialize Flow Control status immediately from config
	// This ensures it reports "enabled: true" even before the first health check runs
	if config.Cfg.FlowControl.Enabled {
		mgr.flowStatus.Enabled = true
	}

	return mgr
}

func (m *Manager) SetState(state, message string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.state = fmt.Sprintf("%s (%s)", state, message)
	m.lastStateChange = time.Now().UTC()
	logging.PrintInfo(fmt.Sprintf("[STATUS] State changed to: %s", m.state), 0)
}

// IsCDCActive checks if the application is currently in the CDC phase
func (m *Manager) IsCDCActive() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return strings.HasPrefix(m.state, "running") && m.initialSyncCompleted
}

// IsCloneCompleted returns the current full load status
func (m *Manager) IsCloneCompleted() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.cloneCompleted
}

func (m *Manager) SetError(errMsg string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.state = "error"
	m.error = errMsg
	m.lastStateChange = time.Now().UTC()
}

func (m *Manager) AddEstimatedBytes(bytes int64) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.estimatedBytes += bytes
}

func (m *Manager) AddClonedBytes(bytes int64) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.clonedBytes += bytes
}

func (m *Manager) SetCloneCompleted() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.cloneCompleted = true
	if m.estimatedBytes > m.clonedBytes {
		m.clonedBytes = m.estimatedBytes
	}
}

func (m *Manager) SetInitialSyncCompleted(lag int64) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.initialSyncCompleted = true
	m.lag = lag
}

func (m *Manager) GetEventsApplied() int64 {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.eventsApplied
}

// UpdateCDCStats updates the "Read" stats
func (m *Manager) UpdateCDCStats(eventsApplied int64, lastEventTS bson.Timestamp) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.eventsApplied = eventsApplied
	m.lastEventTimestamp = lastEventTS
}

// UpdateAppliedStats updates the "Write" stats (called by flush workers)
func (m *Manager) UpdateAppliedStats(lastAppliedTS bson.Timestamp) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.lastBatchAppliedAt = time.Now().UTC()

	// Only update if newer
	if lastAppliedTS.T > m.lastAppliedTimestamp.T ||
		(lastAppliedTS.T == m.lastAppliedTimestamp.T && lastAppliedTS.I > m.lastAppliedTimestamp.I) {
		m.lastAppliedTimestamp = lastAppliedTS
	}
}

func (m *Manager) UpdateFlowControl(paused bool, reason string, queuedOps, residentMB int) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.flowStatus = FlowControlStatus{
		Enabled:          true,
		IsPaused:         paused,
		PauseReason:      reason,
		TargetQueuedOps:  queuedOps,
		TargetResidentMB: residentMB,
	}

	go m.Persist(context.Background())
}

func (m *Manager) LoadAndMerge(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	dbName := config.Cfg.Migration.MetadataDB
	collName := config.Cfg.Migration.StatusCollection
	docID := config.Cfg.Migration.StatusDocID
	coll := m.targetClient.Database(dbName).Collection(collName)

	var s Status
	err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: docID}}).Decode(&s)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil
		}
		return fmt.Errorf("failed to load status: %w", err)
	}

	m.state = s.State
	m.lastStateChange = s.LastStateChange
	m.error = s.Error
	m.cloneCompleted = s.CloneCompleted
	m.initialSyncCompleted = s.InitialSyncCompleted
	m.lag = s.Lag
	m.estimatedBytes = s.EstimatedBytes
	m.clonedBytes = s.ClonedBytes
	m.eventsApplied = s.EventsApplied
	m.lastEventTimestamp = s.LastEventTimestamp
	m.lastAppliedTimestamp = s.LastAppliedTimestamp
	m.lastBatchAppliedAt = s.LastBatchAppliedAt
	m.isPersisted = true
	m.flowStatus.TargetQueuedOps = s.FlowQueuedOps
	m.flowStatus.TargetResidentMB = s.FlowResidentMB
	return nil
}

func (m *Manager) Persist(ctx context.Context) {
	m.lock.Lock()
	defer m.lock.Unlock()

	dbName := config.Cfg.Migration.MetadataDB
	collName := config.Cfg.Migration.StatusCollection
	docID := config.Cfg.Migration.StatusDocID
	coll := m.targetClient.Database(dbName).Collection(collName)

	doc := bson.M{
		"_id":                  docID,
		"state":                m.state,
		"lastStateChange":      m.lastStateChange,
		"error":                m.error,
		"cloneCompleted":       m.cloneCompleted,
		"initialSyncCompleted": m.initialSyncCompleted,
		"lag":                  m.lag,
		"estimatedBytes":       m.estimatedBytes,
		"clonedBytes":          m.clonedBytes,
		"eventsApplied":        m.eventsApplied,
		"lastEventTimestamp":   m.lastEventTimestamp,
		"lastAppliedTimestamp": m.lastAppliedTimestamp,
		"lastBatchAppliedAt":   m.lastBatchAppliedAt,
		"flowQueuedOps":        m.flowStatus.TargetQueuedOps,
		"flowResidentMB":       m.flowStatus.TargetResidentMB,
	}

	opts := options.Replace().SetUpsert(true)
	_, err := coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: docID}}, doc, opts)
	if err != nil {
		logging.PrintWarning(fmt.Sprintf("[STATUS] Failed to persist status: %v", err), 0)
	} else {
		m.isPersisted = true
	}
}

func (m *Manager) StatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	jsonBytes, err := m.ToJSON(r.Context())
	if err != nil {
		http.Error(w, "Failed to serialize status", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonBytes)
}

func ByteToHuman(bytes int64) string {
	const (
		kb = 1024
		mb = kb * 1024
		gb = mb * 1024
	)
	f := float64(bytes)
	if bytes >= gb {
		return fmt.Sprintf("%.f GB", f/gb)
	}
	if bytes >= mb {
		return fmt.Sprintf("%.1f MB", f/mb)
	}
	if bytes >= kb {
		return fmt.Sprintf("%.f KB", f/kb)
	}
	return fmt.Sprintf("%d B", bytes)
}

// getValidationStats helper to fetch validation stats directly from DB
func (m *Manager) getValidationStats(ctx context.Context) ValidationInfo {
	statsColl := m.targetClient.Database(config.Cfg.Migration.MetadataDB).Collection(config.Cfg.Migration.ValidationStatsCollection)
	failColl := m.targetClient.Database(config.Cfg.Migration.MetadataDB).Collection(config.Cfg.Migration.ValidationFailuresCollection)

	// 1. Aggregate totals (cumulative throughput) from stats
	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$total_checked"}}},
			{Key: "valid", Value: bson.D{{Key: "$sum", Value: "$valid_count"}}},
			// We ignore mismatch sum from stats as it's historical
			{Key: "lastVal", Value: bson.D{{Key: "$max", Value: "$last_updated"}}},
		}}},
	}

	cursor, err := statsColl.Aggregate(ctx, pipeline)
	if err != nil {
		return ValidationInfo{}
	}
	defer cursor.Close(ctx)

	var result struct {
		Total   int64     `bson:"total"`
		Valid   int64     `bson:"valid"`
		LastVal time.Time `bson:"lastVal"`
	}

	if cursor.Next(ctx) {
		if err := cursor.Decode(&result); err != nil {
			logging.PrintWarning(fmt.Sprintf("[STATUS] Failed to decode stats: %v", err), 0)
		}
	}

	// 2. Count ACTUAL active failures
	currentMismatch, err := failColl.CountDocuments(ctx, bson.D{})
	if err != nil {
		logging.PrintWarning(fmt.Sprintf("[STATUS] Failed to count failures: %v", err), 0)
		currentMismatch = 0
	}

	percent := 0.0
	if result.Total > 0 {
		percent = (float64(result.Valid) / float64(result.Total)) * 100.0
	}

	lastValStr := ""
	if !result.LastVal.IsZero() {
		lastValStr = result.LastVal.Format(time.RFC3339)
	}

	return ValidationInfo{
		TotalChecked:    result.Total,
		ValidCount:      result.Valid,
		MismatchCount:   currentMismatch, // Use live count
		SyncPercent:     percent,
		LastValidatedAt: lastValStr,
	}
}

// GetStats returns a snapshot of the current status for internal logic
func (m *Manager) GetStats() StatusOutput {
	m.lock.RLock()
	defer m.lock.RUnlock()

	var cdcLag float64
	if m.initialSyncCompleted && m.lastEventTimestamp.T > 0 {
		eventTime := time.Unix(int64(m.lastEventTimestamp.T), 0)
		if !m.lastBatchAppliedAt.IsZero() {
			if time.Since(m.lastBatchAppliedAt) > 10*time.Second {
				cdcLag = 0.0
			} else {
				cdcLag = m.lastBatchAppliedAt.Sub(eventTime).Seconds()
				if cdcLag < 0 {
					cdcLag = 0.0
				}
			}
		}
	}

	// We use a background context here because this is for internal logic
	valInfo := m.getValidationStats(context.Background())

	return StatusOutput{
		CDCLagSeconds: cdcLag,
		Validation:    valInfo,
	}
}

func (m *Manager) ToJSON(ctx context.Context) ([]byte, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	opTimeInfo := OpTimeInfo{}
	if m.lastEventTimestamp.T > 0 {
		opTimeInfo.TS = fmt.Sprintf("%d.%d", m.lastEventTimestamp.T, m.lastEventTimestamp.I)
		opTime := time.Unix(int64(m.lastEventTimestamp.T), 0).UTC()
		opTimeInfo.ISODate = opTime.Format("2006-01-02T15:04:05Z")
	}

	// Applied Time Info
	appliedTimeInfo := OpTimeInfo{}
	if m.lastAppliedTimestamp.T > 0 {
		appliedTimeInfo.TS = fmt.Sprintf("%d.%d", m.lastAppliedTimestamp.T, m.lastAppliedTimestamp.I)
		apTime := time.Unix(int64(m.lastAppliedTimestamp.T), 0).UTC()
		appliedTimeInfo.ISODate = apTime.Format("2006-01-02T15:04:05Z")
	}

	var idleTime float64
	var cdcLag float64

	if m.initialSyncCompleted && m.lastEventTimestamp.T > 0 {
		eventTime := time.Unix(int64(m.lastEventTimestamp.T), 0)
		idleTime = time.Since(eventTime).Seconds()
		if idleTime < 0 {
			idleTime = 0.0
		}

		if !m.lastBatchAppliedAt.IsZero() {
			if time.Since(m.lastBatchAppliedAt) > 10*time.Second {
				cdcLag = 0.0
			} else {
				cdcLag = m.lastBatchAppliedAt.Sub(eventTime).Seconds()
				if cdcLag < 0 {
					cdcLag = 0.0
				}
			}
		}
	}

	baseState := m.state
	info := ""
	if idx := strings.Index(m.state, " ("); idx != -1 {
		baseState = m.state[:idx]
		info = strings.TrimSuffix(m.state[idx+2:], ")")
	} else {
		info = m.state
	}

	lastBatchStr := ""
	if !m.lastBatchAppliedAt.IsZero() {
		lastBatchStr = m.lastBatchAppliedAt.Format(time.RFC3339)
	}

	valInfo := m.getValidationStats(ctx)

	s := StatusOutput{
		OK:                        baseState != "error",
		State:                     baseState,
		Info:                      info,
		FlowControl:               &m.flowStatus,
		TimeSinceLastEventSeconds: idleTime,
		CDCLagSeconds:             cdcLag,
		EventsApplied:             m.eventsApplied,
		Validation:                valInfo,
		LastSourceEventTime:       opTimeInfo,
		LastAppliedEventTime:      appliedTimeInfo,
		LastBatchAppliedAt:        lastBatchStr,
		InitialSync: InitialSync{
			Completed:               m.initialSyncCompleted,
			CompletionLagSeconds:    m.lag,
			CloneCompleted:          m.cloneCompleted,
			EstimatedCloneSizeBytes: m.estimatedBytes,
			ClonedSizeBytes:         m.clonedBytes,
			EstimatedCloneSizeHuman: ByteToHuman(m.estimatedBytes),
			ClonedSizeHuman:         ByteToHuman(m.clonedBytes),
		},
	}
	// If flow control is disabled in config, we can hide it or show enabled: false
	if !config.Cfg.FlowControl.Enabled {
		s.FlowControl = nil
	}
	return json.MarshalIndent(s, "", "    ")
}
