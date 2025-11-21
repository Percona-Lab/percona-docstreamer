package status

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Percona-Lab/docMongoStream/internal/config"
	"github.com/Percona-Lab/docMongoStream/internal/logging"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Status represents the current state of the migration
type Status struct {
	State                string              `json:"state"`
	LastStateChange      time.Time           `json:"lastStateChange"`
	Error                string              `json:"error,omitempty"`
	CloneCompleted       bool                `json:"cloneCompleted"`
	InitialSyncCompleted bool                `json:"initialSyncCompleted"`
	Lag                  int64               `json:"lag,omitempty"`
	EstimatedBytes       int64               `json:"estimatedBytes"`
	ClonedBytes          int64               `json:"clonedBytes"`
	EventsApplied        int64               `json:"eventsApplied"`
	LastEventTimestamp   primitive.Timestamp `json:"lastEventTimestamp,omitempty"`
	LastBatchAppliedAt   time.Time           `json:"lastBatchAppliedAt,omitempty"`
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
	OK                        bool        `json:"ok"`
	State                     string      `json:"state"`
	Info                      string      `json:"info"`
	TimeSinceLastEventSeconds float64     `json:"timeSinceLastEventSeconds"` // Metric: Source Idle Time (Grows when quiet)
	CDCLagSeconds             float64     `json:"cdcLagSeconds"`             // Metric: Processing Latency (Stays low when quiet)
	EventsApplied             int64       `json:"totalEventsApplied"`
	LastSourceEventTime       OpTimeInfo  `json:"lastSourceEventTime"`
	LastBatchAppliedAt        string      `json:"lastBatchAppliedAt"`
	InitialSync               InitialSync `json:"initialSync"`
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
	lastEventTimestamp   primitive.Timestamp
	lastBatchAppliedAt   time.Time

	targetClient *mongo.Client
	isPersisted  bool
	lock         sync.RWMutex
}

func NewManager(targetClient *mongo.Client, isPersisted bool) *Manager {
	logging.PrintInfo(fmt.Sprintf("Status manager initialized (collection: %s.%s)",
		config.Cfg.Migration.MetadataDB, config.Cfg.Migration.StatusCollection), 0)

	return &Manager{
		state:           "starting",
		lastStateChange: time.Now().UTC(),
		targetClient:    targetClient,
		isPersisted:     isPersisted,
	}
}

func (m *Manager) SetState(state, message string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.state = fmt.Sprintf("%s (%s)", state, message)
	m.lastStateChange = time.Now().UTC()
	logging.PrintInfo(fmt.Sprintf("[STATUS] State changed to: %s", m.state), 0)
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

// UpdateCDCStats updates the CDC counters and records wall time if progress is made
func (m *Manager) UpdateCDCStats(eventsApplied int64, lastEventTS primitive.Timestamp) {
	m.lock.Lock()
	defer m.lock.Unlock()

	// Only update the "Last Applied" wall clock time if we actually applied something new
	if eventsApplied > m.eventsApplied {
		m.lastBatchAppliedAt = time.Now().UTC()
	}

	m.eventsApplied = eventsApplied
	m.lastEventTimestamp = lastEventTS
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
	m.lastBatchAppliedAt = s.LastBatchAppliedAt
	m.isPersisted = true
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
		"lastBatchAppliedAt":   m.lastBatchAppliedAt,
	}

	opts := options.Replace().SetUpsert(true)
	_, err := coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: docID}}, doc, opts)
	if err != nil {
		logging.PrintWarning(fmt.Sprintf("[STATUS] Failed to persist status: %v", err), 0)
	} else {
		m.isPersisted = true
	}
}

func (m *Manager) StartServer(port string) {
	if port == "" {
		port = "8080"
	}
	addr := ":" + port
	mux := http.NewServeMux()
	mux.HandleFunc("/status", m.statusHandler)
	logging.PrintInfo(fmt.Sprintf("Starting status HTTP server on :%s/status", port), 0)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logging.PrintWarning(fmt.Sprintf("Status server failed: %v", err), 0)
	}
}

func (m *Manager) statusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	json, err := m.ToJSON()
	if err != nil {
		http.Error(w, "Failed to serialize status", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(json)
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

func (m *Manager) ToJSON() ([]byte, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	// 1. Last Source Event Info
	opTimeInfo := OpTimeInfo{}
	if m.lastEventTimestamp.T > 0 {
		opTimeInfo.TS = fmt.Sprintf("%d.%d", m.lastEventTimestamp.T, m.lastEventTimestamp.I)
		opTime := time.Unix(int64(m.lastEventTimestamp.T), 0).UTC()
		opTimeInfo.ISODate = opTime.Format("2006-01-02T15:04:05Z")
	}

	var idleTime float64
	var cdcLag float64

	// Only calculate if we have processed at least one event
	if m.initialSyncCompleted && m.lastEventTimestamp.T > 0 {
		eventTime := time.Unix(int64(m.lastEventTimestamp.T), 0)

		// Metric 1: Time Since Last Event (Idle Time)
		// Grows when source is quiet
		idleTime = time.Since(eventTime).Seconds()
		if idleTime < 0 {
			idleTime = 0.0
		}

		// Metric 2: CDC Lag (End-to-End Latency)
		// Difference between when event happened (Source) and when it was applied (Dest)
		// Stays constant/low when source is quiet
		if !m.lastBatchAppliedAt.IsZero() {
			cdcLag = m.lastBatchAppliedAt.Sub(eventTime).Seconds()
			if cdcLag < 0 {
				cdcLag = 0.0
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

	s := StatusOutput{
		OK:                        baseState != "error",
		State:                     baseState,
		Info:                      info,
		TimeSinceLastEventSeconds: idleTime, // IDLE TIME
		CDCLagSeconds:             cdcLag,   // LATENCY
		EventsApplied:             m.eventsApplied,
		LastSourceEventTime:       opTimeInfo,
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
	return json.MarshalIndent(s, "", "    ")
}
