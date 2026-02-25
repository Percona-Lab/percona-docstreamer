package flow

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"github.com/Percona-Lab/percona-docstreamer/internal/status"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Manager struct {
	targetClient     *mongo.Client
	monitoredClients []*mongo.Client
	nodeNames        map[*mongo.Client]string
	isPaused         atomic.Bool
	stopChan         chan struct{}
	statusMgr        *status.Manager
	user             string
	password         string

	currentQueuedOps atomic.Int64
	currentWTTickets atomic.Int64
}

func NewManager(targetClient *mongo.Client, statusMgr *status.Manager, user, password string) *Manager {
	return &Manager{
		targetClient:     targetClient,
		monitoredClients: []*mongo.Client{},
		nodeNames:        make(map[*mongo.Client]string),
		stopChan:         make(chan struct{}),
		statusMgr:        statusMgr,
		user:             user,
		password:         password,
	}
}

func (m *Manager) Start() {
	if !config.Cfg.FlowControl.Enabled {
		return
	}
	logging.PrintInfo("[FlowControl] Started. Establishing dedicated control channels...", 0)

	if err := m.discoverCluster(); err != nil {
		logging.PrintWarning(fmt.Sprintf("[FlowControl] Shard discovery failed: %v. Monitoring limited to connected node.", err), 0)
		m.monitoredClients = []*mongo.Client{m.targetClient}
		m.nodeNames[m.targetClient] = m.getRealNodeName(m.targetClient, "Primary/Mongos")
	} else {
		logging.PrintInfo(fmt.Sprintf("[FlowControl] Discovery complete. Monitoring %d node(s).", len(m.monitoredClients)), 0)
	}

	go m.monitorLoop()
}

func (m *Manager) Stop() {
	close(m.stopChan)
	for _, client := range m.monitoredClients {
		if client != m.targetClient {
			_ = client.Disconnect(context.Background())
		}
	}
}

func (m *Manager) Wait() {
	if !config.Cfg.FlowControl.Enabled {
		return
	}
	for m.isPaused.Load() {
		time.Sleep(100 * time.Millisecond)
	}
}

func (m *Manager) WaitIfPaused() {
	if !config.Cfg.FlowControl.Enabled {
		return
	}

	// 1. Fast Path: If not paused, return immediately (zero allocation)
	if !m.isPaused.Load() {
		return
	}

	// 2. Slow Path: We are paused.
	// Log the event once so we know why the reader stopped.
	logging.PrintInfo("[CDC] Reader paused due to Flow Control (Target Overload)...", 0)

	// Use a ticker to check the state periodically.
	// We check every 500ms because the monitor updates the state every 1000ms.
	// This balances responsiveness with CPU usage.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for m.isPaused.Load() {
		<-ticker.C
	}

	// 3. Resumed
	logging.PrintInfo("[CDC] Reader resumed. Target is healthy.", 0)
}

// GetStatus returns the current state.
func (m *Manager) GetStatus() (bool, int) {
	return m.isPaused.Load(), int(m.currentQueuedOps.Load())
}

func (m *Manager) getRealNodeName(client *mongo.Client, fallback string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var hello bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err == nil {
		if me, ok := hello["me"].(string); ok && me != "" {
			return me
		}
	}
	return fallback
}

func (m *Manager) discoverCluster() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	m.monitoredClients = append(m.monitoredClients, m.targetClient)
	realName := m.getRealNodeName(m.targetClient, "Entry Node (Mongos)")
	m.nodeNames[m.targetClient] = realName
	logging.PrintInfo(fmt.Sprintf("[FlowControl] Verified Monitor 1: %s (Type: Entry)", realName), 0)

	var helloResult bson.M
	if err := m.targetClient.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&helloResult); err != nil {
		return err
	}

	msg, _ := helloResult["msg"].(string)
	if msg != "isdbgrid" {
		logging.PrintInfo("[FlowControl] Target is Standalone/ReplicaSet (Not Sharded).", 0)
		return nil
	}

	logging.PrintInfo("[FlowControl] Sharded Cluster (mongos) detected. Auto-discovering backend shards...", 0)

	cursor, err := m.targetClient.Database("config").Collection("shards").Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to query config.shards: %w", err)
	}
	defer cursor.Close(ctx)

	var shards []struct {
		ID   string `bson:"_id"`
		Host string `bson:"host"`
	}
	if err := cursor.All(ctx, &shards); err != nil {
		return fmt.Errorf("failed to decode shards: %w", err)
	}

	for _, shard := range shards {
		parts := strings.Split(shard.Host, "/")
		if len(parts) < 2 {
			continue
		}
		rsName := parts[0]
		hosts := parts[1]

		safeUser := url.QueryEscape(m.user)
		safePass := url.QueryEscape(m.password)

		queryParams := url.Values{}
		queryParams.Set("replicaSet", rsName)
		queryParams.Set("authSource", "admin")
		queryParams.Set("authMechanism", "SCRAM-SHA-256")

		if config.Cfg.Mongo.ExtraParams != "" {
			extra, err := url.ParseQuery(config.Cfg.Mongo.ExtraParams)
			if err == nil {
				for k, v := range extra {
					if k != "replicaSet" && k != "authSource" && k != "tls" {
						for _, val := range v {
							queryParams.Add(k, val)
						}
					}
				}
			}
		}

		if config.Cfg.Mongo.TLS {
			queryParams.Set("tls", "true")
			if config.Cfg.Mongo.TlsAllowInvalidHostnames {
				queryParams.Set("tlsAllowInvalidHostnames", "true")
			}
			if config.Cfg.Mongo.CaFile != "" {
				queryParams.Set("tlsCAFile", config.Cfg.Mongo.CaFile)
			}
		}

		shardURI := fmt.Sprintf("mongodb://%s:%s@%s/?%s",
			safeUser, safePass, hosts, queryParams.Encode())

		clientOpts := options.Client().ApplyURI(shardURI).SetDirect(false)
		shardClient, err := mongo.Connect(clientOpts)
		if err != nil {
			logging.PrintWarning(fmt.Sprintf("[FlowControl] Failed to connect to shard %s: %v", shard.ID, err), 0)
			continue
		}

		pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := shardClient.Ping(pingCtx, nil); err != nil {
			logging.PrintWarning(fmt.Sprintf("[FlowControl] Shard %s unreachable: %v. Skipping.", shard.ID, err), 0)
			_ = shardClient.Disconnect(context.Background())
			pingCancel()
			continue
		}
		pingCancel()

		shardRealName := m.getRealNodeName(shardClient, fmt.Sprintf("Shard-%s", shard.ID))
		logging.PrintInfo(fmt.Sprintf("[FlowControl] Verified Monitor: %s (Shard: %s)", shardRealName, shard.ID), 0)

		m.monitoredClients = append(m.monitoredClients, shardClient)
		m.nodeNames[shardClient] = shardRealName
	}

	return nil
}

func (m *Manager) monitorLoop() {
	// Perform an initial health check immediately before starting the ticker
	m.checkHealth()

	interval := config.Cfg.FlowControl.CheckIntervalMS

	// Enforce a sane minimum to prevent tight loops (e.g., 100ms).
	if interval < 100 {
		interval = 1000 // Default to 1 second if config is missing or too dangerous
	}

	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			m.checkHealth()
		}
	}
}

func (m *Manager) checkHealth() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var maxQueuedOps int
	var maxActiveClients int
	var maxLatencyMs int64

	var maxWriterQueue int
	var isFlowControlLagged bool
	var maxSustainerRate int

	minWTTicketsAvailable := math.MaxInt

	var latencyErr error
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, client := range m.monitoredClients {
		wg.Add(1)
		go func(c *mongo.Client) {
			defer wg.Done()

			nodeName := "Unknown"
			mu.Lock()
			if name, ok := m.nodeNames[c]; ok {
				nodeName = name
			}
			mu.Unlock()

			start := time.Now()
			var raw bson.Raw
			err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&raw)
			latency := time.Since(start).Milliseconds()

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				if strings.Contains(err.Error(), "connecting to a sharded cluster improperly") {
					return
				}
				latencyErr = err
				maxLatencyMs = 9999
				if config.Cfg.Logging.Level == "debug" {
					logging.PrintWarning(fmt.Sprintf("[FlowControl DEBUG] Node: %s | Status: FAILED (%v)", nodeName, err), 0)
				}
			} else {
				if latency > maxLatencyMs {
					maxLatencyMs = latency
				}

				// 1. Global Lock
				lockVal := raw.Lookup("globalLock", "currentQueue", "total")
				qOps := getIntFromRaw(lockVal)

				writerVal := raw.Lookup("globalLock", "currentQueue", "writers")
				wOps := getIntFromRaw(writerVal)

				activeVal := raw.Lookup("globalLock", "activeClients", "total")
				activeClients := getIntFromRaw(activeVal)

				// 2. WiredTiger Tickets
				wtStr := "N/A"
				wtVal := raw.Lookup("wiredTiger", "concurrentTransactions", "write", "available")
				if wtVal.Type != bson.Type(0) {
					wtAvailable := getIntFromRaw(wtVal)
					wtStr = fmt.Sprintf("%d", wtAvailable)
					if wtAvailable < minWTTicketsAvailable {
						minWTTicketsAvailable = wtAvailable
					}
				}

				// 3. Native Flow Control Checks
				fcLagged := false
				fcSustainer := 0

				fcVal := raw.Lookup("flowControl")
				if fcVal.Type != bson.Type(0) {
					isLaggedVal := raw.Lookup("flowControl", "isLagged")
					if isLaggedVal.Type == bson.TypeBoolean {
						fcLagged = isLaggedVal.Boolean()
					}

					susVal := raw.Lookup("flowControl", "sustainerRate")
					if susVal.Type != bson.Type(0) {
						fcSustainer = getIntFromRaw(susVal)
					}
				}

				// Aggregation
				if qOps > maxQueuedOps {
					maxQueuedOps = qOps
				}
				if wOps > maxWriterQueue {
					maxWriterQueue = wOps
				}
				if activeClients > maxActiveClients {
					maxActiveClients = activeClients
				}

				if fcLagged {
					isFlowControlLagged = true
				}
				if fcSustainer > maxSustainerRate {
					maxSustainerRate = fcSustainer
				}

				fcStatus := "OK"
				if fcLagged {
					fcStatus = "LAGGED"
				}

				if config.Cfg.Logging.Level == "debug" {
					logging.PrintInfo(fmt.Sprintf("[FlowControl DEBUG] Node: %s | Queue: %d (W:%d) | FC: %s (Rate:%d) | WT: %s | Latency: %dms",
						nodeName, qOps, wOps, fcStatus, fcSustainer, wtStr, latency), 0)
				}
			}
		}(client)
	}
	wg.Wait()

	m.currentQueuedOps.Store(int64(maxQueuedOps))
	m.currentWTTickets.Store(int64(minWTTicketsAvailable))

	shouldPause := false
	reason := ""

	latencyThreshold := config.Cfg.FlowControl.LatencyThresholdMS
	if latencyThreshold <= 0 {
		latencyThreshold = 250
	}

	if maxLatencyMs > int64(latencyThreshold) {
		shouldPause = true
		reason = fmt.Sprintf("High Latency: %dms > %dms (Err: %v)", maxLatencyMs, latencyThreshold, latencyErr)
	}

	// --- 1. Native Flow Control Logic ---
	if !shouldPause && isFlowControlLagged {
		shouldPause = true
		reason = "Native Flow Control: Replication Lag Detected (isLagged=true)"
	}

	// --- 2. Global Lock Queue Logic ---
	if !shouldPause {
		if limit := config.Cfg.FlowControl.TargetMaxQueuedOps; maxQueuedOps > limit {
			shouldPause = true
			reason = fmt.Sprintf("High Queue: %d > %d", maxQueuedOps, limit)
		}
	}

	// --- 3. Active Clients Logic ---
	if !shouldPause {
		limit := config.Cfg.FlowControl.ActiveClientThreshold
		if limit <= 0 {
			limit = 50
		}
		if maxActiveClients > limit {
			shouldPause = true
			reason = fmt.Sprintf("High Active Clients: %d > %d", maxActiveClients, limit)
		}
	}

	// --- 4. WiredTiger Logic ---
	if !shouldPause {
		limit := config.Cfg.FlowControl.MinWiredTigerTickets
		if limit > 0 {
			if minWTTicketsAvailable == math.MaxInt {
				// skip
			} else if minWTTicketsAvailable <= limit {
				shouldPause = true
				reason = fmt.Sprintf("Low WiredTiger Tickets: %d <= %d", minWTTicketsAvailable, limit)
			}
		}
	}

	if m.statusMgr != nil {
		m.statusMgr.UpdateFlowControl(
			shouldPause,
			reason,
			maxQueuedOps,
			maxWriterQueue,
			isFlowControlLagged,
			maxSustainerRate,
		)
	}

	if shouldPause {
		if !m.isPaused.Load() {
			logging.PrintWarning(fmt.Sprintf("[FlowControl] THROTTLING PAUSED: %s", reason), 0)
			m.isPaused.Store(true)
			if m.statusMgr != nil {
				m.statusMgr.SetState("throttled", fmt.Sprintf("Flow Control Active: %s", reason))
			}
		}
		time.Sleep(time.Duration(config.Cfg.FlowControl.PauseDurationMS) * time.Millisecond)
	} else {
		if m.isPaused.Load() {
			logging.PrintInfo("[FlowControl] Throttling released. Cluster healthy.", 0)
			m.isPaused.Store(false)
			if m.statusMgr != nil {
				if m.statusMgr.IsMigrationFinalized() {
					m.statusMgr.SetState("completed", "Migration Finalized")
				} else if m.statusMgr.IsCloneCompleted() {
					m.statusMgr.SetState("running", "Change Data Capture")
				} else {
					m.statusMgr.SetState("running", "Initial Sync")
				}
			}
		}
	}
}

func getIntFromRaw(val bson.RawValue) int {
	if val.Type == bson.Type(0) {
		return 0
	}
	switch val.Type {
	case bson.TypeInt32:
		return int(val.Int32())
	case bson.TypeInt64:
		return int(val.Int64())
	case bson.TypeDouble:
		return int(val.Double())
	default:
		return 0
	}
}
