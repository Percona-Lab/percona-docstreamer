package flow

import (
	"context"
	"fmt"
	"net/url" // <--- Added import
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
	isPaused         atomic.Bool
	stopChan         chan struct{}
	statusMgr        *status.Manager
	user             string
	password         string
}

func NewManager(targetClient *mongo.Client, statusMgr *status.Manager, user, password string) *Manager {
	return &Manager{
		targetClient: targetClient,
		stopChan:     make(chan struct{}),
		statusMgr:    statusMgr,
		user:         user,
		password:     password,
	}
}

func (m *Manager) Start() {
	if !config.Cfg.FlowControl.Enabled {
		return
	}
	logging.PrintInfo("[FlowControl] Started. Monitoring target database health...", 0)

	// 1. Discover Shards (if connected to Mongos)
	if err := m.discoverCluster(); err != nil {
		logging.PrintWarning(fmt.Sprintf("[FlowControl] Shard discovery failed: %v. Will only monitor the connected node.", err), 0)
		m.monitoredClients = []*mongo.Client{m.targetClient}
	}

	go m.monitorLoop()
}

func (m *Manager) Stop() {
	close(m.stopChan)
	for _, client := range m.monitoredClients {
		if client != m.targetClient {
			client.Disconnect(context.Background())
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

// discoverCluster checks if we are on a mongos and connects to shards if needed
func (m *Manager) discoverCluster() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Check if we are connected to a Mongos
	var helloResult bson.M
	if err := m.targetClient.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&helloResult); err != nil {
		return err
	}

	msg, _ := helloResult["msg"].(string)
	if msg != "isdbgrid" {
		m.monitoredClients = []*mongo.Client{m.targetClient}
		return nil
	}

	logging.PrintInfo("[FlowControl] Sharded Cluster (mongos) detected. Auto-discovering backend shards...", 0)

	// 2. Query config.shards
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

	m.monitoredClients = append(m.monitoredClients, m.targetClient)

	// 3. Connect to each shard
	for _, shard := range shards {
		parts := strings.Split(shard.Host, "/")
		if len(parts) < 2 {
			continue
		}
		rsName := parts[0]
		hosts := parts[1]

		// FIX: Use url.QueryEscape to handle special characters in passwords safely
		safeUser := url.QueryEscape(m.user)
		safePass := url.QueryEscape(m.password)

		// Explicitly enforce SCRAM-SHA-256 to match modern defaults and avoid negotiation errors
		shardURI := fmt.Sprintf("mongodb://%s:%s@%s/?replicaSet=%s&authSource=admin&authMechanism=SCRAM-SHA-256",
			safeUser, safePass, hosts, rsName)

		if config.Cfg.Mongo.TLS {
			shardURI += "&tls=true"
			if config.Cfg.Mongo.TlsAllowInvalidHostnames {
				shardURI += "&tlsAllowInvalidHostnames=true"
			}
			if config.Cfg.Mongo.CaFile != "" {
				shardURI += fmt.Sprintf("&tlsCAFile=%s", config.Cfg.Mongo.CaFile)
			}
		}

		clientOpts := options.Client().ApplyURI(shardURI).SetDirect(false)
		shardClient, err := mongo.Connect(clientOpts)
		if err != nil {
			logging.PrintWarning(fmt.Sprintf("[FlowControl] Failed to connect to shard %s: %v. It will be unmonitored.", shard.ID, err), 0)
			continue
		}

		// Quick Ping
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := shardClient.Ping(pingCtx, nil); err != nil {
			logging.PrintWarning(fmt.Sprintf("[FlowControl] Shard %s unreachable: %v. Skipping.", shard.ID, err), 0)
			shardClient.Disconnect(context.Background())
			pingCancel()
			continue
		}
		pingCancel()

		logging.PrintInfo(fmt.Sprintf("[FlowControl] Added monitor for shard: %s", shard.ID), 0)
		m.monitoredClients = append(m.monitoredClients, shardClient)
	}

	return nil
}

func (m *Manager) monitorLoop() {
	m.checkHealth()
	ticker := time.NewTicker(time.Duration(config.Cfg.FlowControl.CheckIntervalMS) * time.Millisecond)
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
	var maxResidentMB int

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, client := range m.monitoredClients {
		wg.Add(1)
		go func(c *mongo.Client) {
			defer wg.Done()

			var raw bson.Raw
			if err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&raw); err != nil {
				return
			}

			lockVal := raw.Lookup("globalLock", "currentQueue", "total")
			qOps := getIntFromRaw(lockVal)

			memVal := raw.Lookup("mem", "resident")
			resMB := getIntFromRaw(memVal)

			mu.Lock()
			if qOps > maxQueuedOps {
				maxQueuedOps = qOps
			}
			if resMB > maxResidentMB {
				maxResidentMB = resMB
			}
			mu.Unlock()
		}(client)
	}
	wg.Wait()

	shouldPause := false
	reason := ""

	if limit := config.Cfg.FlowControl.TargetMaxQueuedOps; maxQueuedOps > limit {
		shouldPause = true
		reason = fmt.Sprintf("Cluster High Load: Max Queued Ops (%d) > Limit (%d)", maxQueuedOps, limit)
	}

	if !shouldPause && config.Cfg.FlowControl.TargetMaxResidentMB > 0 {
		limit := config.Cfg.FlowControl.TargetMaxResidentMB
		if maxResidentMB > limit {
			shouldPause = true
			reason = fmt.Sprintf("Cluster High Memory: Max Resident (%d MB) > Limit (%d MB)", maxResidentMB, limit)
		}
	}

	if m.statusMgr != nil {
		m.statusMgr.UpdateFlowControl(shouldPause, reason, maxQueuedOps, maxResidentMB)
	}

	if shouldPause {
		if !m.isPaused.Load() {
			logging.PrintWarning(fmt.Sprintf("[FlowControl] THROTTLING PAUSED: %s", reason), 0)
			m.isPaused.Store(true)
		}
		time.Sleep(time.Duration(config.Cfg.FlowControl.PauseDurationMS) * time.Millisecond)
	} else {
		if m.isPaused.Load() {
			logging.PrintInfo("[FlowControl] Throttling released. Cluster healthy.", 0)
			m.isPaused.Store(false)
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
