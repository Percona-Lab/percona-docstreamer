package config

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

// ShardRule defines the sharding configuration for a specific collection
type ShardRule struct {
	Namespace        string `mapstructure:"namespace"`          // e.g. "db.collection"
	ShardKey         string `mapstructure:"shard_key"`          // e.g. "field_1:1, field_2:hashed"
	NumInitialChunks int    `mapstructure:"num_initial_chunks"` // Optional: Set specific number of chunks (Hashed only)
	DisablePreSplit  bool   `mapstructure:"disable_presplit"`   // Optional: Skip source scanning (Fast start, but 1 chunk for Range)

	// Options: "none" (default), "scan", "composite_uuid_oid"
	PreSplitStrategy string `mapstructure:"pre_split_strategy"`

	// Explicitly define fields for the composite_uuid_oid strategy.
	// We do NOT guess. You must specify which field is which.
	UUIDField string `mapstructure:"uuid_field"` // e.g. "uuid_type"
	OIDField  string `mapstructure:"oid_field"`  // e.g. "objectid_type"
}

// CDCConfig holds tuning parameters for Change Data Capture
type CDCConfig struct {
	BatchSize       int `mapstructure:"batch_size"`
	BatchIntervalMS int `mapstructure:"batch_interval_ms"`
	MaxAwaitTimeMS  int `mapstructure:"max_await_time_ms"`
	MaxWriteWorkers int `mapstructure:"max_write_workers"`
	NumRetries      int `mapstructure:"num_retries"`
	RetryIntervalMS int `mapstructure:"retry_interval_ms"`
	WriteTimeoutMS  int `mapstructure:"write_timeout_ms"`
}

// LoggingConfig holds logging settings
type LoggingConfig struct {
	Level           string `mapstructure:"level"`
	FilePath        string `mapstructure:"file_path"`
	OpsLogPath      string `mapstructure:"ops_log_path"`
	FullLoadLogPath string `mapstructure:"full_load_log_path"`
}

// DocDBConfig holds source-specific settings
type DocDBConfig struct {
	Endpoint                 string `mapstructure:"endpoint"`
	Port                     string `mapstructure:"port"`
	CaFile                   string `mapstructure:"ca_file"`
	ExtraParams              string `mapstructure:"extra_params"`
	TlsAllowInvalidHostnames bool   `mapstructure:"tls_allow_invalid_hostnames"`
	TLS                      bool   `mapstructure:"tls"`
}

// MongoConfig holds target-specific settings
type MongoConfig struct {
	Endpoint                 string `mapstructure:"endpoint"`
	Port                     string `mapstructure:"port"`
	CaFile                   string `mapstructure:"ca_file"`
	ExtraParams              string `mapstructure:"extra_params"`
	TlsAllowInvalidHostnames bool   `mapstructure:"tls_allow_invalid_hostnames"`
	TLS                      bool   `mapstructure:"tls"`
}

// MigrationConfig holds general migration settings
type MigrationConfig struct {
	NetworkCompressors           string   `mapstructure:"network_compressors"`
	ExcludeDBs                   []string `mapstructure:"exclude_dbs"`
	ExcludeCollections           []string `mapstructure:"exclude_collections"`
	MetadataDB                   string   `mapstructure:"metadata_db"`
	CheckpointCollection         string   `mapstructure:"checkpoint_collection"`
	CheckpointDocID              string   `mapstructure:"checkpoint_doc_id"`
	StatusCollection             string   `mapstructure:"status_collection"`
	StatusDocID                  string   `mapstructure:"status_doc_id"`
	ValidationStatsCollection    string   `mapstructure:"validation_stats_collection"`
	ValidationFailuresCollection string   `mapstructure:"validation_failures_collection"`
	PIDFilePath                  string   `mapstructure:"pid_file_path"`
	MaxConcurrentWorkers         int      `mapstructure:"max_concurrent_workers"`
	Destroy                      bool     `mapstructure:"destroy"`
	EnvPrefix                    string   `mapstructure:"env_prefix"`
	StatusHTTPPort               string   `mapstructure:"status_http_port"`
	DryRun                       bool     `mapstructure:"dry_run"`
}

// ClonerConfig holds full-load specific settings
type ClonerConfig struct {
	NumReadWorkers   int   `mapstructure:"num_read_workers"`
	SegmentSizeDocs  int64 `mapstructure:"segment_size_docs"`
	NumInsertWorkers int   `mapstructure:"num_insert_workers"`
	InsertBatchSize  int   `mapstructure:"insert_batch_size"`
	ReadBatchSize    int   `mapstructure:"read_batch_size"`
	InsertBatchBytes int64 `mapstructure:"insert_batch_bytes"`
	NumRetries       int   `mapstructure:"num_retries"`
	RetryIntervalMS  int   `mapstructure:"retry_interval_ms"`
	WriteTimeoutMS   int   `mapstructure:"write_timeout_ms"`
}

// ValidationConfig holds settings for online data validation
type ValidationConfig struct {
	Enabled              bool `mapstructure:"enabled"`
	BatchSize            int  `mapstructure:"batch_size"`
	RetryIntervalMS      int  `mapstructure:"retry_interval_ms"`
	MaxValidationWorkers int  `mapstructure:"max_validation_workers"`
	MaxRetries           int  `mapstructure:"max_retries"`
	QueueSize            int  `mapstructure:"queue_size"`
}

// FlowControlConfig holds settings for adaptive throttling
type FlowControlConfig struct {
	Enabled             bool `mapstructure:"enabled"`
	CheckIntervalMS     int  `mapstructure:"check_interval_ms"`
	TargetMaxQueuedOps  int  `mapstructure:"target_max_queued_ops"`
	TargetMaxResidentMB int  `mapstructure:"target_max_resident_mb"`
	PauseDurationMS     int  `mapstructure:"pause_duration_ms"`
}

// Config holds all configuration for the application
type Config struct {
	Logging     LoggingConfig     `mapstructure:"logging"`
	DocDB       DocDBConfig       `mapstructure:"docdb"`
	Mongo       MongoConfig       `mapstructure:"mongo"`
	Migration   MigrationConfig   `mapstructure:"migration"`
	Cloner      ClonerConfig      `mapstructure:"cloner"`
	CDC         CDCConfig         `mapstructure:"cdc"`
	Validation  ValidationConfig  `mapstructure:"validation"`
	FlowControl FlowControlConfig `mapstructure:"flow_control"`
	Sharding    []ShardRule       `mapstructure:"sharding"`
}

// Cfg is the global config object
var Cfg *Config

// LoadConfig loads configuration from file, env vars, and sets defaults.
func LoadConfig() {
	// --- 1. Set Defaults ---
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.file_path", "logs/docStreamer.log")
	viper.SetDefault("logging.ops_log_path", "logs/cdc.log")
	viper.SetDefault("logging.full_load_log_path", "logs/full_load.log")

	viper.SetDefault("docdb.endpoint", "localhost")
	viper.SetDefault("docdb.port", "27017")
	viper.SetDefault("docdb.ca_file", "global-bundle.pem")
	viper.SetDefault("docdb.extra_params", "")
	viper.SetDefault("docdb.tls_allow_invalid_hostnames", false)
	viper.SetDefault("docdb.tls", false)

	viper.SetDefault("mongo.endpoint", "localhost")
	viper.SetDefault("mongo.port", "27017")
	viper.SetDefault("mongo.ca_file", "")
	viper.SetDefault("mongo.extra_params", "")
	viper.SetDefault("mongo.tls_allow_invalid_hostnames", false)
	viper.SetDefault("mongo.tls", false)

	viper.SetDefault("migration.network_compressors", "zlib,snappy")
	viper.SetDefault("migration.exclude_dbs", []string{"admin", "local", "config"})
	viper.SetDefault("migration.exclude_collections", []string{})
	viper.SetDefault("migration.max_concurrent_workers", 4)
	viper.SetDefault("migration.metadata_db", "docStreamer")
	viper.SetDefault("migration.checkpoint_collection", "checkpoints")
	viper.SetDefault("migration.checkpoint_doc_id", "cdc_resume_timestamp")
	viper.SetDefault("migration.status_collection", "status")
	viper.SetDefault("migration.status_doc_id", "migration_status")
	viper.SetDefault("migration.validation_stats_collection", "validation_stats")
	viper.SetDefault("migration.validation_failures_collection", "validation_failures")
	viper.SetDefault("migration.pid_file_path", "./docStreamer.pid")
	viper.SetDefault("migration.destroy", false)
	viper.SetDefault("migration.dry_run", false)
	viper.SetDefault("migration.env_prefix", "MIGRATION")
	viper.SetDefault("migration.status_http_port", "8080")

	// Cloner Defaults
	viper.SetDefault("cloner.num_read_workers", 4)
	viper.SetDefault("cloner.num_insert_workers", 8)
	viper.SetDefault("cloner.read_batch_size", 1000)
	viper.SetDefault("cloner.insert_batch_size", 1000)
	viper.SetDefault("cloner.insert_batch_bytes", 48*1024*1024)
	viper.SetDefault("cloner.segment_size_docs", 10000)
	viper.SetDefault("cloner.num_retries", 5)
	viper.SetDefault("cloner.retry_interval_ms", 1000)
	viper.SetDefault("cloner.write_timeout_ms", 30000)

	// CDC Defaults
	viper.SetDefault("cdc.batch_size", 1000)
	viper.SetDefault("cdc.batch_interval_ms", 500)
	viper.SetDefault("cdc.max_await_time_ms", 1000)
	viper.SetDefault("cdc.max_write_workers", 4)
	viper.SetDefault("cdc.num_retries", 10)
	viper.SetDefault("cdc.retry_interval_ms", 1000)
	viper.SetDefault("cdc.write_timeout_ms", 30000)

	// Validation Defaults
	viper.SetDefault("validation.enabled", true)
	viper.SetDefault("validation.batch_size", 100)
	viper.SetDefault("validation.retry_interval_ms", 500)
	viper.SetDefault("validation.max_validation_workers", 4)
	viper.SetDefault("validation.max_retries", 3)
	viper.SetDefault("validation.queue_size", 2000)

	// Flow control Defaults
	viper.SetDefault("flow_control.enabled", true)
	viper.SetDefault("flow_control.check_interval_ms", 1000)
	viper.SetDefault("flow_control.target_max_queued_ops", 50) // Conservative default
	viper.SetDefault("flow_control.target_max_resident_mb", 0) // 0 = disabled
	viper.SetDefault("flow_control.pause_duration_ms", 500)

	// --- 2. Read config file ---
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("/etc/docStreamer")
	viper.AddConfigPath("$HOME/.docStreamer")

	ex, err := os.Executable()
	if err == nil {
		exePath := filepath.Dir(ex)
		viper.AddConfigPath(exePath)
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Println("Config file not found, using defaults and environment variables.")
		} else {
			log.Printf("Warning: cannot read config file: %v\n", err)
		}
	}

	// --- 3. Read Environment Variables ---
	prefix := viper.GetString("migration.env_prefix")
	if prefix == "" {
		prefix = "MIGRATION"
	}
	viper.SetEnvPrefix(prefix)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// --- 4. Unmarshal ---
	if err := viper.Unmarshal(&Cfg); err != nil {
		log.Fatalf("Fatal error unmarshaling config: %v\n", err)
	}

	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		log.Printf("Config file changed: %s. Reloading...", e.Name)
		if err := viper.Unmarshal(&Cfg); err != nil {
			log.Printf("Error reloading config: %v\n", err)
		} else {
			log.Println("Config successfully reloaded.")
		}
	})
}

func addQueryParam(params *url.Values, key, value string) {
	if value != "" && value != "none" {
		params.Add(key, value)
	}
}

func buildTLSParams(extraParams string, allowInvalid bool) string {
	params, _ := url.ParseQuery(extraParams)
	if allowInvalid && params.Get("tlsAllowInvalidHostnames") == "" {
		params.Set("tlsAllowInvalidHostnames", "true")
	}
	return params.Encode()
}

func (c *Config) BuildDocDBURI(user, password string) string {
	useTunnel := (c.DocDB.Endpoint == "localhost" || c.DocDB.Endpoint == "127.0.0.1")
	params := url.Values{}
	if c.DocDB.TLS {
		addQueryParam(&params, "tls", "true")
		addQueryParam(&params, "tlsCAFile", c.DocDB.CaFile)
	} else {
		addQueryParam(&params, "tls", "false")
		// We do not add tlsCAFile if tls is false
	}

	if useTunnel {
		addQueryParam(&params, "directConnection", "true")
	} else {
		addQueryParam(&params, "replicaSet", "rs0")
	}
	addQueryParam(&params, "compressors", c.Migration.NetworkCompressors)

	finalParamsStr := buildTLSParams(c.DocDB.ExtraParams, c.DocDB.TlsAllowInvalidHostnames)
	if finalParamsStr != "" {
		paramsStr := params.Encode()
		if paramsStr != "" {
			return fmt.Sprintf("mongodb://%s:%s@%s:%s/?%s&%s",
				url.QueryEscape(user), url.QueryEscape(password),
				c.DocDB.Endpoint, c.DocDB.Port, paramsStr, finalParamsStr)
		}
		return fmt.Sprintf("mongodb://%s:%s@%s:%s/?%s",
			url.QueryEscape(user), url.QueryEscape(password),
			c.DocDB.Endpoint, c.DocDB.Port, finalParamsStr)
	}

	return fmt.Sprintf("mongodb://%s:%s@%s:%s/?%s",
		url.QueryEscape(user), url.QueryEscape(password),
		c.DocDB.Endpoint, c.DocDB.Port, params.Encode())
}

func (c *Config) BuildMongoURI(user, password string) string {
	params := url.Values{}
	if c.Mongo.TLS {
		addQueryParam(&params, "tls", "true")
		if c.Mongo.CaFile != "" {
			addQueryParam(&params, "tlsCAFile", c.Mongo.CaFile)
		}
	} else {
		addQueryParam(&params, "tls", "false")
	}

	addQueryParam(&params, "compressors", c.Migration.NetworkCompressors)

	finalParamsStr := buildTLSParams(c.Mongo.ExtraParams, c.Mongo.TlsAllowInvalidHostnames)
	if finalParamsStr != "" {
		paramsStr := params.Encode()
		if paramsStr != "" {
			return fmt.Sprintf("mongodb://%s:%s@%s:%s/?%s&%s",
				url.QueryEscape(user), url.QueryEscape(password),
				c.Mongo.Endpoint, c.Mongo.Port, paramsStr, finalParamsStr)
		}
		return fmt.Sprintf("mongodb://%s:%s@%s:%s/?%s",
			url.QueryEscape(user), url.QueryEscape(password),
			c.Mongo.Endpoint, c.Mongo.Port, finalParamsStr)
	}

	return fmt.Sprintf("mongodb://%s:%s@%s:%s/?%s",
		url.QueryEscape(user), url.QueryEscape(password),
		c.Mongo.Endpoint, c.Mongo.Port, params.Encode())
}
