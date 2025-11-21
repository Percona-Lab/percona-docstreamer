package config

import (
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

// CDCConfig holds tuning parameters for Change Data Capture
type CDCConfig struct {
	BatchSize       int `mapstructure:"batch_size"`
	BatchIntervalMS int `mapstructure:"batch_interval_ms"`
	MaxAwaitTimeMS  int `mapstructure:"max_await_time_ms"`
	MaxWriteWorkers int `mapstructure:"max_write_workers"`
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
}

// MongoConfig holds target-specific settings
type MongoConfig struct {
	Endpoint                 string `mapstructure:"endpoint"`
	Port                     string `mapstructure:"port"`
	CaFile                   string `mapstructure:"ca_file"`
	ExtraParams              string `mapstructure:"extra_params"`
	TlsAllowInvalidHostnames bool   `mapstructure:"tls_allow_invalid_hostnames"`
}

// MigrationConfig holds general migration settings
type MigrationConfig struct {
	NetworkCompressors   string   `mapstructure:"network_compressors"`
	ExcludeDBs           []string `mapstructure:"exclude_dbs"`
	ExcludeCollections   []string `mapstructure:"exclude_collections"`
	MetadataDB           string   `mapstructure:"metadata_db"`
	CheckpointCollection string   `mapstructure:"checkpoint_collection"`
	CheckpointDocID      string   `mapstructure:"checkpoint_doc_id"`
	StatusCollection     string   `mapstructure:"status_collection"`
	StatusDocID          string   `mapstructure:"status_doc_id"`
	PIDFilePath          string   `mapstructure:"pid_file_path"`
	MaxConcurrentWorkers int      `mapstructure:"max_concurrent_workers"`
	Destroy              bool     `mapstructure:"destroy"`
	EnvPrefix            string   `mapstructure:"env_prefix"`
	StatusHTTPPort       string   `mapstructure:"status_http_port"`
	DryRun               bool     `mapstructure:"dry_run"`
}

// ClonerConfig holds full-load specific settings
type ClonerConfig struct {
	NumReadWorkers   int   `mapstructure:"num_read_workers"`
	SegmentSizeDocs  int64 `mapstructure:"segment_size_docs"`
	NumInsertWorkers int   `mapstructure:"num_insert_workers"`
	InsertBatchSize  int   `mapstructure:"insert_batch_size"`
	ReadBatchSize    int   `mapstructure:"read_batch_size"`
	InsertBatchBytes int64 `mapstructure:"insert_batch_bytes"`
}

// Config holds all configuration for the application
type Config struct {
	Logging   LoggingConfig   `mapstructure:"logging"`
	DocDB     DocDBConfig     `mapstructure:"docdb"`
	Mongo     MongoConfig     `mapstructure:"mongo"`
	Migration MigrationConfig `mapstructure:"migration"`
	Cloner    ClonerConfig    `mapstructure:"cloner"`
	CDC       CDCConfig       `mapstructure:"cdc"`
}

// Cfg is the global config object
var Cfg *Config

// LoadConfig loads configuration from file, env vars, and sets defaults.
func LoadConfig() {
	// --- 1. Set Defaults ---
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.file_path", "logs/docMongoStream.log")
	viper.SetDefault("logging.ops_log_path", "logs/cdc.log")
	viper.SetDefault("logging.full_load_log_path", "logs/full_load.log")

	viper.SetDefault("docdb.endpoint", "localhost")
	viper.SetDefault("docdb.port", "27017")
	viper.SetDefault("docdb.ca_file", "global-bundle.pem")
	viper.SetDefault("docdb.extra_params", "")
	viper.SetDefault("docdb.tls_allow_invalid_hostnames", false) // Default to secure

	viper.SetDefault("mongo.endpoint", "localhost")
	viper.SetDefault("mongo.port", "27017")
	viper.SetDefault("mongo.ca_file", "")
	viper.SetDefault("mongo.extra_params", "")
	viper.SetDefault("mongo.tls_allow_invalid_hostnames", false) // Default to secure

	viper.SetDefault("migration.network_compressors", "zlib,snappy")
	viper.SetDefault("migration.exclude_dbs", []string{"admin", "local", "config"})
	viper.SetDefault("migration.exclude_collections", []string{})
	viper.SetDefault("migration.max_concurrent_workers", 4)
	viper.SetDefault("migration.metadata_db", "docMongoStream")
	viper.SetDefault("migration.checkpoint_collection", "checkpoints")
	viper.SetDefault("migration.checkpoint_doc_id", "cdc_resume_timestamp")
	viper.SetDefault("migration.status_collection", "status")
	viper.SetDefault("migration.status_doc_id", "migration_status")
	viper.SetDefault("migration.pid_file_path", "./docMongoStream.pid")
	viper.SetDefault("migration.destroy", false)
	viper.SetDefault("migration.dry_run", false)
	viper.SetDefault("migration.env_prefix", "MIGRATION")
	viper.SetDefault("migration.status_http_port", "8080")

	viper.SetDefault("cloner.num_read_workers", 4)
	viper.SetDefault("cloner.num_insert_workers", 8)
	viper.SetDefault("cloner.read_batch_size", 1000)
	viper.SetDefault("cloner.insert_batch_size", 1000)
	viper.SetDefault("cloner.insert_batch_bytes", 16*1024*1024) // 16MB
	viper.SetDefault("cloner.segment_size_docs", 10000)

	viper.SetDefault("cdc.batch_size", 1000)
	viper.SetDefault("cdc.batch_interval_ms", 500)
	viper.SetDefault("cdc.max_await_time_ms", 1000)
	viper.SetDefault("cdc.max_write_workers", 4) // <--- DEFAULT (Use a sensible default like 4 or 8)

	// --- 2. Read config file , we will search in any of the path's below---
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("/etc/docMongoStream")
	viper.AddConfigPath("$HOME/.docMongoStream")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Println("Config file not found, using defaults and environment variables.")
		} else {
			log.Printf("Warning: cannot read config file: %v\n", err)
		}
	}

	// --- 3. Read Environment Variables ---
	// --- Load prefix from config file FIRST ---
	prefix := viper.GetString("migration.env_prefix")
	if prefix == "" {
		prefix = "MIGRATION" // Fallback just in case
	}
	viper.SetEnvPrefix(prefix) // e.g., MIGRATION_DOCDB_USER
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// --- 4. Unmarshal all settings into our Cfg struct ---
	if err := viper.Unmarshal(&Cfg); err != nil {
		log.Fatalf("Fatal error unmarshaling config: %v\n", err)
	}

	// --- 5. Watch the config file for live changes ---
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

// --- helper function to build query params ---
func addQueryParam(params *url.Values, key, value string) {
	if value != "" && value != "none" {
		params.Add(key, value)
	}
}

// --- helper to de-duplicate tlsAllowInvalidHostnames ---
func buildTLSParams(extraParams string, allowInvalid bool) string {
	params, _ := url.ParseQuery(extraParams)
	if allowInvalid && params.Get("tlsAllowInvalidHostnames") == "" {
		params.Set("tlsAllowInvalidHostnames", "true")
	}
	return params.Encode()
}

// BuildDocDBURI
func (c *Config) BuildDocDBURI(user, password string) string {
	useTunnel := (c.DocDB.Endpoint == "localhost" || c.DocDB.Endpoint == "127.0.0.1")
	params := url.Values{}
	addQueryParam(&params, "tls", "true")
	addQueryParam(&params, "tlsCAFile", c.DocDB.CaFile)

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

// BuildMongoURI
func (c *Config) BuildMongoURI(user, password string) string {
	params := url.Values{}
	if c.Mongo.CaFile != "" {
		addQueryParam(&params, "tls", "true")
		addQueryParam(&params, "tlsCAFile", c.Mongo.CaFile)
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
