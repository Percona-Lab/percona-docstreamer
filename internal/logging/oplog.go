package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/rs/zerolog"
)

// OpLog is the dedicated structured JSON logger for CDC operations
// Not to be confused with mongodb oplog, this is just our log functionality to log operations :-)
var OpLog zerolog.Logger

// FullLoadLog is the dedicated structured JSON logger for Full Load (cloner) operations
var FullLoadLog zerolog.Logger

// InitOpLogger configures the OpLog to write to the designated file
func InitOpLogger() {
	logPath := config.Cfg.Logging.OpsLogPath
	if logPath == "" {
		// Disable if path is empty
		OpLog = zerolog.Nop()
		return
	}

	// Ensure the directory exists
	dir := filepath.Dir(logPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		PrintWarning(fmt.Sprintf("Failed to create ops log directory: %v", err), 0)
		OpLog = zerolog.Nop()
		return
	}

	// Open the file for appending
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		PrintWarning(fmt.Sprintf("Failed to open ops log file: %v", err), 0)
		OpLog = zerolog.Nop()
		return
	}

	// Configure zerolog for the format we want as the example below
	// Use "2006-01-02 15:04:05.000" for our specific time format
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"

	// Create the logger
	OpLog = zerolog.New(file).With().Timestamp().Logger()
	// PrintInfo(fmt.Sprintf("CDC Operations logger initialized (file: %s)", logPath), 1)
}

// InitFullLoadLogger configures the FullLoadLog to write to its designated file
func InitFullLoadLogger() {
	logPath := config.Cfg.Logging.FullLoadLogPath
	if logPath == "" {
		// Disable if path is empty
		FullLoadLog = zerolog.Nop()
		return
	}

	// Ensure the directory exists
	dir := filepath.Dir(logPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		PrintWarning(fmt.Sprintf("Failed to create full load log directory: %v", err), 0)
		FullLoadLog = zerolog.Nop()
		return
	}

	// Open the file for appending
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		PrintWarning(fmt.Sprintf("Failed to open full load log file: %v", err), 0)
		FullLoadLog = zerolog.Nop()
		return
	}

	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"

	// Create the logger
	FullLoadLog = zerolog.New(file).With().Timestamp().Logger()
	// PrintInfo(fmt.Sprintf("Full Load logger initialized (file: %s)", logPath), 1)
}

// LogCDCOp writes a structured log entry for a CDC batch operation
func LogCDCOp(startTime time.Time, batchSize int64, namespaces []string, err error) {
	// Don't log empty, successful batches
	if batchSize == 0 && err == nil {
		return
	}
	// Calculate elapsed time in seconds, with millisecond precision
	elapsed := time.Since(startTime).Seconds()

	var event *zerolog.Event
	if err != nil {
		event = OpLog.Error().Err(err)
		event.Str("message", "CDC batch flush failed")
	} else {
		event = OpLog.Info()
		event.Str("message", "CDC batch applied")
	}

	event.Str("s", "cdc").
		Int64("batch_size", batchSize).
		Float64("elapsed_secs", elapsed).
		Strs("namespaces", namespaces). // This logs the array of namespaces in this batch
		Send()
}

// LogFullLoadBatchOp writes a structured log entry for a *single batch*
// of the Full Load (cloner) operation.
func LogFullLoadBatchOp(startTime time.Time, ns string, docCount int64, byteSize int64, err error) {
	// Calculate elapsed time in seconds, with millisecond precision
	elapsed := time.Since(startTime).Seconds()

	var event *zerolog.Event
	if err != nil {
		event = FullLoadLog.Error().Err(err)
		event.Str("message", "Full load batch failed")
	} else {
		event = FullLoadLog.Info()
		event.Str("message", "Full load batch applied")
	}

	event.Str("s", "clone_batch"). // Different 's' value to distinguish (clone_batch)
					Str("ns", ns).
					Int64("doc_count", docCount).
					Int64("byte_size", byteSize).
					Float64("elapsed_secs", elapsed).
					Send()
}

// LogFullLoadOp writes a structured log entry for a completed Full Load (cloner) operation
func LogFullLoadOp(startTime time.Time, ns string, docCount int64, err error) {
	// Calculate elapsed time in seconds, with millisecond precision
	elapsed := time.Since(startTime).Seconds()

	var event *zerolog.Event
	if err != nil {
		event = FullLoadLog.Error().Err(err)
		event.Str("message", "Full load for namespace failed")
	} else {
		event = FullLoadLog.Info()
		event.Str("message", "Full load for namespace completed")
	}

	event.Str("s", "clone").
		Str("ns", ns).
		Int64("doc_count", docCount).
		Float64("elapsed_secs", elapsed).
		Send()
}
