package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/rs/zerolog"
)

var OpLog zerolog.Logger
var FullLoadLog zerolog.Logger

type ElapsedTime float64

func (e ElapsedTime) MarshalJSON() ([]byte, error) {
	s := fmt.Sprintf("%.9f", e)
	return []byte(s), nil
}

func InitOpLogger() {
	logPath := config.Cfg.Logging.OpsLogPath
	if logPath == "" {
		OpLog = zerolog.Nop()
		return
	}

	dir := filepath.Dir(logPath)
	os.MkdirAll(dir, 0755)

	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		OpLog = zerolog.Nop()
		return
	}

	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"
	OpLog = zerolog.New(file).With().Timestamp().Logger()
}

func InitFullLoadLogger() {
	logPath := config.Cfg.Logging.FullLoadLogPath
	if logPath == "" {
		FullLoadLog = zerolog.Nop()
		return
	}

	dir := filepath.Dir(logPath)
	os.MkdirAll(dir, 0755)

	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		FullLoadLog = zerolog.Nop()
		return
	}

	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"
	FullLoadLog = zerolog.New(file).With().Timestamp().Logger()
}

func LogCDCOp(startTime time.Time, ins, upd, del int64, namespaces []string, err error, workerID int) {
	batchSize := ins + upd + del
	if batchSize == 0 && err == nil {
		return
	}
	elapsed := ElapsedTime(time.Since(startTime).Seconds())

	var event *zerolog.Event
	if err != nil {
		event = OpLog.Error().Err(err)
		event.Str("message", "CDC batch flush failed")
	} else {
		event = OpLog.Info()
		event.Str("message", "CDC batch applied")
	}

	event.Str("s", "cdc").
		Int("worker", workerID).
		Int64("batch_size", batchSize).
		Int64("ins", ins).
		Int64("upd", upd).
		Int64("del", del).
		Interface("elapsed_secs", elapsed).
		Strs("namespaces", namespaces).
		Send()
}

func LogFullLoadBatchOp(startTime time.Time, ns string, docCount int64, byteSize int64, err error, workerID int) {
	elapsed := ElapsedTime(time.Since(startTime).Seconds())

	var event *zerolog.Event
	if err != nil {
		event = FullLoadLog.Error().Err(err)
		event.Str("message", "Full load batch failed")
	} else {
		event = FullLoadLog.Info()
		event.Str("message", "Full load batch applied")
	}

	event.Str("s", "clone_batch").
		Int("worker", workerID).
		Str("ns", ns).
		Int64("doc_count", docCount).
		Int64("byte_size", byteSize).
		Interface("elapsed_secs", elapsed).
		Send()
}

func LogFullLoadOp(startTime time.Time, ns string, docCount int64, err error, workerID int) {
	elapsed := ElapsedTime(time.Since(startTime).Seconds())

	var event *zerolog.Event
	if err != nil {
		event = FullLoadLog.Error().Err(err)
		event.Str("message", "Full load for namespace failed")
	} else {
		event = FullLoadLog.Info()
		event.Str("message", "Full load for namespace completed")
	}

	event.Str("s", "clone").
		Int("worker", workerID).
		Str("ns", ns).
		Int64("doc_count", docCount).
		Interface("elapsed_secs", elapsed).
		Send()
}
