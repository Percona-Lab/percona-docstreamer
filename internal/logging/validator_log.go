package logging

import (
	"log"
	"os"
	"path/filepath"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
)

var valLogger *log.Logger
var valLogFile *os.File

// InitValidatorLog initializes the dedicated validator log file.
// Call this right after logging.Init() in your main.go
func InitValidatorLog() {
	logFilePath := config.Cfg.Logging.ValidatorLogPath
	if logFilePath == "" {
		logFilePath = "log/validator.log"
	}

	logDir := filepath.Dir(logFilePath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("Failed to create validator log directory %s: %v", logDir, err)
	}

	var err error
	valLogFile, err = os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open validator log file %s: %v", logFilePath, err)
	}

	// Writes strictly to the file, not to standard output
	valLogger = log.New(valLogFile, "", log.Ldate|log.Ltime)
}

func LogValidatorInfo(message string) {
	if valLogger != nil {
		valLogger.Printf("[INFO] %s\n", message)
	}
}

func LogValidatorWarning(message string) {
	if valLogger != nil {
		valLogger.Printf("[WARN] %s\n", message)
	}
}

func LogValidatorError(message string) {
	if valLogger != nil {
		valLogger.Printf("[ERR] %s\n", message)
	}
}

func LogValidatorSuccess(message string) {
	if valLogger != nil {
		valLogger.Printf("[OK] %s\n", message)
	}
}
