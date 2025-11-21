package logging

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/Percona-Lab/docMongoStream/internal/config"
)

// ANSI escape codes for colors
const (
	Reset  = "\033[0m"
	Bold   = "\033[1m"
	Cyan   = "\033[36m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Red    = "\033[31m"
	Gray   = "\033[90m"
)

// Simple in-memory file handle
var logFile *os.File

const LOGO = "\n" +
	"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
	"________            ______  ___                                ____________                               \n" +
	"___  __ \\______________   |/  /___________________ ______      __  ___/_  /__________________ _______ ___ \n" +
	"__  / / /  __ \\  ___/_  /|_/ /_  __ \\_  __ \\_  __ `/  __ \\     _____ \\_  __/_  ___/  _ \\  __ `/_  __ `__ \\\n" +
	"_  /_/ // /_/ / /__ _  /  / / / /_/ /  / / /  /_/ // /_/ /     ____/ // /_ _  /   /  __/ /_/ /_  / / / / /\n" +
	"/_____/ \\____/\\___/ /_/  /_/  \\____//_/ /_/\\__, / \\____/      /____/ \\__/ /_/    \\___/\\__,_/ /_/ /_/ /_/ \n" +
	"                                           /____/      \n" +
	"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~                                                                                              \n"

// Init sets up the global logger to write to both console and file
func Init() {
	logFilePath := config.Cfg.Logging.FilePath

	// Ensure the directory for the log file exists
	logDir := filepath.Dir(logFilePath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("Failed to create log directory %s: %v", logDir, err)
	}

	// Open the log file for appending
	var err error
	logFile, err = os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file %s: %v", logFilePath, err)
	}

	var logWriter io.Writer

	// Check if stdout is a TTY (i.e., an interactive terminal)
	stat, _ := os.Stdout.Stat()
	if (stat.Mode() & os.ModeCharDevice) != 0 {
		// We are in an interactive terminal (e.g., 'start' cmd), write to both
		logWriter = io.MultiWriter(os.Stdout, logFile)
	} else {
		// We are in the background ('run' cmd), write *only* to the file
		logWriter = logFile
	}

	// Configure the standard log package
	log.SetOutput(logWriter)
	log.SetFlags(log.Ldate | log.Ltime) // Add timestamps to log entries
}

// PrintHeader prints the logo
func PrintHeader(title string) {
	log.Printf("%s %s %s\n", Cyan, LOGO, Reset)
}

// PrintPhase prints a new phase header
func PrintPhase(phaseNum, title string) {
	log.Printf("\n%s--- Phase %s: %s ---%s", Bold, phaseNum, strings.ToUpper(title), Reset)
}

// PrintStep prints a non-indented step message
func PrintStep(message string, indent int) {
	log.Printf("%s[TASK]%s %s", Gray, Reset, message)
}

// PrintSuccess prints a non-indented success message
func PrintSuccess(message string, indent int) {
	log.Printf("%s[OK]  %s %s%s", Green, Reset, message, Reset)
}

// PrintWarning prints a non-indented warning message
func PrintWarning(message string, indent int) {
	log.Printf("%s[WARN]%s %s%s", Yellow, Reset, message, Reset)
}

// PrintError prints a non-indented error message
func PrintError(message string, indent int) {
	log.Printf("%s[ERR] %s %s%s", Red, Reset, message, Reset)
}

// PrintInfo prints a non-indented info message
func PrintInfo(message string, indent int) {
	log.Printf("%s[INFO]%s %s%s", Cyan, Reset, message, Reset)
}
