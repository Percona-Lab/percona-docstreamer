package logging

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/Percona-Lab/percona-docstreamer/internal/config"
)

const (
	Reset  = "\033[0m"
	Bold   = "\033[1m"
	Cyan   = "\033[36m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Red    = "\033[31m"
	Gray   = "\033[90m"
)

var logFile *os.File

const LOGO = "\n" +
	"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
	"_________           ____________                                           \n" +
	"______  /_____________  ___/_  /__________________ _______ ________________\n" +
	"_  __  /_  __ \\  ___/____ \\_  __/_  ___/  _ \\  __ `/_  __ `__ \\  _ \\_  ___/\n" +
	"/ /_/ / / /_/ / /__ ____/ // /_ _  /   /  __/ /_/ /_  / / / / /  __/  /    \n" +
	"\\__,_/  \\____/\\___/ /____/ \\__/ /_/    \\___/\\__,_/ /_/ /_/ /_/\\___//_/     \n" +
	"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~                                                                                              \n"

func Init() {
	logFilePath := config.Cfg.Logging.FilePath
	logDir := filepath.Dir(logFilePath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("Failed to create log directory %s: %v", logDir, err)
	}

	var err error
	logFile, err = os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file %s: %v", logFilePath, err)
	}

	var logWriter io.Writer
	stat, _ := os.Stdout.Stat()
	if (stat.Mode() & os.ModeCharDevice) != 0 {
		logWriter = io.MultiWriter(os.Stdout, logFile)
	} else {
		logWriter = logFile
	}

	log.SetOutput(logWriter)
	log.SetFlags(log.Ldate | log.Ltime)
}

func PrintHeader(title string) {
	log.Printf("%s %s %s\n", Cyan, LOGO, Reset)
}

func PrintPhase(phaseNum, title string) {
	log.Printf("\n%s--- Phase %s: %s ---%s", Bold, phaseNum, strings.ToUpper(title), Reset)
}

func PrintStep(message string, indent int) {
	log.Printf("%s[TASK]%s %s", Gray, Reset, message)
}

func PrintSuccess(message string, indent int) {
	log.Printf("%s[OK]  %s %s%s", Green, Reset, message, Reset)
}

func PrintWarning(message string, indent int) {
	log.Printf("%s[WARN]%s %s%s", Yellow, Reset, message, Reset)
}

func PrintError(message string, indent int) {
	log.Printf("%s[ERR] %s %s%s", Red, Reset, message, Reset)
}

func PrintInfo(message string, indent int) {
	log.Printf("%s[INFO]%s %s%s", Cyan, Reset, message, Reset)
}

func PrintFallback(message string, indent int) {
	log.Printf("%s[FALLBACK]%s %s%s", Cyan, Reset, message, Reset)
}
