package pid

import (
	"fmt"
	"os"
	"strconv"
	"syscall"

	"github.com/Percona-Lab/docMongoStream/internal/config"
	"github.com/Percona-Lab/docMongoStream/internal/logging"
)

// Write the current PID to the config.Cfg.Migration.PIDFilePath
func Write() error {
	pid := os.Getpid()
	pidStr := strconv.Itoa(pid)
	logging.PrintInfo(fmt.Sprintf("Writing PID %d to %s", pid, config.Cfg.Migration.PIDFilePath), 1)
	return os.WriteFile(config.Cfg.Migration.PIDFilePath, []byte(pidStr), 0644)
}

// Read the PID from the config.Cfg.Migration.PIDFilePath
func Read() (int, error) {
	data, err := os.ReadFile(config.Cfg.Migration.PIDFilePath)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, fmt.Errorf("invalid PID in %s: %w", config.Cfg.Migration.PIDFilePath, err)
	}
	return pid, nil
}

// Remove PID from the config.Cfg.Migration.PIDFilePath
func Clear() {
	os.Remove(config.Cfg.Migration.PIDFilePath)
}

// IsRunning checks if a process with the given PID is running
func IsRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false // Not found
	}
	// On Unix, send signal 0 to check if process exists
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

// Stop reads the PID and sends a SIGTERM signal
func Stop() error {
	pid, err := Read()
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("Application is not running (no config.Cfg.Migration.PIDFilePath found)")
		}
		return fmt.Errorf("Failed to read config.Cfg.Migration.PIDFilePath: %w", err)
	}

	if !IsRunning(pid) {
		logging.PrintWarning(fmt.Sprintf("Application not running (PID %d), but config.Cfg.Migration.PIDFilePath exists. Clearing file.", pid), 1)
		Clear()
		return fmt.Errorf("Application not running (PID %d)", pid)
	}

	process, _ := os.FindProcess(pid)
	if err := process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("Failed to send SIGTERM to PID %d: %w", pid, err)
	}

	return nil
}
