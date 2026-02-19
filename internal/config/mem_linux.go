//go:build linux

package config

import (
	"strconv"
	"strings"
	"syscall"
)

func GetSystemTotalMemory() uint64 {
	var sysInfo syscall.Sysinfo_t
	if err := syscall.Sysinfo(&sysInfo); err == nil {
		return uint64(sysInfo.Totalram) * uint64(sysInfo.Unit)
	}
	return 4 * 1024 * 1024 * 1024
}

func ResolveGoMemLimit() string {
	input := Cfg.Migration.GoMemLimit
	if input == "" || !strings.HasSuffix(input, "%") {
		input = "80%" // Default from config or fallback
	}

	percentStr := strings.TrimSuffix(input, "%")
	percent, _ := strconv.ParseFloat(percentStr, 64)

	totalBytes := GetSystemTotalMemory()
	limitBytes := uint64(float64(totalBytes) * (percent / 100.0))

	return strconv.FormatUint(limitBytes, 10)
}
