//go:build !linux

package config

func ResolveGoMemLimit() string {
	// Safe default for non-linux development environments
	return "4294967296" // 4GiB in bytes
}
