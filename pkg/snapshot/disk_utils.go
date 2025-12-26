package snapshot

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"
)

// DiskInfo contains disk space information for a given path
type DiskInfo struct {
	TotalBytes    uint64 // Total disk space
	FreeBytes     uint64 // Available disk space
	UsedBytes     uint64 // Used disk space
	SnapshotBytes uint64 // Space used by snapshot files specifically
}

// GetDiskInfo returns disk space information for the snapshot directory
func GetDiskInfo(snapshotPath string) (DiskInfo, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(snapshotPath, &stat); err != nil {
		return DiskInfo{}, fmt.Errorf("failed to get disk stats: %w", err)
	}

	info := DiskInfo{
		TotalBytes: stat.Blocks * uint64(stat.Bsize),
		FreeBytes:  stat.Bavail * uint64(stat.Bsize),
	}
	info.UsedBytes = info.TotalBytes - info.FreeBytes

	// Calculate space used by snapshots
	info.SnapshotBytes = calculateSnapshotSize(snapshotPath)

	return info, nil
}

// calculateSnapshotSize sums up sizes of all snapshot files in the directory
func calculateSnapshotSize(snapshotPath string) uint64 {
	var total uint64
	patterns := []string{
		"snapshot-*.tar.*",
		"incremental-snapshot-*.tar.*",
		"genesis.tar.bz2",
	}

	for _, pattern := range patterns {
		matches, err := filepath.Glob(filepath.Join(snapshotPath, pattern))
		if err != nil {
			continue
		}
		for _, match := range matches {
			if info, err := os.Stat(match); err == nil {
				total += uint64(info.Size())
			}
		}
	}

	// Also check remote/ subdirectory
	remoteDir := filepath.Join(snapshotPath, "remote")
	for _, pattern := range patterns[:2] { // Skip genesis for remote
		matches, err := filepath.Glob(filepath.Join(remoteDir, pattern))
		if err != nil {
			continue
		}
		for _, match := range matches {
			if info, err := os.Stat(match); err == nil {
				total += uint64(info.Size())
			}
		}
	}

	return total
}

// FormatBytes formats bytes as human-readable string (e.g., "95.23 GB")
func FormatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
		TB = 1024 * GB
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/TB)
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// LogDiskSpace logs current disk space status to stdout
func LogDiskSpace(snapshotPath string) {
	info, err := GetDiskInfo(snapshotPath)
	if err != nil {
		log.Printf("Warning: Could not get disk info: %v", err)
		return
	}

	log.Printf("Disk space: Total=%s, Free=%s, Used by snapshots=%s",
		FormatBytes(info.TotalBytes),
		FormatBytes(info.FreeBytes),
		FormatBytes(info.SnapshotBytes))

	// Calculate how much space is effectively available for new snapshots
	// This is free space, not accounting for space that would be freed by cleanup
	log.Printf("Available for new snapshots: %s", FormatBytes(info.FreeBytes))
}

// CheckDiskSpaceForDownload checks if there's enough space for a download
// Returns true if enough space, false otherwise
func CheckDiskSpaceForDownload(snapshotPath string, requiredBytes uint64) (bool, error) {
	info, err := GetDiskInfo(snapshotPath)
	if err != nil {
		return false, err
	}

	// Add 10% buffer for safety
	requiredWithBuffer := uint64(float64(requiredBytes) * 1.1)

	if info.FreeBytes < requiredWithBuffer {
		log.Printf("Warning: Insufficient disk space. Need %s, have %s free",
			FormatBytes(requiredWithBuffer), FormatBytes(info.FreeBytes))
		return false, nil
	}

	return true, nil
}
