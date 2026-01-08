package snapshot

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/config"
)

// SnapshotURLInfo holds information about a remote snapshot URL
type SnapshotURLInfo struct {
	URL          string // Full URL to download from
	Filename     string // Parsed filename (e.g., snapshot-123456-abc.tar.zst)
	Slot         int    // Full snapshot slot (or end slot for incremental)
	BaseSlot     int    // Base slot (only for incremental, 0 for full)
	IsIncremental bool
	ContentLength int64 // Size in bytes (from HEAD request)
}

// GetSnapshotURL probes an RPC endpoint and returns the snapshot URL info without downloading.
// This is useful for mithril integration where you want to stream directly.
// snapshotType should be "full" or "incremental"
func GetSnapshotURL(ctx context.Context, rpcAddress string, snapshotType string) (*SnapshotURLInfo, error) {
	rpcAddr := strings.TrimSuffix(rpcAddress, "/")
	if !strings.HasPrefix(rpcAddr, "http://") && !strings.HasPrefix(rpcAddr, "https://") {
		rpcAddr = "http://" + rpcAddr
	}

	extensions := []string{".tar.zst", ".tar.bz2"}
	var lastErr error

	for _, ext := range extensions {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var snapshotURL string
		if snapshotType == "incremental" {
			snapshotURL = fmt.Sprintf("%s/incremental-snapshot%s", rpcAddr, ext)
		} else {
			snapshotURL = fmt.Sprintf("%s/snapshot%s", rpcAddr, ext)
		}

		info, err := probeSnapshotURL(ctx, snapshotURL, snapshotType == "incremental")
		if err != nil {
			lastErr = err
			continue
		}
		return info, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no snapshot found at %s", rpcAddress)
}

// probeSnapshotURL makes a HEAD request to get snapshot info without downloading
func probeSnapshotURL(ctx context.Context, snapshotURL string, isIncremental bool) (*SnapshotURLInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", snapshotURL, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return nil // Allow redirects
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d for %s", resp.StatusCode, snapshotURL)
	}

	// Get filename from Content-Disposition or final URL
	finalURL := resp.Request.URL.String()
	fileName := filepath.Base(finalURL)
	if contentDisposition := resp.Header.Get("Content-Disposition"); contentDisposition != "" {
		parts := strings.Split(contentDisposition, "filename=")
		if len(parts) > 1 {
			fileName = strings.Trim(parts[1], `"' `)
		}
	}

	info := &SnapshotURLInfo{
		URL:           finalURL,
		Filename:      fileName,
		ContentLength: resp.ContentLength,
		IsIncremental: isIncremental,
	}

	// Parse slot info from filename
	if isIncremental {
		baseSlot, endSlot, err := ExtractIncrementalSnapshotSlots(fileName)
		if err != nil {
			return nil, fmt.Errorf("invalid incremental filename %s: %v", fileName, err)
		}
		info.BaseSlot = baseSlot
		info.Slot = endSlot
	} else {
		slot, err := ExtractFullSnapshotSlot(fileName)
		if err != nil {
			return nil, fmt.Errorf("invalid full snapshot filename %s: %v", fileName, err)
		}
		info.Slot = slot
	}

	return info, nil
}

// IsSnapshotValid checks if a snapshot at the given slot is still valid
// based on the reference slot and validity window.
// For Agave 2.0+, snapshots are valid for ~25k slots after creation.
func IsSnapshotValid(snapshotSlot, referenceSlot, validitySlots int) bool {
	if validitySlots <= 0 {
		validitySlots = 25000 // Default Agave 2.0 validity
	}
	age := referenceSlot - snapshotSlot
	return age >= 0 && age <= validitySlots
}

// ValidateExistingSnapshot checks if an existing local snapshot is still usable.
// Returns the path and slot if valid, or error if not found/expired.
func ValidateExistingSnapshot(snapshotPath string, referenceSlot, validitySlots int) (string, int, error) {
	path, slot, err := FindRecentFullSnapshot(snapshotPath, referenceSlot, 0)
	if err != nil {
		return "", 0, err
	}

	if !IsSnapshotValid(slot, referenceSlot, validitySlots) {
		age := referenceSlot - slot
		return "", 0, fmt.Errorf("existing snapshot at slot %d is too old (age: %d slots, validity: %d slots)",
			slot, age, validitySlots)
	}

	return path, slot, nil
}

// SnapshotPair holds matched full and incremental snapshot URLs
// where the incremental's base slot matches the full snapshot's slot.
type SnapshotPair struct {
	Full        *SnapshotURLInfo
	Incremental *SnapshotURLInfo // nil if no matching incremental found
}

// GetMatchedSnapshotURLs probes an RPC endpoint and returns both full and incremental
// snapshot URLs, ensuring the incremental's base slot matches the full snapshot's slot.
// This is the recommended function for mithril integration.
func GetMatchedSnapshotURLs(ctx context.Context, rpcAddress string) (*SnapshotPair, error) {
	// Get full snapshot URL first
	fullInfo, err := GetSnapshotURL(ctx, rpcAddress, "full")
	if err != nil {
		return nil, fmt.Errorf("failed to get full snapshot: %v", err)
	}

	pair := &SnapshotPair{Full: fullInfo}

	// Try to get matching incremental
	incInfo, err := GetSnapshotURL(ctx, rpcAddress, "incremental")
	if err != nil {
		log.Printf("No incremental snapshot available at %s: %v", rpcAddress, err)
		return pair, nil // Return with just full snapshot
	}

	// Check if incremental base matches full slot
	if incInfo.BaseSlot != fullInfo.Slot {
		log.Printf("Incremental base slot (%d) doesn't match full slot (%d) - incremental not usable",
			incInfo.BaseSlot, fullInfo.Slot)
		return pair, nil // Return with just full snapshot
	}

	pair.Incremental = incInfo
	return pair, nil
}

// RefreshIncrementalURL re-probes for an incremental snapshot and checks if its base
// matches the given full snapshot slot. Use this after downloading a full snapshot
// to get a fresh incremental URL.
func RefreshIncrementalURL(ctx context.Context, rpcAddress string, fullSnapshotSlot int) (*SnapshotURLInfo, error) {
	incInfo, err := GetSnapshotURL(ctx, rpcAddress, "incremental")
	if err != nil {
		return nil, err
	}

	if incInfo.BaseSlot != fullSnapshotSlot {
		return nil, fmt.Errorf("incremental base slot (%d) doesn't match full slot (%d)",
			incInfo.BaseSlot, fullSnapshotSlot)
	}

	return incInfo, nil
}

// SnapshotSession tracks the state of a two-phase snapshot download workflow.
// This is designed for mithril integration where:
//  1. First, get the full snapshot URL and stream/process it
//  2. After the full snapshot is processed, get a matching incremental URL
//
// The session remembers which node and slot were used for the full snapshot,
// so subsequent calls to GetIncrementalURL know what base slot to match.
type SnapshotSession struct {
	NodeRPC          string           // The RPC endpoint being used
	FullSnapshot     *SnapshotURLInfo // Full snapshot info (set after GetFullSnapshotURL)
	IncrementalSnapshot *SnapshotURLInfo // Incremental info (set after GetIncrementalURL)
}

// NewSnapshotSession creates a new session for a given RPC endpoint.
// Use this to coordinate full + incremental snapshot downloads.
func NewSnapshotSession(rpcAddress string) *SnapshotSession {
	return &SnapshotSession{
		NodeRPC: rpcAddress,
	}
}

// GetFullSnapshotURL probes for the full snapshot URL and stores it in the session.
// Call this first. The returned URL can be streamed directly.
func (s *SnapshotSession) GetFullSnapshotURL(ctx context.Context) (*SnapshotURLInfo, error) {
	info, err := GetSnapshotURL(ctx, s.NodeRPC, "full")
	if err != nil {
		return nil, err
	}
	s.FullSnapshot = info
	return info, nil
}

// GetIncrementalURL probes for an incremental snapshot that matches the full snapshot.
// Call this AFTER processing the full snapshot.
// Returns an error if no matching incremental is available (base slot must match full slot).
func (s *SnapshotSession) GetIncrementalURL(ctx context.Context) (*SnapshotURLInfo, error) {
	if s.FullSnapshot == nil {
		return nil, fmt.Errorf("must call GetFullSnapshotURL first")
	}

	info, err := RefreshIncrementalURL(ctx, s.NodeRPC, s.FullSnapshot.Slot)
	if err != nil {
		return nil, err
	}
	s.IncrementalSnapshot = info
	return info, nil
}

// GetFullSlot returns the slot of the full snapshot (for matching incrementals).
// Returns 0 if GetFullSnapshotURL hasn't been called yet.
func (s *SnapshotSession) GetFullSlot() int {
	if s.FullSnapshot == nil {
		return 0
	}
	return s.FullSnapshot.Slot
}

// SearchOtherNodesForIncremental searches a list of alternative RPC endpoints
// for an incremental that matches our full snapshot slot.
// Use this if GetIncrementalURL fails because the primary node's incremental
// no longer matches (e.g., it updated while we were processing the full).
func (s *SnapshotSession) SearchOtherNodesForIncremental(ctx context.Context, alternativeRPCs []string) (*SnapshotURLInfo, error) {
	if s.FullSnapshot == nil {
		return nil, fmt.Errorf("must call GetFullSnapshotURL first")
	}

	fullSlot := s.FullSnapshot.Slot
	log.Printf("Searching %d alternative nodes for incremental with base slot %d...", len(alternativeRPCs), fullSlot)

	for i, rpc := range alternativeRPCs {
		if rpc == s.NodeRPC {
			continue // Skip the primary node
		}

		info, err := RefreshIncrementalURL(ctx, rpc, fullSlot)
		if err != nil {
			log.Printf("  Node %d/%d (%s): %v", i+1, len(alternativeRPCs), rpc, err)
			continue
		}

		log.Printf("  Found matching incremental at %s (base=%d, end=%d)", rpc, info.BaseSlot, info.Slot)
		s.IncrementalSnapshot = info
		return info, nil
	}

	return nil, fmt.Errorf("no incremental with base slot %d found across %d nodes", fullSlot, len(alternativeRPCs))
}

// TempFileTracker allows tracking temporary files for cleanup on cancellation
type TempFileTracker interface {
	AddTempFile(path string)
	RemoveTempFile(path string)
}

type ProgressWriter struct {
	TotalBytes   int64
	Downloaded   int64
	LastLoggedAt time.Time
	StartTime    time.Time
	FileName     string // Optional: display filename in progress
}

// formatBytes formats bytes as human-readable string (GB, MB, KB)
func formatBytes(bytes int64) string {
	const (
		GB = 1024 * 1024 * 1024
		MB = 1024 * 1024
		KB = 1024
	)
	if bytes >= GB {
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	} else if bytes >= MB {
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	} else if bytes >= KB {
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	}
	return fmt.Sprintf("%d B", bytes)
}

// formatDuration formats seconds as human-readable duration
func formatDuration(seconds float64) string {
	if seconds < 0 || seconds > 86400*7 { // Cap at 7 days
		return "calculating..."
	}
	if seconds < 60 {
		return fmt.Sprintf("%.0fs", seconds)
	}
	minutes := int(seconds) / 60
	secs := int(seconds) % 60
	if minutes < 60 {
		return fmt.Sprintf("%dm %02ds", minutes, secs)
	}
	hours := minutes / 60
	mins := minutes % 60
	return fmt.Sprintf("%dh %02dm", hours, mins)
}

// buildProgressBar creates an ASCII progress bar
func buildProgressBar(percentage float64, width int) string {
	if percentage < 0 {
		percentage = 0
	}
	if percentage > 100 {
		percentage = 100
	}
	filled := int(percentage / 100 * float64(width))
	empty := width - filled
	return strings.Repeat("█", filled) + strings.Repeat("░", empty)
}

func (pw *ProgressWriter) Write(p []byte) (int, error) {
	n := len(p)
	atomic.AddInt64(&pw.Downloaded, int64(n))

	now := time.Now()
	elapsed := now.Sub(pw.StartTime).Seconds()

	// Log progress every 5 seconds
	if now.Sub(pw.LastLoggedAt) >= 5*time.Second {
		pw.LastLoggedAt = now

		downloaded := atomic.LoadInt64(&pw.Downloaded)
		speed := float64(downloaded) / 1024 / 1024 / elapsed // MB/s
		percentage := float64(0)
		if pw.TotalBytes > 0 {
			percentage = (float64(downloaded) / float64(pw.TotalBytes)) * 100
		}

		// Calculate estimated time remaining
		var etaStr string
		if downloaded > 0 && pw.TotalBytes > 0 {
			remainingBytes := pw.TotalBytes - downloaded
			bytesPerSec := float64(downloaded) / elapsed
			if bytesPerSec > 0 {
				remainingSeconds := float64(remainingBytes) / bytesPerSec
				etaStr = formatDuration(remainingSeconds)
			} else {
				etaStr = "calculating..."
			}
		} else {
			etaStr = "unknown"
		}

		// Build ASCII progress bar (40 chars wide)
		progressBar := buildProgressBar(percentage, 40)

		// Format: [████████████░░░░░░░░░░░░░░░░] 25.0% | 123.45 MB/s | 1.23 GB / 5.00 GB | ETA: 5m 30s
		log.Printf("[%s] %5.1f%% | %6.2f MB/s | %s / %s | ETA: %s",
			progressBar,
			percentage,
			speed,
			formatBytes(downloaded),
			formatBytes(pw.TotalBytes),
			etaStr)
	}
	return n, nil
}

func DownloadGenesis(rpcAddress, snapshotPath string) error {
	tmpDir := filepath.Join(snapshotPath, "tmp")
	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create TMP directory: %v", err)
	}

	genesisURL := fmt.Sprintf("%s/genesis.tar.bz2", rpcAddress)
	_, _, err := writeSnapshotToFile(genesisURL, tmpDir, snapshotPath, true)
	if err != nil {
		return fmt.Errorf("failed to download genesis snapshot: %v", err)
	}

	return nil
}

func writeSnapshotToFile(snapshotURL, tmpDir, baseDir string, genesis bool) (string, int64, error) {
	// Ensure the temporary directory exists
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return "", 0, fmt.Errorf("failed to create temporary directory %s: %v", tmpDir, err)
	}
	//log.Printf("Created/verified tmp directory: %s", tmpDir)

	// Initiate the download
	resp, err := http.Get(snapshotURL)
	if err != nil {
		return "", 0, fmt.Errorf("failed to fetch snapshot: %v", err)
	}
	defer resp.Body.Close()

	// Get the final URL after redirects
	finalURL := resp.Request.URL.String()
	fileName := filepath.Base(finalURL)
	log.Printf("Original final URL: %s", finalURL)
	log.Printf("Original filename from final URL: %s", fileName)

	if fileName == "" {
		return "", 0, fmt.Errorf("invalid file name parsed from URL: %s", finalURL)
	}

	// Extract the slot and hash from the response headers or URL
	if contentDisposition := resp.Header.Get("Content-Disposition"); contentDisposition != "" {
		if strings.Contains(contentDisposition, "filename=") {
			fileName = strings.Split(contentDisposition, "filename=")[1]
			fileName = strings.Trim(fileName, `"'`)
			log.Printf("Filename from Content-Disposition: %s", fileName)
		}
	}

	// Define paths for temporary and final files
	tmpFilePath := filepath.Join(tmpDir, "tmp-"+fileName)
	//log.Printf("Temporary file path: %s", tmpFilePath)

	var finalFilePath string
	if genesis {
		finalFilePath = filepath.Join(baseDir, fileName)
	} else {
		remoteFilePath := filepath.Join(baseDir, "remote")
		if err := os.MkdirAll(remoteFilePath, 0755); err != nil {
			return "", 0, fmt.Errorf("failed to create remote directory %s: %v", remoteFilePath, err)
		}
		//log.Printf("Created/verified remote directory: %s", remoteFilePath)
		finalFilePath = filepath.Join(remoteFilePath, fileName)
	}
	//log.Printf("Final file path: %s", finalFilePath)

	// Create a temporary file for download
	tmpFile, err := os.Create(tmpFilePath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temporary file %s: %v", tmpFilePath, err)
	}
	//log.Printf("Created temporary file: %s", tmpFilePath)

	// Track download progress
	pw := &ProgressWriter{
		TotalBytes:   resp.ContentLength,
		StartTime:    time.Now(),
		LastLoggedAt: time.Now(),
	}

	// Download to temporary file
	totalBytes, err := io.Copy(io.MultiWriter(tmpFile, pw), resp.Body)
	if err != nil {
		log.Printf("Error during download: %v", err)
		return "", 0, fmt.Errorf("error during snapshot download: %v", err)
	}
	tmpFile.Close()
	//log.Printf("Download completed to temporary file. Size: %d bytes", totalBytes)

	// Copy from temporary to final location
	srcFile, err := os.Open(tmpFilePath)
	if err != nil {
		log.Printf("Error opening temp file: %v", err)
		return "", 0, fmt.Errorf("failed to open temp file for copying: %v", err)
	}
	defer srcFile.Close()

	// Create the destination file with explicit permissions
	dstFile, err := os.OpenFile(finalFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("Error creating destination file: %v", err)
		return "", 0, fmt.Errorf("failed to create destination file: %v", err)
	}
	defer dstFile.Close()

	// Copy the file contents
	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		log.Printf("Error copying file: %v", err)
		return "", 0, fmt.Errorf("failed to copy file to final location: %v", err)
	}
	//log.Printf("Copied %d bytes to final location: %s", copiedBytes, finalFilePath)

	// Sync the file to disk
	if err = dstFile.Sync(); err != nil {
		log.Printf("Warning: Failed to sync file to disk: %v", err)
	}

	// Keep the temporary file as backup
	backupPath := filepath.Join(tmpDir, "backup-"+fileName)
	if err := os.Rename(tmpFilePath, backupPath); err != nil {
		log.Printf("Warning: Failed to rename temporary file to backup: %v", err)
	} else {
		//log.Printf("Kept backup file at: %s", backupPath)
	}

	// Verify the final file exists
	if _, err := os.Stat(finalFilePath); err != nil {
		// If final file doesn't exist but we have a backup, try to restore from backup
		if _, backupErr := os.Stat(backupPath); backupErr == nil {
			log.Printf("Final file missing, attempting to restore from backup")
			if err := copyFile(backupPath, finalFilePath); err != nil {
				log.Printf("Failed to restore from backup: %v", err)
			} else {
				log.Printf("Successfully restored from backup")
			}
		}
		return "", 0, fmt.Errorf("final file does not exist after copy: %v", err)
	}
	//log.Printf("Verified final file exists: %s", finalFilePath)

	// Print final progress
	speed := float64(totalBytes) / 1024 / 1024 / time.Since(pw.StartTime).Seconds()
	log.Printf("Download completed: %s | Speed: %.2f MB/s", finalFilePath, speed)
	return finalFilePath, totalBytes, nil
}

func DownloadSnapshot(rpcAddress string, cfg config.Config, snapshotType string, referenceSlot int) (string, error) {
	// Define the TMP directory
	tmpDir := filepath.Join(cfg.SnapshotPath, "tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create TMP directory: %v", err)
	}

	// Ensure remote directory exists with proper permissions
	remoteDir := filepath.Join(cfg.SnapshotPath, "remote")
	if err := os.MkdirAll(remoteDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create remote directory: %v", err)
	}

	// Use FullThreshold as validity window
	validitySlots := cfg.FullThreshold
	if validitySlots <= 0 {
		validitySlots = 100000
	}

	// Get existing snapshot slot if it exists
	var existingSlot int
	var existingSnapshotPath string
	var existingSnapshotTooOld bool
	if strings.HasPrefix(snapshotType, "snapshot-") {
		path, slot, err := ValidateExistingSnapshot(cfg.SnapshotPath, referenceSlot, validitySlots)
		if err == nil {
			existingSnapshotPath = path
			existingSlot = slot
			existingAge := referenceSlot - existingSlot
			log.Printf("Found valid existing full snapshot: %s (Slot: %d, Age: %d/%d slots)",
				filepath.Base(existingSnapshotPath), existingSlot, existingAge, validitySlots)
		} else {
			// Check if we have an expired snapshot
			oldPath, oldSlot, findErr := FindRecentFullSnapshot(cfg.SnapshotPath, referenceSlot, 0)
			if findErr == nil {
				existingSnapshotPath = oldPath
				existingSlot = oldSlot
				existingSnapshotTooOld = true
				oldAge := referenceSlot - oldSlot
				log.Printf("Existing snapshot expired: %s (Slot: %d, Age: %d slots > validity: %d slots)",
					filepath.Base(oldPath), oldSlot, oldAge, validitySlots)
			}
		}
	}

	// Try both .tar.bz2 and .tar.zst extensions
	var finalPath string
	//var sizeBytes int64
	var downloadErr error

	extensions := []string{".tar.bz2", ".tar.zst"}
	for _, ext := range extensions {
		var snapshotURL string
		if strings.HasPrefix(snapshotType, "incremental") {
			snapshotURL = fmt.Sprintf("%s/incremental-snapshot%s", rpcAddress, ext)
		} else {
			snapshotURL = fmt.Sprintf("%s/snapshot%s", rpcAddress, ext)
		}
		//log.Printf("Trying URL: %s", snapshotURL)

		// Make a HEAD request first to get information about the snapshot
		client := &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return nil // Allow redirects
			},
		}
		resp, err := client.Head(snapshotURL)
		if err != nil {
			//log.Printf("HEAD request failed for %s: %v", snapshotURL, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			//log.Printf("HEAD request returned status %d for %s", resp.StatusCode, snapshotURL)
			resp.Body.Close()
			continue
		}

		// Try to get filename from the Content-Disposition header or final URL
		fileName := filepath.Base(resp.Request.URL.String())
		if contentDisposition := resp.Header.Get("Content-Disposition"); contentDisposition != "" {
			parts := strings.Split(contentDisposition, "filename=")
			if len(parts) > 1 {
				fileName = strings.Trim(parts[1], `"' `)
				log.Printf("Filename from Content-Disposition: %s", fileName)
			}
		}
		//log.Printf("Remote snapshot filename: %s", fileName)
		resp.Body.Close()

		// Extract slot from the filename
		var remoteSlot int
		if strings.HasPrefix(snapshotType, "snapshot-") {
			var err error
			remoteSlot, err = ExtractFullSnapshotSlot(fileName)
			if err != nil {
				//log.Printf("Warning: Could not extract slot from filename %s: %v", fileName, err)
				continue
			}
			//log.Printf("Remote snapshot slot: %d", remoteSlot)

			// Compare with existing slot and skip download if not newer
			// But only skip if existing is not too old
			if existingSlot > 0 && remoteSlot <= existingSlot && !existingSnapshotTooOld {
				log.Printf("Remote snapshot (slot %d) is not newer than existing snapshot (slot %d). Using existing snapshot.",
					remoteSlot, existingSlot)
				return existingSnapshotPath, nil
			}

			// If existing is too old and remote is not newer, continue trying other RPCs
			if existingSlot > 0 && remoteSlot <= existingSlot && existingSnapshotTooOld {
				log.Printf("Remote snapshot (slot %d) is not newer than existing (slot %d), but existing is too old. Trying next RPC.",
					remoteSlot, existingSlot)
				continue
			}
		}

		// Proceed with the download since we've confirmed it's a newer snapshot
		finalPath, _, downloadErr = writeSnapshotToFile(snapshotURL, tmpDir, cfg.SnapshotPath, false)
		if downloadErr == nil {
			//log.Printf("Successfully downloaded snapshot to: %s", finalPath)
			break
		}
		//log.Printf("Failed to download with %s extension: %v", ext, downloadErr)
	}

	if downloadErr != nil {
		// If we have an existing snapshot that's still valid (not too old), use it
		if existingSnapshotPath != "" && !existingSnapshotTooOld {
			log.Printf("No newer snapshot available, but existing snapshot is still within threshold. Using existing: %s",
				existingSnapshotPath)
			return existingSnapshotPath, nil
		}
		return "", fmt.Errorf("failed to download snapshot with any extension: %v", downloadErr)
	}

	// If finalPath is empty (all RPCs skipped) but we have a valid existing snapshot, use it
	if finalPath == "" && existingSnapshotPath != "" && !existingSnapshotTooOld {
		log.Printf("All RPCs checked. Using existing snapshot within threshold: %s", existingSnapshotPath)
		return existingSnapshotPath, nil
	}

	// If finalPath is empty and existing is too old, that's an error
	if finalPath == "" {
		return "", fmt.Errorf("no snapshot downloaded and no valid existing snapshot found")
	}

	// Verify the file exists in the remote directory
	if _, err := os.Stat(finalPath); err != nil {
		// Try to restore from backup if the final file is missing
		backupFile := filepath.Join(tmpDir, "backup-"+filepath.Base(finalPath))
		if _, backupErr := os.Stat(backupFile); backupErr == nil {
			log.Printf("Attempting to restore from backup: %s", backupFile)
			if err := copyFile(backupFile, finalPath); err != nil {
				return "", fmt.Errorf("failed to restore from backup: %v", err)
			}
			log.Printf("Successfully restored from backup to: %s", finalPath)
		} else {
			return "", fmt.Errorf("snapshot file not found and no backup available: %v", err)
		}
	}
	//log.Printf("Verified snapshot exists at: %s", finalPath)

	// Verify the downloaded snapshot without removing files
	if strings.HasPrefix(snapshotType, "snapshot-") {
		downloadedSlot, err := ExtractFullSnapshotSlot(finalPath)
		if err != nil {
			log.Printf("Error: Failed to extract slot from downloaded snapshot: %v", err)
			return "", fmt.Errorf("failed to extract slot from downloaded snapshot: %v", err)
		}

		// Double-check that downloaded slot matches what we expected
		if existingSlot > 0 && downloadedSlot <= existingSlot {
			log.Printf("Warning: Downloaded snapshot (slot %d) is not newer than existing snapshot (slot %d). Removing redundant download and using existing.",
				downloadedSlot, existingSlot)
			// Remove the downloaded file since it's not newer
			if err := os.Remove(finalPath); err != nil {
				log.Printf("Warning: Failed to remove redundant downloaded snapshot: %v", err)
			}
			return existingSnapshotPath, nil
		}

		if downloadedSlot < referenceSlot-cfg.FullThreshold {
			//log.Printf("Warning: Downloaded snapshot might be old, but keeping it anyway")
		}
	} else {
		slotStart, slotEnd, err := ExtractIncrementalSnapshotSlots(finalPath)
		if err != nil {
			log.Printf("Error: Failed to extract slots from incremental snapshot: %v", err)
			return "", fmt.Errorf("failed to extract slots from incremental snapshot: %v", err)
		}

		if referenceSlot-slotEnd > cfg.IncrementalThreshold {
			log.Printf("Warning: Incremental snapshot might be old, but keeping it anyway")
		}

		_, fullSlot, err := FindRecentFullSnapshot(cfg.SnapshotPath, referenceSlot, 0)
		if err != nil {
			log.Printf("Error: Could not find full snapshot for incremental: %v", err)
			return "", fmt.Errorf("could not find full snapshot for incremental: %v", err)
		}

		if slotStart != fullSlot {
			log.Printf("Warning: Incremental snapshot might not match full snapshot, but keeping it anyway")
		}
	}

	/*duration := time.Since(start)
	seconds := int(duration.Seconds())
	if seconds < 60 {
		log.Printf("%s snapshot saved to: %s | Time: %d seconds | Size: %.2f MB",
			snapshotType, finalPath, seconds, float64(sizeBytes)/(1024*1024))
	} else {
		log.Printf("%s snapshot saved to: %s | Time: %d minutes, %d seconds | Size: %.2f MB",
			snapshotType, finalPath, seconds/60, seconds%60, float64(sizeBytes)/(1024*1024))
	}*/

	return finalPath, nil
}

// DownloadSnapshotWithContext downloads a snapshot with context support for cancellation.
// The tracker is used to register temporary files for cleanup on cancellation.
// rpcAddress is the RPC address or base URL of the snapshot source.
func DownloadSnapshotWithContext(ctx context.Context, rpcAddress string, cfg config.Config, snapshotType string, referenceSlot int, tracker TempFileTracker) (string, error) {
	// Check for cancellation before starting
	if ctx.Err() != nil {
		return "", ctx.Err()
	}

	// Define the TMP directory
	tmpDir := filepath.Join(cfg.SnapshotPath, "tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create TMP directory: %v", err)
	}

	// Ensure remote directory exists
	remoteDir := filepath.Join(cfg.SnapshotPath, "remote")
	if err := os.MkdirAll(remoteDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create remote directory: %v", err)
	}

	// Check for valid existing snapshot (using proper validity window)
	var existingSlot int
	var existingSnapshotPath string
	var existingIsValid bool

	if strings.HasPrefix(snapshotType, "snapshot-") {
		// Use FullThreshold as validity window
		validitySlots := cfg.FullThreshold
		if validitySlots <= 0 {
			validitySlots = 100000
		}

		path, slot, err := ValidateExistingSnapshot(cfg.SnapshotPath, referenceSlot, validitySlots)
		if err == nil {
			existingSnapshotPath = path
			existingSlot = slot
			existingIsValid = true
			existingAge := referenceSlot - existingSlot
			log.Printf("Found valid existing full snapshot: %s (Slot: %d, Age: %d/%d slots)",
				filepath.Base(existingSnapshotPath), existingSlot, existingAge, validitySlots)
		} else {
			// Check if we have an expired snapshot (for logging)
			oldPath, oldSlot, findErr := FindRecentFullSnapshot(cfg.SnapshotPath, referenceSlot, 0)
			if findErr == nil {
				oldAge := referenceSlot - oldSlot
				log.Printf("Existing snapshot expired: %s (Slot: %d, Age: %d slots > validity: %d slots)",
					filepath.Base(oldPath), oldSlot, oldAge, validitySlots)
			}
		}
	}

	// Try constructing URLs from RPC address
	rpcAddr := strings.TrimSuffix(rpcAddress, "/")
	if !strings.HasPrefix(rpcAddr, "http://") && !strings.HasPrefix(rpcAddr, "https://") {
		rpcAddr = "http://" + rpcAddr
	}

	var finalPath string
	var downloadErr error

	extensions := []string{".tar.zst", ".tar.bz2"} // Prefer zst (faster decompression)
	for _, ext := range extensions {
		// Check for cancellation
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		var snapshotURL string
		if strings.HasPrefix(snapshotType, "incremental") {
			snapshotURL = fmt.Sprintf("%s/incremental-snapshot%s", rpcAddr, ext)
		} else {
			snapshotURL = fmt.Sprintf("%s/snapshot%s", rpcAddr, ext)
		}

		// Download with context support
		finalPath, downloadErr = writeSnapshotWithContext(ctx, snapshotURL, tmpDir, cfg.SnapshotPath, tracker)
		if downloadErr == nil {
			break
		}

		// If context was cancelled, return immediately
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
	}

	if downloadErr != nil {
		// Only fall back to existing if it's still valid
		if existingSnapshotPath != "" && existingIsValid {
			log.Printf("Download failed, using valid existing snapshot: %s", filepath.Base(existingSnapshotPath))
			return existingSnapshotPath, nil
		}
		return "", fmt.Errorf("failed to download snapshot: %v", downloadErr)
	}

	return finalPath, nil
}

// writeSnapshotWithContext downloads a snapshot file with context cancellation support
func writeSnapshotWithContext(ctx context.Context, snapshotURL, tmpDir, baseDir string, tracker TempFileTracker) (string, error) {
	// Ensure the temporary directory exists
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create temporary directory %s: %v", tmpDir, err)
	}

	// Create HTTP request with context
	req, err := http.NewRequestWithContext(ctx, "GET", snapshotURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to fetch snapshot: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP status %d for %s", resp.StatusCode, snapshotURL)
	}

	// Get the final URL after redirects
	finalURL := resp.Request.URL.String()
	fileName := filepath.Base(finalURL)

	if fileName == "" {
		return "", fmt.Errorf("invalid file name parsed from URL: %s", finalURL)
	}

	// Check Content-Disposition header
	if contentDisposition := resp.Header.Get("Content-Disposition"); contentDisposition != "" {
		if strings.Contains(contentDisposition, "filename=") {
			fileName = strings.Split(contentDisposition, "filename=")[1]
			fileName = strings.Trim(fileName, `"'`)
		}
	}

	// Define paths
	tmpFilePath := filepath.Join(tmpDir, "tmp-"+fileName)
	remoteFilePath := filepath.Join(baseDir, "remote")
	if err := os.MkdirAll(remoteFilePath, 0755); err != nil {
		return "", fmt.Errorf("failed to create remote directory: %v", err)
	}
	finalFilePath := filepath.Join(remoteFilePath, fileName)

	// Create temporary file
	tmpFile, err := os.Create(tmpFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file %s: %v", tmpFilePath, err)
	}

	// Register temp file for cleanup on cancellation
	if tracker != nil {
		tracker.AddTempFile(tmpFilePath)
	}

	// Track download progress
	pw := &ProgressWriter{
		TotalBytes:   resp.ContentLength,
		StartTime:    time.Now(),
		LastLoggedAt: time.Now(),
	}

	// Download with periodic context checks
	buf := make([]byte, 32*1024) // 32KB buffer
	var totalBytes int64

	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			tmpFile.Close()
			os.Remove(tmpFilePath)
			if tracker != nil {
				tracker.RemoveTempFile(tmpFilePath)
			}
			return "", ctx.Err()
		default:
		}

		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			_, writeErr := tmpFile.Write(buf[:n])
			if writeErr != nil {
				tmpFile.Close()
				os.Remove(tmpFilePath)
				if tracker != nil {
					tracker.RemoveTempFile(tmpFilePath)
				}
				return "", fmt.Errorf("error writing to file: %v", writeErr)
			}
			totalBytes += int64(n)
			pw.Write(buf[:n]) // Update progress
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			tmpFile.Close()
			os.Remove(tmpFilePath)
			if tracker != nil {
				tracker.RemoveTempFile(tmpFilePath)
			}
			return "", fmt.Errorf("error reading response: %v", readErr)
		}
	}
	tmpFile.Close()

	// Copy to final location
	srcFile, err := os.Open(tmpFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to open temp file: %v", err)
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(finalFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return "", fmt.Errorf("failed to create destination file: %v", err)
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return "", fmt.Errorf("failed to copy to final location: %v", err)
	}

	if err = dstFile.Sync(); err != nil {
		log.Printf("Warning: Failed to sync file to disk: %v", err)
	}

	// Rename temp to backup
	backupPath := filepath.Join(tmpDir, "backup-"+fileName)
	os.Rename(tmpFilePath, backupPath)

	// Remove from tracker since download completed successfully
	if tracker != nil {
		tracker.RemoveTempFile(tmpFilePath)
	}

	// Print final progress
	speed := float64(totalBytes) / 1024 / 1024 / time.Since(pw.StartTime).Seconds()
	log.Printf("Download completed: %s | Speed: %.2f MB/s", finalFilePath, speed)

	return finalFilePath, nil
}
