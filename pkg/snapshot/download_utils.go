package snapshot

import (
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

type ProgressWriter struct {
	TotalBytes   int64
	Downloaded   int64
	LastLoggedAt time.Time
	StartTime    time.Time
}

func (pw *ProgressWriter) Write(p []byte) (int, error) {
	n := len(p)
	atomic.AddInt64(&pw.Downloaded, int64(n))

	now := time.Now()
	elapsed := now.Sub(pw.StartTime).Seconds()

	// Log progress every 3 seconds
	if now.Sub(pw.LastLoggedAt) >= 10*time.Second {
		pw.LastLoggedAt = now
		speed := float64(pw.Downloaded) / 1024 / 1024 / elapsed // MB/s
		percentage := (float64(pw.Downloaded) / float64(pw.TotalBytes)) * 100

		// Calculate estimated time remaining
		remainingBytes := pw.TotalBytes - pw.Downloaded
		remainingSeconds := float64(remainingBytes) / (float64(pw.Downloaded) / elapsed)
		var timeLeftStr string
		if remainingSeconds < 60 {
			timeLeftStr = fmt.Sprintf("%.0f seconds", remainingSeconds)
		} else {
			minutes := int(remainingSeconds / 60)
			seconds := int(remainingSeconds) % 60
			timeLeftStr = fmt.Sprintf("%d minutes, %d seconds", minutes, seconds)
		}

		log.Printf("snapshot download progress: %.2f%% (%.2f MB/s) - Downloaded: %d bytes of %d bytes - Time remaining: %s",
			percentage, speed, pw.Downloaded, pw.TotalBytes, timeLeftStr)
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
	log.Printf("Original filename from URL: %s", fileName)

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
	//start := time.Now()

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
	//log.Printf("Ensured remote directory exists: %s", remoteDir)

	// Get existing snapshot slot if it exists
	var existingSlot int
	var existingSnapshotPath string
	var existingSnapshotTooOld bool
	if strings.HasPrefix(snapshotType, "snapshot-") {
		var err error
		existingSnapshotPath, existingSlot, err = findRecentFullSnapshot(cfg.SnapshotPath, referenceSlot, 0)
		if err == nil {
			existingAge := referenceSlot - existingSlot
			log.Printf("Found existing full snapshot: %s (Slot: %d, Age: %d slots)",
				existingSnapshotPath, existingSlot, existingAge)

			if existingAge > cfg.FullThreshold {
				log.Printf("Existing snapshot is too old (age: %d > threshold: %d). Will try to download newer snapshot.",
					existingAge, cfg.FullThreshold)
				existingSnapshotTooOld = true
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

		_, fullSlot, err := findRecentFullSnapshot(cfg.SnapshotPath, referenceSlot, 0)
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
