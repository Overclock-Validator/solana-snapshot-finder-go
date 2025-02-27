package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	if now.Sub(pw.LastLoggedAt) >= 3*time.Second {
		pw.LastLoggedAt = now
		speed := float64(pw.Downloaded) / 1024 / 1024 / elapsed // MB/s
		percentage := (float64(pw.Downloaded) / float64(pw.TotalBytes)) * 100
		log.Printf("Download progress: %.2f%% (%.2f MB/s) - Downloaded: %d bytes of %d bytes", percentage, speed, pw.Downloaded, pw.TotalBytes)
	}
	return n, nil
}

func cleanupOldSnapshots(snapshotPath string, referenceSlot, fullThreshold, incrementalThreshold int) error {
	log.Println("Cleaning up old snapshots...")

	// List all files in both main and remote directories
	directories := []string{snapshotPath, filepath.Join(snapshotPath, "remote")}

	// Add a grace period of 1 hour for recently downloaded files
	gracePeriod := time.Hour

	for _, dir := range directories {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue // Skip if directory doesn't exist
			}
			return fmt.Errorf("failed to read directory %s: %v", dir, err)
		}

		var removedCount int
		var totalSize int64

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			fileName := file.Name()
			filePath := filepath.Join(dir, fileName)

			// Skip files modified within grace period
			fileInfo, err := os.Stat(filePath)
			if err != nil {
				log.Printf("Failed to get file info for %s: %v", fileName, err)
				continue
			}

			// Skip recently modified files
			if time.Since(fileInfo.ModTime()) < gracePeriod {
				log.Printf("Skipping recent file (within grace period): %s", fileName)
				continue
			}

			// Handle full snapshots
			if strings.HasPrefix(fileName, "snapshot-") && strings.HasSuffix(fileName, ".tar.zst") {
				slot, err := ExtractFullSnapshotSlot(fileName)
				if err != nil {
					continue
				}

				diff := referenceSlot - slot
				// Use a more conservative threshold for cleanup (1.5x the normal threshold)
				conservativeThreshold := int(float64(fullThreshold) * 1.5)
				if diff > conservativeThreshold {
					size := file.Size()
					if err := os.Remove(filePath); err != nil {
						log.Printf("Failed to remove old full snapshot %s: %v", fileName, err)
						continue
					}
					removedCount++
					totalSize += size
					log.Printf("Removed old full snapshot: %s (Slot: %d, Diff: %d, Size: %.2f MB)",
						fileName, slot, diff, float64(size)/(1024*1024))
				}
			}

			// Handle incremental snapshots
			if strings.HasPrefix(fileName, "incremental-snapshot-") && strings.HasSuffix(fileName, ".tar.zst") {
				_, slotEnd, err := ExtractIncrementalSnapshotSlots(fileName)
				if err != nil {
					continue
				}

				diff := referenceSlot - slotEnd
				// Use a more conservative threshold for cleanup (1.5x the normal threshold)
				conservativeThreshold := int(float64(incrementalThreshold) * 1.5)
				if diff > conservativeThreshold {
					size := file.Size()
					if err := os.Remove(filePath); err != nil {
						log.Printf("Failed to remove old incremental snapshot %s: %v", fileName, err)
						continue
					}
					removedCount++
					totalSize += size
					log.Printf("Removed old incremental snapshot: %s (SlotEnd: %d, Diff: %d, Size: %.2f MB)",
						fileName, slotEnd, diff, float64(size)/(1024*1024))
				}
			}
		}

		if removedCount > 0 {
			log.Printf("Cleaned up %d old snapshots in %s (Total size freed: %.2f MB)",
				removedCount, dir, float64(totalSize)/(1024*1024))
		} else {
			log.Printf("No old snapshots to clean up in %s", dir)
		}
	}

	return nil
}

func manageSnapshots(config Config, referenceSlot int) (bool, bool) {
	log.Println("Checking snapshots...")

	// Find most recent full snapshot first
	fullSnapshot, fullSlot, err := findRecentFullSnapshot(config.SnapshotPath, referenceSlot, 0) // Pass 0 as threshold to get the most recent regardless of age
	if err != nil {
		log.Printf("No full snapshots found: %v", err)
		return true, true // Need both full and incremental
	}

	// Calculate full snapshot diff
	fullDiff := referenceSlot - fullSlot
	log.Printf("Full snapshot found: %s (Slot: %d, Diff: %d, Threshold: %d)",
		fullSnapshot, fullSlot, fullDiff, config.FullThreshold)

	// Check if full snapshot is too old
	if fullDiff > config.FullThreshold {
		log.Printf("Full snapshot is outdated. Diff (%d) exceeds threshold (%d)", fullDiff, config.FullThreshold)
		return true, true // Need both full and incremental
	}

	// Full snapshot is good, check incremental
	incrementalSnapshot, err := findHighestIncrementalSnapshot(config.SnapshotPath, fullSlot)
	if err != nil {
		log.Printf("No valid incremental snapshots found: %v", err)
		return false, true // Only need incremental
	}

	incrementalDiff := referenceSlot - incrementalSnapshot.SlotEnd
	log.Printf("Incremental snapshot found: %s (SlotStart: %d, SlotEnd: %d, Diff: %d, Threshold: %d)",
		incrementalSnapshot.FileName, incrementalSnapshot.SlotStart, incrementalSnapshot.SlotEnd,
		incrementalDiff, config.IncrementalThreshold)

	if incrementalDiff > config.IncrementalThreshold {
		log.Printf("Incremental snapshot is outdated. Diff (%d) exceeds threshold (%d)",
			incrementalDiff, config.IncrementalThreshold)
		return false, true // Only need new incremental
	}

	log.Println("All snapshots are within thresholds")
	return false, false
}

func deleteAllSnapshots(snapshotPath string) error {
	directories := []string{snapshotPath, filepath.Join(snapshotPath, "remote")}

	for _, dir := range directories {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			fileName := file.Name()
			// Match both full and incremental snapshots
			if (strings.HasPrefix(fileName, "snapshot-") ||
				strings.HasPrefix(fileName, "incremental-snapshot-")) &&
				strings.HasSuffix(fileName, ".tar.zst") {

				filePath := filepath.Join(dir, fileName)
				log.Printf("Deleting old snapshot: %s", fileName)
				if err := os.Remove(filePath); err != nil {
					log.Printf("Warning: Failed to delete snapshot %s: %v", fileName, err)
				}
			}
		}
	}
	return nil
}

func MeasureSpeed(url string, measureTime int) (float64, float64, error) {
	client := &http.Client{
		Timeout: 10 * time.Second, // Connection timeout
	}

	// Measure latency
	startTime := time.Now()
	resp, err := client.Get(url)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to fetch URL: %v", err)
	}
	defer resp.Body.Close()
	latency := time.Since(startTime).Milliseconds() // Latency in ms

	// Measure download speed
	buffer := make([]byte, 81920) // Chunk size
	var totalLoaded int64
	var speeds []float64

	lastTime := time.Now()
	for time.Since(startTime).Seconds() < float64(measureTime) {
		n, err := resp.Body.Read(buffer)
		if n > 0 {
			totalLoaded += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, float64(latency), fmt.Errorf("error reading response body: %v", err)
		}

		// Calculate speed every second
		elapsed := time.Since(lastTime).Seconds()
		if elapsed >= 1 {
			speed := float64(totalLoaded) / elapsed // Bytes/sec
			speeds = append(speeds, speed)
			lastTime = time.Now()
			totalLoaded = 0
		}
	}

	if len(speeds) == 0 {
		return 0, float64(latency), fmt.Errorf("no data collected during the measurement period")
	}

	medianSpeed := calculateMedian(speeds) / (1024 * 1024) // Convert to MB/s
	return medianSpeed, float64(latency), nil
}

func calculateMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	n := len(values)
	sort.Float64s(values)
	if n%2 == 0 {
		return (values[n/2-1] + values[n/2]) / 2
	}
	return values[n/2]
}

// IncrementalSnapshot represents an incremental snapshot with its slots.
type IncrementalSnapshot struct {
	FileName  string
	SlotStart int
	SlotEnd   int
}

func evaluateNodesWithVersions(nodes []RPCNode, config Config, defaultSlot int) []struct {
	rpc     string
	speed   float64
	latency float64
	slot    int
	diff    int
	version string
	status  string
} {
	totalNodes := len(nodes)
	results := make(chan struct {
		rpc     string
		speed   float64
		latency float64
		slot    int
		diff    int
		version string
		status  string
	}, totalNodes)

	var wg sync.WaitGroup
	sem := make(chan struct{}, config.WorkerCount)

	// Batch progress counter with atomic operations
	var processedNodes int32
	var goodNodes int32
	var slowNodes int32
	var badNodes int32

	// Progress logging ticker
	ticker := time.NewTicker(5 * time.Second)
	done := make(chan bool)

	// Progress reporting goroutine
	go func() {
		for {
			select {
			case <-ticker.C:
				processed := atomic.LoadInt32(&processedNodes)
				good := atomic.LoadInt32(&goodNodes)
				slow := atomic.LoadInt32(&slowNodes)
				bad := atomic.LoadInt32(&badNodes)
				log.Printf("Progress: %d/%d nodes processed (%.1f%%) | Good: %d, Slow: %d, Bad: %d",
					processed, totalNodes, float64(processed)/float64(totalNodes)*100, good, slow, bad)
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()

	// Helper to append results with atomic counters
	appendResult := func(node RPCNode, rpc string, speed, latency float64, slot, diff int, status string) {
		results <- struct {
			rpc     string
			speed   float64
			latency float64
			slot    int
			diff    int
			version string
			status  string
		}{
			rpc:     rpc,
			speed:   speed,
			latency: latency,
			slot:    slot,
			diff:    diff,
			version: node.Version,
			status:  status,
		}

		processed := atomic.AddInt32(&processedNodes, 1)
		switch status {
		case "good":
			atomic.AddInt32(&goodNodes, 1)
		case "slow":
			atomic.AddInt32(&slowNodes, 1)
		case "bad":
			atomic.AddInt32(&badNodes, 1)
		}

		// Log milestone progress (every 50 nodes)
		if processed%50 == 0 {
			log.Printf("Milestone reached: %d/%d nodes processed", processed, totalNodes)
		}
	}

	// Optimized health check function with timeout
	checkHealth := func(rpc string) bool {
		client := &http.Client{
			Timeout: 5 * time.Second,
		}
		resp, err := client.Get(rpc + "/health")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}

	// Process nodes in parallel
	for _, node := range nodes {
		wg.Add(1)
		go func(node RPCNode) {
			defer wg.Done()
			sem <- struct{}{}        // Acquire semaphore
			defer func() { <-sem }() // Release semaphore

			rpc := node.Address
			if !strings.HasPrefix(rpc, "http://") && !strings.HasPrefix(rpc, "https://") {
				rpc = "http://" + rpc
			}

			if !checkHealth(rpc) {
				appendResult(node, rpc, 0, 0, 0, 0, "bad")
				return
			}

			baseURL, err := url.Parse(rpc)
			if err != nil {
				appendResult(node, rpc, 0, 0, 0, 0, "bad")
				return
			}

			baseURL.Path = "/snapshot.tar.bz2"
			snapshotURL := baseURL.String()

			// Measure speed and latency with shorter measurement time
			speed, latency, err := MeasureSpeed(snapshotURL, config.SleepBeforeRetry/2)
			if err != nil {
				appendResult(node, rpc, speed, latency, 0, 0, "slow")
				return
			}

			// Fetch slot with timeout
			slot, err := getReferenceSlot(rpc)
			if err != nil {
				appendResult(node, rpc, speed, latency, 0, 0, "slow")
				return
			}

			diff := defaultSlot - slot
			status := "slow"
			if speed >= float64(config.MinDownloadSpeed) && latency <= float64(config.MaxLatency) && diff <= 100 {
				status = "good"
			}

			appendResult(node, rpc, speed, latency, slot, diff, status)
		}(node)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	done <- true // Signal the progress reporting goroutine to stop
	close(results)

	// Collect and sort results
	var evaluatedResults []struct {
		rpc     string
		speed   float64
		latency float64
		slot    int
		diff    int
		version string
		status  string
	}
	for result := range results {
		evaluatedResults = append(evaluatedResults, result)
	}

	// Sort results by speed (faster nodes first)
	sort.Slice(evaluatedResults, func(i, j int) bool {
		return evaluatedResults[i].speed > evaluatedResults[j].speed
	})

	// Print final summary
	log.Printf("Node evaluation complete: %d/%d nodes processed | Good: %d, Slow: %d, Bad: %d",
		atomic.LoadInt32(&processedNodes), totalNodes,
		atomic.LoadInt32(&goodNodes),
		atomic.LoadInt32(&slowNodes),
		atomic.LoadInt32(&badNodes))

	return evaluatedResults
}

// Summarizes the results of the node evaluation
func summarizeResultsWithVersions(results []struct {
	rpc     string
	speed   float64
	latency float64
	slot    int
	diff    int
	version string
	status  string
}) {
	totalNodes := len(results)
	goodNodes := 0
	slowNodes := 0
	badNodes := 0

	for _, result := range results {
		switch result.status {
		case "good":
			goodNodes++
		case "slow":
			slowNodes++
		case "bad":
			badNodes++
		}
	}

	log.Printf("Node evaluation complete. Total nodes: %d | Good: %d | Slow: %d | Bad: %d", totalNodes, goodNodes, slowNodes, badNodes)

	log.Println("List of good nodes:")
	for _, result := range results {
		if result.status == "good" {
			log.Printf("Node: %s | Speed: %.2f MB/s | Latency: %.2f ms | Slot: %d | Diff: %d | Version: %s",
				result.rpc, result.speed, result.latency, result.slot, result.diff, result.version)
		}
	}
}

// Dumps good and slow nodes into a JSON file
func dumpGoodAndSlowNodesToFile(
	results []struct {
		rpc     string
		speed   float64
		latency float64
		slot    int
		diff    int
		version string
		status  string
	},
	outputFile string,
) {
	// Filter good and slow nodes
	var filteredNodes []struct {
		RPC     string  `json:"rpc"`
		Speed   float64 `json:"speed"`
		Latency float64 `json:"latency"`
		Slot    int     `json:"slot"`
		Diff    int     `json:"diff"`
		Version string  `json:"version"`
		Status  string  `json:"status"`
	}

	for _, result := range results {
		if result.status == "good" || result.status == "slow" {
			filteredNodes = append(filteredNodes, struct {
				RPC     string  `json:"rpc"`
				Speed   float64 `json:"speed"`
				Latency float64 `json:"latency"`
				Slot    int     `json:"slot"`
				Diff    int     `json:"diff"`
				Version string  `json:"version"`
				Status  string  `json:"status"`
			}{
				RPC:     result.rpc,
				Speed:   result.speed,
				Latency: result.latency,
				Slot:    result.slot,
				Diff:    result.diff,
				Version: result.version,
				Status:  result.status,
			})
		}
	}

	// Write filtered nodes to JSON file
	file, err := os.Create(outputFile)
	if err != nil {
		log.Printf("Error creating output file: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // Pretty print JSON
	if err := encoder.Encode(filteredNodes); err != nil {
		log.Printf("Error writing to JSON file: %v", err)
		return
	}

	log.Printf("Good and slow nodes saved to %s", outputFile)
}

func cleanTmpDir(tmpDir string) error {
	files, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		return fmt.Errorf("failed to read temporary directory %s: %v", tmpDir, err)
	}

	for _, file := range files {
		if strings.HasPrefix(file.Name(), "tmp-") {
			tmpFilePath := filepath.Join(tmpDir, file.Name())
			log.Printf("Removing leftover temporary file: %s", tmpFilePath)
			err := os.Remove(tmpFilePath)
			if err != nil {
				log.Printf("Failed to remove temporary file %s: %v", tmpFilePath, err)
			}
		}
	}
	return nil
}

func writeSnapshotToFile(snapshotURL, tmpDir, baseDir string, genesis bool) (string, int64, error) {
	// Ensure the temporary directory exists
	err := os.MkdirAll(tmpDir, 0755)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temporary directory %s: %v", tmpDir, err)
	}
	log.Printf("Created/verified tmp directory: %s", tmpDir)

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
		// Try to get filename from Content-Disposition header
		if strings.Contains(contentDisposition, "filename=") {
			fileName = strings.Split(contentDisposition, "filename=")[1]
			fileName = strings.Trim(fileName, `"'`)
			log.Printf("Filename from Content-Disposition: %s", fileName)
		}
	}

	// Define paths for temporary and final files
	tmpFilePath := filepath.Join(tmpDir, "tmp-"+fileName)
	log.Printf("Temporary file path: %s", tmpFilePath)

	var finalFilePath string
	if genesis {
		finalFilePath = filepath.Join(baseDir, fileName)
	} else {
		remoteFilePath := filepath.Join(baseDir, "remote")
		// Ensure the remote directory exists with explicit permissions
		err = os.MkdirAll(remoteFilePath, 0755)
		if err != nil {
			return "", 0, fmt.Errorf("failed to create remote directory %s: %v", remoteFilePath, err)
		}
		log.Printf("Created/verified remote directory: %s", remoteFilePath)
		finalFilePath = filepath.Join(remoteFilePath, fileName)
	}
	log.Printf("Final file path: %s", finalFilePath)

	// Create a temporary file for download
	tmpFile, err := os.Create(tmpFilePath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temporary file %s: %v", tmpFilePath, err)
	}
	log.Printf("Created temporary file: %s", tmpFilePath)

	// Track download progress
	pw := &ProgressWriter{
		TotalBytes:   resp.ContentLength,
		StartTime:    time.Now(),
		LastLoggedAt: time.Now(),
	}

	// Download to temporary file
	var totalBytes int64
	totalBytes, err = io.Copy(io.MultiWriter(tmpFile, pw), resp.Body)
	if err != nil {
		log.Printf("Error during download: %v", err)
		return "", 0, fmt.Errorf("error during snapshot download: %v", err)
	}
	tmpFile.Close()
	log.Printf("Download completed to temporary file. Size: %d bytes", totalBytes)

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
	copiedBytes, err := io.Copy(dstFile, srcFile)
	if err != nil {
		log.Printf("Error copying file: %v", err)
		return "", 0, fmt.Errorf("failed to copy file to final location: %v", err)
	}
	log.Printf("Copied %d bytes to final location: %s", copiedBytes, finalFilePath)

	// Sync the file to disk
	err = dstFile.Sync()
	if err != nil {
		log.Printf("Warning: Failed to sync file to disk: %v", err)
	}

	// Keep the temporary file as backup
	backupPath := filepath.Join(tmpDir, "backup-"+fileName)
	if err := os.Rename(tmpFilePath, backupPath); err != nil {
		log.Printf("Warning: Failed to rename temporary file to backup: %v", err)
	} else {
		log.Printf("Kept backup file at: %s", backupPath)
	}

	// Verify the final file exists
	if _, err := os.Stat(finalFilePath); err != nil {
		// If final file doesn't exist but we have a backup, try to restore from backup
		if _, backupErr := os.Stat(backupPath); backupErr == nil {
			log.Printf("Final file missing, attempting to restore from backup")
			if err := os.Link(backupPath, finalFilePath); err != nil {
				log.Printf("Failed to restore from backup: %v", err)
			} else {
				log.Printf("Successfully restored from backup")
			}
		}
		return "", 0, fmt.Errorf("final file does not exist after copy: %v", err)
	}
	log.Printf("Verified final file exists: %s", finalFilePath)

	// Print final progress
	speed := float64(totalBytes) / 1024 / 1024 / time.Since(pw.StartTime).Seconds()
	log.Printf("Download completed: %s | Speed: %.2f MB/s", finalFilePath, speed)
	return finalFilePath, totalBytes, nil
}

func downloadSnapshot(rpcAddress string, config Config, snapshotType string, referenceSlot int) error {
	log.Printf("Downloading %s snapshot from %s", snapshotType, rpcAddress)

	start := time.Now()

	// Define the TMP directory
	tmpDir := filepath.Join(config.SnapshotPath, "tmp")
	err := os.MkdirAll(tmpDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create TMP directory: %v", err)
	}

	// Ensure remote directory exists with proper permissions
	remoteDir := filepath.Join(config.SnapshotPath, "remote")
	if err := os.MkdirAll(remoteDir, 0755); err != nil {
		return fmt.Errorf("failed to create remote directory: %v", err)
	}
	log.Printf("Ensured remote directory exists: %s", remoteDir)

	// Try both .tar.bz2 and .tar.zst extensions
	var snapshotURL string
	var finalPath string
	var sizeBytes int64
	var downloadErr error

	extensions := []string{".tar.bz2", ".tar.zst"}
	for _, ext := range extensions {
		// Construct URL
		if snapshotType == "incremental" {
			snapshotURL = fmt.Sprintf("%s/incremental-snapshot%s", rpcAddress, ext)
		} else {
			snapshotURL = fmt.Sprintf("%s/snapshot%s", rpcAddress, ext)
		}
		log.Printf("Trying URL: %s", snapshotURL)

		// Make a HEAD request first to check if the file exists and get its name
		client := &http.Client{
			Timeout: 10 * time.Second,
		}
		resp, err := client.Head(snapshotURL)
		if err != nil {
			log.Printf("HEAD request failed for %s: %v", snapshotURL, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			log.Printf("HEAD request returned status %d for %s", resp.StatusCode, snapshotURL)
			continue
		}

		// Try to download with current extension
		finalPath, sizeBytes, downloadErr = writeSnapshotToFile(snapshotURL, tmpDir, config.SnapshotPath, false)
		if downloadErr == nil {
			log.Printf("Successfully downloaded snapshot to: %s", finalPath)
			break // Successfully downloaded
		}
		log.Printf("Failed to download with %s extension: %v", ext, downloadErr)
	}

	if downloadErr != nil {
		return fmt.Errorf("failed to download snapshot with any extension: %v", downloadErr)
	}

	// Verify the file exists in the remote directory
	if _, err := os.Stat(finalPath); err != nil {
		// Try to restore from backup if the final file is missing
		backupFile := filepath.Join(tmpDir, "backup-"+filepath.Base(finalPath))
		if _, backupErr := os.Stat(backupFile); backupErr == nil {
			log.Printf("Attempting to restore from backup: %s", backupFile)
			// Copy the backup file to the remote directory
			if err := copyFile(backupFile, finalPath); err != nil {
				return fmt.Errorf("failed to restore from backup: %v", err)
			}
			log.Printf("Successfully restored from backup to: %s", finalPath)
		} else {
			return fmt.Errorf("snapshot file not found and no backup available: %v", err)
		}
	}
	log.Printf("Verified snapshot exists at: %s", finalPath)

	// Verify the downloaded snapshot without removing files
	if snapshotType == "full" {
		downloadedSlot, err := ExtractFullSnapshotSlot(finalPath)
		if err != nil {
			log.Printf("Warning: Failed to extract slot from downloaded snapshot: %v", err)
			return nil // Continue anyway since we have the file
		}

		// Check if the downloaded snapshot is actually newer
		if downloadedSlot < referenceSlot-config.FullThreshold {
			log.Printf("Warning: Downloaded snapshot might be old, but keeping it anyway")
		}

	} else {
		// For incremental snapshots
		slotStart, slotEnd, err := ExtractIncrementalSnapshotSlots(finalPath)
		if err != nil {
			log.Printf("Warning: Failed to extract slots from incremental snapshot: %v", err)
			return nil // Continue anyway since we have the file
		}

		// Check if the incremental snapshot is recent enough
		if referenceSlot-slotEnd > config.IncrementalThreshold {
			log.Printf("Warning: Incremental snapshot might be old, but keeping it anyway")
		}

		// Find the full snapshot slot to verify incremental start slot
		_, fullSlot, err := findRecentFullSnapshot(config.SnapshotPath, referenceSlot, 0)
		if err != nil {
			log.Printf("Warning: Could not find full snapshot: %v", err)
			return nil // Continue anyway since we have the file
		}

		// Verify the incremental starts with the full snapshot slot
		if slotStart != fullSlot {
			log.Printf("Warning: Incremental snapshot might not match full snapshot, but keeping it anyway")
		}
	}

	// Calculate duration and format it
	duration := time.Since(start)
	seconds := int(duration.Seconds())
	if seconds < 60 {
		log.Printf("%s snapshot saved to: %s | Time: %d seconds | Size: %.2f MB",
			snapshotType, finalPath, seconds, float64(sizeBytes)/(1024*1024))
	} else {
		log.Printf("%s snapshot saved to: %s | Time: %d minutes, %d seconds | Size: %.2f MB",
			snapshotType, finalPath, seconds/60, seconds%60, float64(sizeBytes)/(1024*1024))
	}

	// Final verification
	if _, err := os.Stat(finalPath); err != nil {
		log.Printf("Warning: Final verification failed, but files should be in tmp directory: %v", err)
	} else {
		log.Printf("Final verification successful, snapshot exists at: %s", finalPath)
	}

	return nil
}

// Helper function to copy a file
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %v", err)
	}
	defer sourceFile.Close()

	destFile, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %v", err)
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return fmt.Errorf("failed to copy file contents: %v", err)
	}

	err = destFile.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync destination file: %v", err)
	}

	return nil
}

func cleanupOldIncrementalSnapshots(snapshotPath string, fullSnapshotSlot int) error {
	directories := []string{snapshotPath, filepath.Join(snapshotPath, "remote")}

	// Store valid incremental snapshots (those matching the full snapshot slot)
	var validSnapshots []struct {
		path    string
		slotEnd int
	}

	// First pass: find all valid incremental snapshots
	for _, dir := range directories {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			fileName := file.Name()
			if strings.HasPrefix(fileName, "incremental-snapshot-") && (strings.HasSuffix(fileName, ".tar.zst") || strings.HasSuffix(fileName, ".tar.bz2")) {
				filePath := filepath.Join(dir, fileName)
				slotStart, slotEnd, err := ExtractIncrementalSnapshotSlots(fileName)
				if err != nil {
					continue
				}

				// Only keep incrementals that start with the full snapshot slot
				if slotStart == fullSnapshotSlot {
					validSnapshots = append(validSnapshots, struct {
						path    string
						slotEnd int
					}{
						path:    filePath,
						slotEnd: slotEnd,
					})
				} else {
					// Remove incremental with wrong start slot
					log.Printf("Removing incremental snapshot with wrong start slot: %s (Expected: %d, Got: %d)",
						fileName, fullSnapshotSlot, slotStart)
					os.Remove(filePath)
				}
			}
		}
	}

	// Sort valid snapshots by slot end (newest first)
	sort.Slice(validSnapshots, func(i, j int) bool {
		return validSnapshots[i].slotEnd > validSnapshots[j].slotEnd
	})

	// Keep only the 4 most recent valid snapshots
	for i := 4; i < len(validSnapshots); i++ {
		snapshot := validSnapshots[i]
		log.Printf("Removing old incremental snapshot (keeping 4 most recent): %s (SlotEnd: %d)",
			filepath.Base(snapshot.path), snapshot.slotEnd)
		os.Remove(snapshot.path)
	}

	return nil
}

// ExtractFullSnapshotSlot parses a full snapshot file name to extract the slot.
func ExtractFullSnapshotSlot(fileName string) (int, error) {
	// Regex for full snapshot: snapshot-<slot>-<hash>.tar.(zst|bz2)
	fullSnapshotRegex := `snapshot-(\d+)-[a-zA-Z0-9]+\.tar\.(zst|bz2)`

	match := regexp.MustCompile(fullSnapshotRegex).FindStringSubmatch(fileName)
	if match == nil {
		return 0, fmt.Errorf("file name does not match full snapshot pattern: %s", fileName)
	}

	slot, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, fmt.Errorf("failed to parse slot from full snapshot: %v", err)
	}
	return slot, nil
}

func ExtractIncrementalSnapshotSlots(fileName string) (int, int, error) {
	// Example regex: incremental-snapshot-<slotStart>-<slotEnd>-<hash>.tar.(zst|bz2)
	regex := regexp.MustCompile(`incremental-snapshot-(\d+)-(\d+)-[a-zA-Z0-9]+\.tar\.(zst|bz2)`)
	match := regex.FindStringSubmatch(fileName)

	if len(match) != 4 { // Now we expect 4 matches because of the extension group
		return 0, 0, fmt.Errorf("invalid incremental snapshot file name: %s", fileName)
	}

	slotStart, err1 := strconv.Atoi(match[1])
	slotEnd, err2 := strconv.Atoi(match[2])
	if err1 != nil || err2 != nil {
		return 0, 0, fmt.Errorf("failed to parse slots from file name: %s", fileName)
	}

	return slotStart, slotEnd, nil
}

func findSnapshotFiles(dir string, referenceSlot, threshold int, isFullSnapshot bool) ([]string, error) {
	// List files in the directory
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to list files in %s: %v", dir, err)
	}

	var snapshotFiles []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filePath := filepath.Join(dir, file.Name())

		var slot int
		var err error
		if isFullSnapshot {
			slot, err = ExtractFullSnapshotSlot(filePath)
		} else {
			_, slot, err = ExtractIncrementalSnapshotSlots(filePath)
		}

		if err != nil {
			log.Printf("Skipping file %s: %v", file.Name(), err)
			continue
		}

		// Check if the snapshot is within the threshold
		diff := referenceSlot - slot
		if diff > threshold {
			log.Printf("Skipping snapshot %s: Slot %d is beyond the threshold (%d > %d)", file.Name(), slot, diff, threshold)
			continue
		}

		// Add file if it meets the criteria
		snapshotFiles = append(snapshotFiles, filePath)
	}

	return snapshotFiles, nil
}

// Find recent full snapshot across both directories (destDir and destDir/remote).
func findRecentFullSnapshot(destDir string, referenceSlot, threshold int) (string, int, error) {
	var mostRecent string
	var mostRecentSlot int

	// Helper function to scan directory for snapshots
	scanDir := func(dir string) error {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				return nil // Skip if directory doesn't exist
			}
			return err
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			fileName := file.Name()
			if strings.HasPrefix(fileName, "snapshot-") && (strings.HasSuffix(fileName, ".tar.zst") || strings.HasSuffix(fileName, ".tar.bz2")) {
				slot, err := ExtractFullSnapshotSlot(fileName)
				if err != nil {
					log.Printf("Skipping file %s: %v", fileName, err)
					continue
				}

				// Update most recent snapshot
				if slot > mostRecentSlot {
					mostRecent = filepath.Join(dir, fileName)
					mostRecentSlot = slot
				}
			}
		}
		return nil
	}

	// Scan both main and remote directories
	if err := scanDir(destDir); err != nil {
		return "", 0, err
	}
	if err := scanDir(filepath.Join(destDir, "remote")); err != nil {
		return "", 0, err
	}

	if mostRecent == "" {
		return "", 0, fmt.Errorf("no full snapshots found")
	}

	log.Printf("Found most recent full snapshot: %s (Slot: %d)", mostRecent, mostRecentSlot)
	return mostRecent, mostRecentSlot, nil
}

// Find highest incremental snapshot across both directories (destDir and destDir/remote).
func findHighestIncrementalSnapshot(destDir string, baseSlot int) (IncrementalSnapshot, error) {
	// Find incremental snapshots in both destDir and remote directory
	incrementalSnapshotsDest, err := findSnapshotFiles(destDir, baseSlot, 0, false)
	if err != nil {
		return IncrementalSnapshot{}, err
	}

	remoteDir := filepath.Join(destDir, "remote")
	incrementalSnapshotsRemote, err := findSnapshotFiles(remoteDir, baseSlot, 0, false)
	if err != nil {
		return IncrementalSnapshot{}, err
	}

	// Combine snapshots from both directories
	allSnapshots := append(incrementalSnapshotsDest, incrementalSnapshotsRemote...)

	var highestSnapshot IncrementalSnapshot
	var highestSlotEnd int

	for _, filePath := range allSnapshots {
		startSlot, endSlot, err := ExtractIncrementalSnapshotSlots(filePath)
		if err != nil {
			continue // Skip invalid or non-incremental snapshot files
		}

		// Ensure the snapshot starts after the baseSlot
		if startSlot != baseSlot {
			continue
		}

		// Keep track of the snapshot with the highest end slot
		if endSlot > highestSlotEnd {
			highestSlotEnd = endSlot
			highestSnapshot = IncrementalSnapshot{
				FileName:  filePath,
				SlotStart: startSlot,
				SlotEnd:   endSlot,
			}
		}
	}

	if highestSlotEnd == 0 {
		return IncrementalSnapshot{}, fmt.Errorf("no valid incremental snapshots found starting at slot %d", baseSlot)
	}

	return highestSnapshot, nil
}

func isGenesisPresent(snapshotPath string) bool {
	genesisPath := filepath.Join(snapshotPath, "genesis.tar.bz2")
	if _, err := os.Stat(genesisPath); err == nil {
		return true
	}
	return false
}

func downloadGenesis(rpcAddress, snapshotPath string) error {
	// Define the TMP directory
	tmpDir := filepath.Join(snapshotPath, "tmp")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create TMP directory: %v", err)
	}

	// Clean TMP directory
	err = cleanTmpDir(tmpDir)
	if err != nil {
		return fmt.Errorf("failed to clean TMP directory: %v", err)
	}

	// Construct the genesis URL
	genesisURL := fmt.Sprintf("%s/genesis.tar.bz2", rpcAddress)

	// Download and handle the genesis snapshot
	_, _, err = writeSnapshotToFile(genesisURL, tmpDir, snapshotPath, true)
	if err != nil {
		return fmt.Errorf("failed to download genesis snapshot: %v", err)
	}

	return nil
}

func processSnapshots(config Config) {
	for attempt := 1; attempt <= config.NumOfRetries; attempt++ {
		log.Printf("Attempt %d/%d to manage snapshots...", attempt, config.NumOfRetries)

		// Fetch the current reference slot
		log.Println("Fetching reference slot...")
		referenceSlot, err := getReferenceSlot(config.RPCAddress)
		if err != nil {
			log.Printf("Failed to fetch reference slot on attempt %d: %v", attempt, err)
			time.Sleep(time.Duration(config.SleepBeforeRetry) * time.Second)
			continue
		}
		log.Printf("Reference Slot: %d", referenceSlot)

		// Manage snapshots
		fullNeeded, incrementalNeeded := manageSnapshots(config, referenceSlot)

		// Check if genesis snapshot is needed
		genesisNeeded := !isGenesisPresent(config.SnapshotPath)
		if genesisNeeded {
			log.Println("Genesis file is missing.")
		}

		// If no snapshots are needed, exit the loop
		if !fullNeeded && !incrementalNeeded {
			log.Println("All snapshots are up to date. No downloads required.")
			return
		}

		// Evaluate nodes if snapshots are needed
		log.Println("Fetching RPC nodes...")
		nodes := fetchRPCNodes(config)
		log.Printf("Evaluating %d nodes...", len(nodes))
		results := evaluateNodesWithVersions(nodes, config, referenceSlot)

		// Summarize results and select the best RPC node
		summarizeResultsWithVersions(results)
		dumpGoodAndSlowNodesToFile(results, config.SnapshotPath+"/nodes.json")
		bestRPC := selectBestRPC(results)

		// Download snapshots as required
		if bestRPC != "" {
			start := time.Now()

			if genesisNeeded {
				log.Println("Downloading genesis snapshot...")
				err := downloadGenesis(bestRPC, config.SnapshotPath)
				if err != nil {
					log.Printf("Failed to download genesis snapshot: %v", err)
					continue
				}
				log.Println("Genesis snapshot downloaded successfully.")
			}

			if fullNeeded {
				downloadSnapshot(bestRPC, config, "full", referenceSlot)
				incrementalNeeded = true // Ensure incremental is downloaded after full
			}

			if incrementalNeeded {
				downloadSnapshot(bestRPC, config, "incremental", referenceSlot)
			}

			// Log time taken for downloads
			duration := time.Since(start)
			log.Printf("Total download time: %s", duration)

			log.Println("Snapshots downloaded successfully.")
			return
		}

		// If successful, exit the loop
		if bestRPC != "" {
			log.Println("Snapshots downloaded successfully.")
			return
		}

		log.Println("Failed to find suitable RPC nodes or download snapshots. Retrying...")
	}

	// If all retries fail, log and exit
	log.Fatalf("Failed to fetch reference slot or manage snapshots after %d attempts.", config.NumOfRetries)
}
