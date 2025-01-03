package main

import (
	"fmt"
	"io"
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

	// Log progress every second
	if now.Sub(pw.LastLoggedAt) >= time.Second {
		pw.LastLoggedAt = now
		speed := float64(pw.Downloaded) / 1024 / 1024 / elapsed // MB/s
		percentage := (float64(pw.Downloaded) / float64(pw.TotalBytes)) * 100
		fmt.Printf("\rDownloaded: %d / %d bytes (%.2f%%) | Speed: %.2f MB/s", pw.Downloaded, pw.TotalBytes, percentage, speed)
	}
	return n, nil
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

func DownloadSnapshot(rpcAddress, destDir string) error {
	// Ensure the destination directory exists
	if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", destDir, err)
	}

	// Check if a full snapshot already exists
	existingSnapshot, err := findExistingSnapshot(destDir)
	if err != nil {
		return fmt.Errorf("error finding existing snapshot: %v", err)
	}

	// Parse the RPC URL
	baseURL, err := url.Parse(rpcAddress)
	if err != nil {
		return fmt.Errorf("invalid RPC URL: %s, error: %v", rpcAddress, err)
	}

	// Determine the snapshot type and path
	var snapshotType, snapshotPath string
	if existingSnapshot != "" {
		fmt.Printf("Existing full snapshot found: %s\n", existingSnapshot)
		baseURL.Path = "/incremental-snapshot.tar.bz2"
		snapshotType = "Incremental"
		snapshotPath = baseURL.String()
	} else {
		fmt.Println("No existing full snapshot found. Downloading full snapshot...")
		baseURL.Path = "/snapshot.tar.bz2"
		snapshotType = "Full"
		snapshotPath = baseURL.String()
	}

	// Initiate the download
	resp, err := http.Get(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to fetch snapshot: %v", err)
	}
	defer resp.Body.Close()

	// Get the final URL after redirects
	finalURL := resp.Request.URL.String()

	// Extract the filename from the final URL
	fileName := filepath.Base(finalURL)
	tempPath := filepath.Join(destDir, "tmp-"+fileName) // Temporary file path
	finalPath := filepath.Join(destDir, fileName)       // Final file path

	// Display snapshot name and type
	fmt.Printf("%s snapshot: %s\n", snapshotType, fileName)

	// Validate content length
	totalBytes := resp.ContentLength
	if totalBytes <= 0 {
		return fmt.Errorf("unable to determine the file size")
	}

	// Create the temporary output file
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer tempFile.Close()

	// Track and display progress with speed
	pw := &ProgressWriter{
		TotalBytes:   totalBytes,
		StartTime:    time.Now(),
		LastLoggedAt: time.Now(),
	}
	_, err = io.Copy(io.MultiWriter(tempFile, pw), resp.Body)
	if err != nil {
		return fmt.Errorf("error during download: %v", err)
	}

	// Ensure final progress message is printed
	speed := float64(pw.Downloaded) / 1024 / 1024 / time.Since(pw.StartTime).Seconds()
	fmt.Printf("\rDownloaded: %d / %d bytes (100%%) | Speed: %.2f MB/s\n", pw.Downloaded, pw.TotalBytes, speed)

	// Rename the temporary file to the final file
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("failed to rename file: %v", err)
	}

	fmt.Printf("Snapshot successfully downloaded and saved to: %s\n", finalPath)
	return nil
}

func findExistingSnapshot(destDir string) (string, error) {
	// Search for files matching the full snapshot pattern
	files, err := filepath.Glob(filepath.Join(destDir, "snapshot-*"))
	if err != nil {
		return "", fmt.Errorf("failed to search for existing snapshots: %v", err)
	}

	if len(files) == 0 {
		return "", nil // No existing snapshot found
	}

	// Return the most recent snapshot (sorted by name for simplicity)
	return files[0], nil
}

// IncrementalSnapshot represents an incremental snapshot with its slots.
type IncrementalSnapshot struct {
	FileName  string
	SlotStart int
	SlotEnd   int
}

// ParseIncrementalSnapshots parses a list of file names to find incremental snapshots.
func ParseIncrementalSnapshots(files []string) ([]IncrementalSnapshot, error) {
	incrementalSnapshotRegex := `incremental-snapshot-(\d+)-(\d+)-[a-zA-Z0-9]+\.tar\.zst`
	var snapshots []IncrementalSnapshot

	for _, file := range files {
		if match := regexp.MustCompile(incrementalSnapshotRegex).FindStringSubmatch(file); match != nil {
			slotStart, err1 := strconv.Atoi(match[1])
			slotEnd, err2 := strconv.Atoi(match[2])
			if err1 != nil || err2 != nil {
				return nil, fmt.Errorf("failed to parse slots from file %s: %v, %v", file, err1, err2)
			}
			snapshots = append(snapshots, IncrementalSnapshot{
				FileName:  file,
				SlotStart: slotStart,
				SlotEnd:   slotEnd,
			})
		}
	}

	return snapshots, nil
}

func FindHighestAndTrimIncrementals(snapshots []IncrementalSnapshot, maxSnapshots int, basePath string) (IncrementalSnapshot, error) {
	if len(snapshots) == 0 {
		return IncrementalSnapshot{}, fmt.Errorf("no incremental snapshots found")
	}

	// Sort by SlotEnd descending
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].SlotEnd > snapshots[j].SlotEnd
	})

	// Retain only the most recent `maxSnapshots`
	if len(snapshots) > maxSnapshots {
		for _, snapshot := range snapshots[maxSnapshots:] {
			fullPath := fmt.Sprintf("%s/%s", strings.TrimRight(basePath, "/"), snapshot.FileName)
			log.Printf("Deleting old incremental snapshot: %s", fullPath)
			err := os.Remove(fullPath)
			if err != nil {
				log.Printf("Failed to delete %s: %v", fullPath, err)
			}
		}
		snapshots = snapshots[:maxSnapshots]
	}

	// Return the most recent snapshot
	return snapshots[0], nil
}

// Reads snapshot files from the snapshot directory
func getSnapshotFiles(config Config) []string {
	files, err := os.ReadDir(config.SnapshotPath)
	if err != nil {
		log.Fatalf("Failed to read snapshot directory: %v", err)
	}

	var snapshotFiles []string
	for _, file := range files {
		snapshotFiles = append(snapshotFiles, file.Name())
	}
	return snapshotFiles
}

// Handles snapshot parsing, pruning, and logging
func handleSnapshots(snapshotFiles []string, config Config, defaultSlot int) *IncrementalSnapshot {
	incrementalSnapshots, err := ParseIncrementalSnapshots(snapshotFiles)
	if err != nil {
		log.Printf("Failed to parse incremental snapshots: %v", err)
		return nil
	}

	highestSnapshot, err := FindHighestAndTrimIncrementals(incrementalSnapshots, 5, config.SnapshotPath)
	if err != nil {
		log.Printf("No incremental snapshots found: %v", err)
		return nil
	}

	log.Printf("Highest incremental snapshot: %s | SlotStart: %d | SlotEnd: %d", highestSnapshot.FileName, highestSnapshot.SlotStart, highestSnapshot.SlotEnd)
	log.Printf("Highest incremental snapshot is %d slots behind the current slot.", defaultSlot-highestSnapshot.SlotEnd)

	return &highestSnapshot
}

func evaluateNodesWithVersions(nodes []RPCNode, config Config, defaultSlot int) []struct {
	rpc     string
	speed   float64
	latency float64
	slot    int
	diff    int
	version string
	status  string // "good", "slow", or "bad"
} {
	results := make(chan struct {
		rpc     string
		speed   float64
		latency float64
		slot    int
		diff    int
		version string
		status  string
	}, len(nodes))

	var wg sync.WaitGroup
	sem := make(chan struct{}, config.WorkerCount) // Semaphore to control concurrency

	// Helper to append results
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
	}

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

			baseURL, err := url.Parse(rpc)
			if err != nil {
				appendResult(node, rpc, 0, 0, 0, 0, "bad")
				return
			}

			baseURL.Path = "/snapshot.tar.bz2"
			snapshotURL := baseURL.String()

			// Measure speed and latency
			speed, latency, err := MeasureSpeed(snapshotURL, config.SleepBeforeRetry)
			if err != nil {
				appendResult(node, rpc, speed, latency, 0, 0, "slow")
				return
			}

			// Fetch slot
			slot, err := GetSlot(rpc)
			if err != nil {
				appendResult(node, rpc, speed, latency, 0, 0, "slow")
				return
			}

			// Calculate slot difference
			diff := defaultSlot - slot
			status := "slow"
			if speed >= float64(config.MinDownloadSpeed) && latency <= float64(config.MaxLatency) && diff <= 100 {
				status = "good"
			}

			appendResult(node, rpc, speed, latency, slot, diff, status)
		}(node)
	}

	wg.Wait()
	close(results)

	// Collect results
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

// Downloads the snapshot from the best RPC
func downloadSnapshot(bestRPC string, config Config) {
	log.Printf("Best RPC: %s", bestRPC)
	baseURL, _ := url.Parse(bestRPC)
	baseURL.Path = "/snapshot.tar.bz2"

	err := DownloadSnapshot(baseURL.String(), config.SnapshotPath)
	if err != nil {
		log.Fatalf("Failed to download snapshot: %v", err)
	}
	log.Println("Download complete.")
}
