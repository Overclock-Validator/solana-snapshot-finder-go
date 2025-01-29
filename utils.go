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

	// Log progress every second
	if now.Sub(pw.LastLoggedAt) >= time.Second {
		pw.LastLoggedAt = now
		speed := float64(pw.Downloaded) / 1024 / 1024 / elapsed // MB/s
		percentage := (float64(pw.Downloaded) / float64(pw.TotalBytes)) * 100
		fmt.Printf("\rDownloaded: %d / %d bytes (%.2f%%) | Speed: %.2f MB/s", pw.Downloaded, pw.TotalBytes, percentage, speed)
	}
	return n, nil
}

func manageSnapshots(config Config, referenceSlot int) (bool, bool) {
	log.Println("Checking snapshots...")
	fullNeeded := false
	incrementalNeeded := false

	// Check full snapshot
	fullSnapshot, fullSlot, err := findRecentFullSnapshot(config.SnapshotPath, referenceSlot, config.FullThreshold)
	if err != nil {
		log.Printf("Error finding recent full snapshot: %v", err)
		fullNeeded = true
	} else {
		fullDiff := referenceSlot - fullSlot
		log.Printf("Full snapshot found: %s (Slot: %d, Diff: %d, Threshold: %d)", fullSnapshot, fullSlot, fullDiff, config.FullThreshold)
		if fullDiff > config.FullThreshold {
			log.Printf("Full snapshot is outdated. Diff (%d) exceeds threshold (%d).", fullDiff, config.FullThreshold)
			fullNeeded = true
		} else {
			log.Printf("Full snapshot is within threshold.")
		}
	}

	// Check incremental snapshot if full snapshot is valid
	if !fullNeeded {
		incrementalSnapshot, err := findHighestIncrementalSnapshot(config.SnapshotPath, fullSlot)
		if err != nil {
			log.Printf("No valid incremental snapshots found: %v", err)
			incrementalNeeded = true
		} else {
			incrementalDiff := referenceSlot - incrementalSnapshot.SlotEnd
			log.Printf("Incremental snapshot found: %s (SlotStart: %d, SlotEnd: %d, Diff: %d, Threshold: %d)",
				incrementalSnapshot.FileName, incrementalSnapshot.SlotStart, incrementalSnapshot.SlotEnd, incrementalDiff, config.IncrementalThreshold)
			if incrementalDiff > config.IncrementalThreshold {
				log.Printf("Incremental snapshot is outdated. Diff (%d) exceeds threshold (%d).", incrementalDiff, config.IncrementalThreshold)
				incrementalNeeded = true
			} else {
				log.Printf("Incremental snapshot is within threshold.")
			}
		}
	}

	return fullNeeded, incrementalNeeded
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
	sem := make(chan struct{}, config.WorkerCount)

	// Batch progress counter
	var batchCounter int32

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

		// Increment the batch counter and print progress
		completed := atomic.AddInt32(&batchCounter, 1)
		if completed%100 == 0 {
			log.Printf("Processed %d nodes...\n", completed)
		}
	}

	// Optimized health check function
	checkHealth := func(rpc string) bool {
		// Perform a simple health check (optional, depending on the node)
		resp, err := http.Get(rpc + "/health")
		if err != nil || resp.StatusCode != http.StatusOK {
			return false
		}
		return true
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

			// Measure speed and latency (optimized)
			speed, latency, err := MeasureSpeed(snapshotURL, config.SleepBeforeRetry)
			if err != nil {
				appendResult(node, rpc, speed, latency, 0, 0, "slow")
				return
			}

			// Fetch slot
			slot, err := getReferenceSlot(rpc)
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
	err := os.MkdirAll(tmpDir, os.ModePerm)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temporary directory %s: %v", tmpDir, err)
	}

	// Initiate the download
	resp, err := http.Get(snapshotURL)
	if err != nil {
		return "", 0, fmt.Errorf("failed to fetch snapshot: %v", err)
	}
	defer resp.Body.Close()

	// Get the final URL after redirects
	finalURL := resp.Request.URL.String()
	fileName := filepath.Base(finalURL)
	if fileName == "" {
		return "", 0, fmt.Errorf("invalid file name parsed from URL: %s", finalURL)
	}

	// Define paths for temporary and final files
	tmpFilePath := filepath.Join(tmpDir, "tmp-"+fileName)
	remoteFilePath := filepath.Join(baseDir, "remote")
	// Ensure the remote directory exists
	err = os.MkdirAll(remoteFilePath, os.ModePerm)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create remote directory %s: %v", remoteFilePath, err)
	}

	var finalFilePath string

	if genesis {
		finalFilePath = filepath.Join(baseDir, fileName)
	} else {
		finalFilePath = filepath.Join(remoteFilePath, fileName)
	}

	// Create a temporary file for download
	tmpFile, err := os.Create(tmpFilePath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temporary file %s: %v", tmpFilePath, err)
	}
	defer tmpFile.Close()

	// Track download progress
	pw := &ProgressWriter{
		TotalBytes:   resp.ContentLength,
		StartTime:    time.Now(),
		LastLoggedAt: time.Now(),
	}
	var totalBytes int64
	totalBytes, err = io.Copy(io.MultiWriter(tmpFile, pw), resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("error during snapshot download: %v", err)
	}

	// Move the file from TMP to the final location
	err = os.Rename(tmpFilePath, finalFilePath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to move snapshot to %s: %v", finalFilePath, err)
	}

	// Print final progress
	speed := float64(pw.Downloaded) / 1024 / 1024 / time.Since(pw.StartTime).Seconds()
	log.Printf("Download completed: %s | Speed: %.2f MB/s", finalFilePath, speed)
	return finalFilePath, totalBytes, nil
}

func downloadSnapshot(rpcAddress string, config Config, snapshotType string, referenceSlot int) error {
	log.Printf("Downloading %s snapshot from %s", snapshotType, rpcAddress)

	// Define the TMP directory
	tmpDir := filepath.Join(config.SnapshotPath, "tmp")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create TMP directory: %v", err)
	}

	// Clean TMP directory
	err = cleanTmpDir(tmpDir)
	if err != nil {
		return fmt.Errorf("failed to clean TMP directory: %v", err)
	}

	// Construct URL
	var snapshotPath string
	if snapshotType == "incremental" {
		snapshotPath = "/incremental-snapshot.tar.bz2"
	} else {
		snapshotPath = "/snapshot.tar.bz2"
	}
	snapshotURL := fmt.Sprintf("%s%s", rpcAddress, snapshotPath)
	start := time.Now() // Start time for download

	// Download and handle the snapshot
	finalPath, sizeBytes, err := writeSnapshotToFile(snapshotURL, tmpDir, config.SnapshotPath, false)
	if err != nil {
		return fmt.Errorf("failed to download %s snapshot: %v", snapshotType, err)
	}

	// Calculate duration and format it
	duration := time.Since(start)
	seconds := int(duration.Seconds())
	if seconds < 60 {
		log.Printf("%s snapshot downloaded and saved to: %s | Time: %d seconds | Size: %.2f MB", snapshotType, finalPath, seconds, float64(sizeBytes)/(1024*1024))
	} else {
		log.Printf("%s snapshot downloaded and saved to: %s | Time: %d minutes, %d seconds | Size: %.2f MB",
			snapshotType, finalPath, seconds/60, seconds%60, float64(sizeBytes)/(1024*1024))
	}
	return nil
}

// ExtractFullSnapshotSlot parses a full snapshot file name to extract the slot.
func ExtractFullSnapshotSlot(fileName string) (int, error) {
	// Regex for full snapshot: snapshot-<slot>-<hash>.tar.zst
	fullSnapshotRegex := `snapshot-(\d+)-[a-zA-Z0-9]+\.tar\.zst`

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
	// Example regex: incremental-snapshot-<slotStart>-<slotEnd>-<hash>.tar.zst
	regex := regexp.MustCompile(`incremental-snapshot-(\d+)-(\d+)-[a-zA-Z0-9]+\.tar\.zst`)
	match := regex.FindStringSubmatch(fileName)

	if len(match) != 3 {
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
	// Find full snapshots in both destDir and remote directory
	fullSnapshotsDest, err := findSnapshotFiles(destDir, referenceSlot, threshold, true)
	if err != nil {
		return "", 0, err
	}

	remoteDir := filepath.Join(destDir, "remote")
	fullSnapshotsRemote, err := findSnapshotFiles(remoteDir, referenceSlot, threshold, true)
	if err != nil {
		return "", 0, err
	}

	// Combine snapshots from both directories
	allSnapshots := append(fullSnapshotsDest, fullSnapshotsRemote...)

	// Check for the most recent snapshot
	var mostRecent string
	var mostRecentSlot int

	for _, filePath := range allSnapshots {
		slot, err := ExtractFullSnapshotSlot(filePath)
		if err != nil {
			log.Printf("Skipping file %s: %v", filePath, err)
			continue
		}

		if slot > mostRecentSlot {
			mostRecent = filePath
			mostRecentSlot = slot
		}
	}

	if mostRecent == "" {
		return "", 0, fmt.Errorf("no recent full snapshot found")
	}

	log.Printf("Found recent full snapshot: %s (Slot: %d | Diff: %d)", mostRecent, mostRecentSlot, referenceSlot-mostRecentSlot)
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
