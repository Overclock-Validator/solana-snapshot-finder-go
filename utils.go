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
		Timeout: 10 * time.Second, // Timeout for establishing connection
	}

	// Measure latency
	startTime := time.Now()
	resp, err := client.Get(url)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to fetch URL: %v", err)
	}
	defer resp.Body.Close()
	latency := time.Since(startTime).Milliseconds() // Latency in milliseconds

	// Measure download speed
	chunkSize := 81920 // Match the chunk size used in Python
	buffer := make([]byte, chunkSize)
	totalLoaded := int64(0)
	speeds := []float64{}
	lastTime := time.Now()

	for {
		if time.Since(startTime).Seconds() >= float64(measureTime) {
			break
		}

		n, err := resp.Body.Read(buffer)
		totalLoaded += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, float64(latency), fmt.Errorf("error reading response body: %v", err)
		}

		curTime := time.Now()
		elapsed := curTime.Sub(lastTime).Seconds()
		if elapsed >= 1 {
			speed := float64(totalLoaded) / elapsed // Bytes per second
			speeds = append(speeds, speed)
			lastTime = curTime
			totalLoaded = 0
		}
	}

	if len(speeds) == 0 {
		return 0, float64(latency), fmt.Errorf("no data collected during the measurement period")
	}

	medianSpeed := calculateMedian(speeds) / 1024 / 1024 // Convert to MB/s
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
	err := os.MkdirAll(destDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory %s: %v", destDir, err)
	}

	// Check if a full snapshot already exists
	existingSnapshot, err := findExistingSnapshot(destDir)
	if err != nil {
		return err
	}

	// Parse the RPC URL
	baseURL, err := url.Parse(rpcAddress)
	if err != nil {
		return fmt.Errorf("invalid RPC URL: %s", rpcAddress)
	}

	// Update the path based on whether a full snapshot exists
	var snapshotType string
	if existingSnapshot != "" {
		fmt.Printf("Existing full snapshot found: %s\n", existingSnapshot)
		baseURL.Path = "/incremental-snapshot.tar.bz2"
		snapshotType = "Incremental"
	} else {
		fmt.Println("No existing full snapshot found. Downloading full snapshot...")
		baseURL.Path = "/snapshot.tar.bz2"
		snapshotType = "Full"
	}

	// Use the updated URL
	url := baseURL.String()

	// Fetch the snapshot
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to fetch snapshot: %v", err)
	}
	defer resp.Body.Close()

	// Get the final URL after redirects
	finalURL := resp.Request.URL.String()

	// Extract the filename from the final URL
	fileName := filepath.Base(finalURL)
	destPath := filepath.Join(destDir, fileName)

	// Display snapshot name and type
	fmt.Printf("%s snapshot: %s\n", snapshotType, fileName)

	// Get the total content length from the response header
	totalBytes := resp.ContentLength
	if totalBytes <= 0 {
		return fmt.Errorf("unable to determine the file size")
	}

	// Create the output file
	file, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	// Track and display progress with speed
	pw := &ProgressWriter{
		TotalBytes:   totalBytes,
		StartTime:    time.Now(),
		LastLoggedAt: time.Now(),
	}
	_, err = io.Copy(io.MultiWriter(file, pw), resp.Body)
	if err != nil {
		return fmt.Errorf("error during download: %v", err)
	}

	// Ensure final progress message is printed
	speed := float64(pw.Downloaded) / 1024 / 1024 / time.Since(pw.StartTime).Seconds()
	fmt.Printf("\rDownloaded: %d / %d bytes (100%%) | Speed: %.2f MB/s\n", pw.Downloaded, pw.TotalBytes, speed)
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
