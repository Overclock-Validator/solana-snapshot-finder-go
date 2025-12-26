package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/config"
)

var version string

// DownloadState tracks in-progress downloads for cleanup on cancellation
type DownloadState struct {
	TempFiles []string   // Temporary files to clean up on cancellation
	mu        sync.Mutex // Protects TempFiles
}

// Global download state for signal handler access
var downloadState = &DownloadState{}

// AddTempFile registers a temporary file for cleanup on cancellation
func (ds *DownloadState) AddTempFile(path string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.TempFiles = append(ds.TempFiles, path)
}

// RemoveTempFile removes a file from the cleanup list (e.g., after successful completion)
func (ds *DownloadState) RemoveTempFile(path string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	for i, f := range ds.TempFiles {
		if f == path {
			ds.TempFiles = append(ds.TempFiles[:i], ds.TempFiles[i+1:]...)
			return
		}
	}
}

// Cleanup removes all registered temporary files
func (ds *DownloadState) Cleanup() {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	for _, tempFile := range ds.TempFiles {
		if err := os.Remove(tempFile); err == nil {
			log.Printf("Cleaned up incomplete download: %s", tempFile)
		}
	}
	ds.TempFiles = nil
}

func main() {
	// Parse command-line arguments
	configPath := flag.String("config", "./config.yaml", "Path to the configuration file directory")
	versionFlag := flag.Bool("version", false, "Show version information")
	flag.Parse()

	// If version flag is passed, print the version and exit
	if *versionFlag {
		if version == "" {
			version = "unknown" // In case version is not set, fallback to 'unknown'
		}
		fmt.Printf("Solana Snapshot Finder, Version: %s\n", version)
		os.Exit(0)
	}

	// Load the configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Println("Starting Solana Snapshot Finder...")
	log.Printf("Loaded Config: %+v", cfg)

	// Create cancellation context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("\nReceived %v, cancelling operations and cleaning up...", sig)
		cancel()

		// Clean up any incomplete downloads
		downloadState.Cleanup()

		os.Exit(130) // Standard exit code for SIGINT
	}()

	// Manage snapshots, including fetching the reference slot internally
	processSnapshots(ctx, cfg, downloadState)
}
