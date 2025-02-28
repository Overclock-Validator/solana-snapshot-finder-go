package main

import (
	"log"
	"os"
	"path/filepath"
)

// processSnapshots manages the snapshot download process
func processSnapshots(config Config) {
	// Create snapshot directory if it doesn't exist
	if err := os.MkdirAll(config.SnapshotPath, os.ModePerm); err != nil {
		log.Fatalf("Failed to create snapshot directory: %v", err)
	}

	// Create tmp directory if it doesn't exist
	tmpDir := filepath.Join(config.SnapshotPath, "tmp")
	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create TMP directory: %v", err)
	}

	// Create remote directory if it doesn't exist
	remoteDir := filepath.Join(config.SnapshotPath, "remote")
	if err := os.MkdirAll(remoteDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create remote directory: %v", err)
	}

	// Get reference slot from RPC
	referenceSlot, err := getReferenceSlot(config.RPCAddress)
	if err != nil {
		log.Fatalf("Failed to get reference slot: %v", err)
	}
	log.Printf("Reference slot: %d", referenceSlot)

	// Check if we need new snapshots
	needFull, needIncremental := manageSnapshots(config, referenceSlot)

	// If no snapshots are needed and genesis is present, exit successfully
	if !needFull && !needIncremental && isGenesisPresent(config.SnapshotPath) {
		log.Println("All snapshots are up to date. Exiting...")
		return
	}

	// Fetch RPC nodes if needed
	nodes := fetchRPCNodes(config)
	if len(nodes) == 0 {
		log.Fatal("No RPC nodes available.")
	}

	// Evaluate nodes
	results := evaluateNodesWithVersions(nodes, config, referenceSlot)
	summarizeResultsWithVersions(results)

	// Save good and slow nodes to file
	outputFile := filepath.Join(config.SnapshotPath, "nodes.json")
	dumpGoodAndSlowNodesToFile(results, outputFile)

	// Select best RPC
	bestRPC := selectBestRPC(results)
	if bestRPC == "" {
		log.Fatal("No suitable RPC found.")
	}
	log.Printf("Selected RPC: %s", bestRPC)

	// Download genesis snapshot if not present
	if !isGenesisPresent(config.SnapshotPath) {
		log.Println("Genesis snapshot not found. Downloading from fastest node...")
		if err := downloadGenesis(bestRPC, config.SnapshotPath); err != nil {
			log.Fatalf("Failed to download genesis snapshot: %v", err)
		}
		log.Println("Genesis snapshot downloaded successfully")
	}

	// Download full snapshot if needed
	if needFull {
		log.Println("Downloading full snapshot...")
		if err := downloadSnapshot(bestRPC, config, "snapshot-", referenceSlot); err != nil {
			log.Fatalf("Failed to download full snapshot: %v", err)
		}
		log.Println("Full snapshot downloaded successfully")
	}

	// Download incremental snapshot if needed
	if needIncremental {
		log.Println("Downloading incremental snapshot...")
		if err := downloadSnapshot(bestRPC, config, "incremental-", referenceSlot); err != nil {
			log.Fatalf("Failed to download incremental snapshot: %v", err)
		}
		log.Println("Incremental snapshot downloaded successfully")
	}

	// Clean up old snapshots
	if err := cleanupOldSnapshots(config.SnapshotPath, referenceSlot, config.FullThreshold, config.IncrementalThreshold); err != nil {
		log.Printf("Failed to clean up old snapshots: %v", err)
	}

	log.Println("All snapshots are up to date. Exiting...")
}
