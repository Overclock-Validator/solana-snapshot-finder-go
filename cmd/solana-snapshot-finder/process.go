package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/config"
	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/rpc"
	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/snapshot"
)

// processSnapshots manages the snapshot download process
func processSnapshots(cfg config.Config) {
	// Create snapshot directory if it doesn't exist
	if err := os.MkdirAll(cfg.SnapshotPath, os.ModePerm); err != nil {
		log.Fatalf("Failed to create snapshot directory: %v", err)
	}

	// Create tmp directory if it doesn't exist
	tmpDir := filepath.Join(cfg.SnapshotPath, "tmp")
	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create TMP directory: %v", err)
	}

	// Create remote directory if it doesn't exist
	remoteDir := filepath.Join(cfg.SnapshotPath, "remote")
	if err := os.MkdirAll(remoteDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create remote directory: %v", err)
	}

	// Get reference slot from RPC
	referenceSlot, err := rpc.GetReferenceSlot(cfg.RPCAddress)
	if err != nil {
		log.Fatalf("Failed to get reference slot: %v", err)
	}
	log.Printf("Reference slot: %d", referenceSlot)

	// Check if we need new snapshots
	needFull, needIncremental := snapshot.ManageSnapshots(cfg, referenceSlot)

	// If no snapshots are needed and genesis is present, exit successfully
	if !needFull && !needIncremental && snapshot.IsGenesisPresent(cfg.SnapshotPath) {
		log.Println("All snapshots are up to date. Exiting...")
		return
	}

	// Fetch RPC nodes if needed
	nodes := rpc.FetchRPCNodes(cfg)
	if len(nodes) == 0 {
		log.Fatal("No RPC nodes available.")
	}

	// Evaluate nodes
	results := rpc.EvaluateNodesWithVersions(nodes, cfg, referenceSlot)
	rpc.SummarizeResultsWithVersions(results)

	// Save good and slow nodes to file
	outputFile := filepath.Join(cfg.SnapshotPath, "nodes.json")
	rpc.DumpGoodAndSlowNodesToFile(results, outputFile)

	// Select best RPC
	bestRPC := rpc.SelectBestRPC(results)
	if bestRPC == "" {
		log.Fatal("No suitable RPC found.")
	}
	log.Printf("Selected RPC: %s", bestRPC)

	// Download genesis snapshot if not present
	if !snapshot.IsGenesisPresent(cfg.SnapshotPath) {
		log.Println("Genesis snapshot not found. Downloading from fastest node...")
		if err := snapshot.DownloadGenesis(bestRPC, cfg.SnapshotPath); err != nil {
			log.Fatalf("Failed to download genesis snapshot: %v", err)
		}
		log.Println("Genesis snapshot downloaded successfully")

		// Untar the genesis file
		if err := snapshot.UntarGenesis(cfg.SnapshotPath); err != nil {
			log.Fatalf("Failed to untar genesis snapshot: %v", err)
		}
		log.Println("Genesis snapshot untarred successfully")
	}

	// Download full snapshot if needed
	if needFull {
		log.Println("Downloading full snapshot...")
		if _, err := snapshot.DownloadSnapshot(bestRPC, cfg, "snapshot-", referenceSlot); err != nil {
			log.Fatalf("Failed to download full snapshot: %v", err)
		}
		log.Println("Full snapshot downloaded successfully")
	}

	// Download incremental snapshot if needed
	if needIncremental {
		log.Println("Downloading incremental snapshot...")
		if _, err := snapshot.DownloadSnapshot(bestRPC, cfg, "incremental-", referenceSlot); err != nil {
			log.Fatalf("Failed to download incremental snapshot: %v", err)
		}
		log.Println("Incremental snapshot downloaded successfully")
	}

	// Clean up old snapshots
	if err := snapshot.CleanupOldSnapshots(cfg.SnapshotPath, referenceSlot, cfg.FullThreshold, cfg.IncrementalThreshold); err != nil {
		log.Printf("Failed to clean up old snapshots: %v", err)
	}

	log.Println("All snapshots are up to date. Exiting...")
}
