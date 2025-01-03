package main

import (
	"log"
)

// Main function
func main() {
	config := ParseConfig()
	log.Println("Starting Solana Snapshot Finder...")

	// Get the current slot from the default RPC endpoint
	defaultSlot := getDefaultSlot(config)

	// List all snapshot files in the snapshot directory
	snapshotFiles := getSnapshotFiles(config)

	// Handle snapshots and decide if downloading is necessary
	latestSnapshot := handleSnapshots(snapshotFiles, config, defaultSlot)

	if latestSnapshot != nil && (defaultSlot-latestSnapshot.SlotEnd) <= 500 {
		log.Printf("Latest snapshot is within 500 slots of the default slot. No need to download a new snapshot.")
		return
	}

	// Fetch RPC nodes
	rpcs := fetchRPCNodes(config)

	// Evaluate nodes
	results := evaluateNodes(rpcs, config, defaultSlot)

	// Summarize and decide
	summarizeResults(results)

	// Select and download the best RPC snapshot
	bestRPC := selectBestRPC(results)
	if bestRPC != "" {
		downloadSnapshot(bestRPC, config)
	} else {
		log.Println("No suitable RPC found.")
	}
}
