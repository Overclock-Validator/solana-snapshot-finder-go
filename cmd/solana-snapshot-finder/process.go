package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/config"
	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/rpc"
	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/snapshot"
)

// promptUserChoice prompts the user for a yes/no response
// Returns true for yes, false for no
func promptUserChoice(message string) bool {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("%s [y/n]: ", message)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(strings.ToLower(input))
		switch input {
		case "y", "yes":
			return true
		case "n", "no":
			return false
		default:
			fmt.Println("Please enter 'y' or 'n'")
		}
	}
}

// processSnapshots manages the snapshot download process with cancellation support
func processSnapshots(ctx context.Context, cfg config.Config, state *DownloadState) {
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

	// Log disk space at start
	snapshot.LogDiskSpace(cfg.SnapshotPath)

	// Get reference slot from multiple RPC endpoints (uses highest slot)
	referenceSlot, workingRPC, err := rpc.GetReferenceSlotFromMultiple(cfg.RPCAddresses)
	if err != nil {
		log.Fatalf("Failed to get reference slot: %v", err)
	}

	// Check expiration status of current snapshots and warn if approaching expiration
	expirationStatus := snapshot.CheckExpirationStatus(cfg.SnapshotPath, referenceSlot, cfg.FullThreshold, cfg.SafetyMarginSlots)
	if expirationStatus.InWarningWindow {
		// Calculate approximate time until expiration (assuming ~400ms per slot)
		minutesRemaining := float64(expirationStatus.SlotsRemaining) * 0.4 / 60.0
		log.Printf("WARNING: Current snapshots approaching expiration (%d slots remaining, ~%.1f minutes)",
			expirationStatus.SlotsRemaining, minutesRemaining)
		log.Printf("  If the download takes longer than this, the incremental snapshot base will change.")
		log.Printf("  You can press Ctrl+C at any time to cancel and try again later.")
	}

	// Check if we need new snapshots
	needFull, needIncremental := snapshot.ManageSnapshots(cfg, referenceSlot)

	// If no snapshots are needed and genesis is present, exit successfully
	if !needFull && !needIncremental && snapshot.IsGenesisPresent(cfg.SnapshotPath) {
		log.Println("All snapshots are up to date. Exiting...")
		return
	}

	// Check for cancellation before proceeding
	if ctx.Err() != nil {
		log.Println("Operation cancelled before node discovery")
		return
	}

	// Fetch cluster nodes (validators that serve snapshots)
	// Use the RPC that worked for getSlot first (faster than trying failed ones)
	nodes := rpc.FetchClusterNodes(cfg, workingRPC)
	if len(nodes) == 0 {
		log.Fatal("No cluster nodes available.")
	}

	// Evaluate nodes with TCP precheck and stats collection
	results, stats := rpc.EvaluateNodesWithVersionsAndStats(nodes, cfg, referenceSlot)

	// Save good and slow nodes to file
	outputFile := filepath.Join(cfg.SnapshotPath, "nodes.json")
	rpc.DumpGoodAndSlowNodesToFile(results, outputFile)

	// Select best snapshot source using two-stage speed sampling with stats tracking
	sortedNodes, rankedNodes := rpc.SortBestNodesWithStats(results, cfg, stats, referenceSlot)

	// Print comprehensive node discovery report
	filterCfg := rpc.FilterConfig{
		MaxRTTMs:        cfg.MaxRTTMs,
		FullThreshold:   cfg.FullThreshold,
		IncThreshold:    cfg.IncrementalThreshold,
		MinVersion:      cfg.MinNodeVersion,
		AllowedVersions: cfg.AllowedNodeVersions,
	}
	stats.PrintReport(filterCfg)

	if len(sortedNodes) == 0 {
		log.Fatal("No suitable snapshot sources found.")
	}
	bestNode := sortedNodes[0]
	_ = rankedNodes // Will be used for incremental fallback search

	// Check for cancellation before downloads
	if ctx.Err() != nil {
		log.Println("Operation cancelled before downloads")
		return
	}

	// Download genesis snapshot if not present
	if !snapshot.IsGenesisPresent(cfg.SnapshotPath) {
		log.Println("Genesis snapshot not found. Downloading from fastest node...")
		if err := snapshot.DownloadGenesis(bestNode, cfg.SnapshotPath); err != nil {
			log.Fatalf("Failed to download genesis snapshot: %v", err)
		}
		log.Println("Genesis snapshot downloaded successfully")

		// Untar the genesis file
		if err := snapshot.UntarGenesis(cfg.SnapshotPath); err != nil {
			log.Fatalf("Failed to untar genesis snapshot: %v", err)
		}
		log.Println("Genesis snapshot untarred successfully")
	}

	// Track the full snapshot slot for incremental matching
	var fullSnapshotSlot int64
	var safetyIncrementalPath string // Path to incremental downloaded before full (for cleanup)

	// Download full snapshot if needed
	if needFull {
		// Get the remote full snapshot slot from the best node
		var remoteFullSlot int64
		if len(rankedNodes) > 0 {
			remoteFullSlot = rankedNodes[0].Result.FullSlot
			log.Printf("Remote full snapshot slot: %d", remoteFullSlot)
		}

		// Download safety incremental BEFORE full snapshot if enabled
		if cfg.DownloadIncrementalFirst && remoteFullSlot > 0 {
			log.Println("Safety mode: downloading incremental before full snapshot...")
			log.Println("This ensures a valid incremental exists if full download exceeds expiry window")

			// Find a node with an incremental matching the remote full slot
			matchInfo, err := rpc.FindMatchingIncremental(rankedNodes, remoteFullSlot, 5000)
			if err != nil {
				log.Printf("Warning: No matching safety incremental found: %v", err)
				log.Println("Continuing with full snapshot download...")
			} else {
				log.Printf("Found safety incremental at %s (base=%d, end=%d)",
					matchInfo.NodeRPC, matchInfo.BaseSlot, matchInfo.EndSlot)
				safetyPath, err := snapshot.DownloadSnapshotWithContext(ctx, matchInfo.NodeRPC, cfg, "incremental-", referenceSlot, state)
				if err != nil {
					if ctx.Err() != nil {
						log.Println("Safety incremental download cancelled")
						return
					}
					log.Printf("Warning: Failed to download safety incremental: %v", err)
				} else {
					safetyIncrementalPath = safetyPath
					log.Println("Safety incremental downloaded successfully")
				}
			}
		}

		log.Println("Downloading full snapshot...")
		downloadedPath, err := snapshot.DownloadSnapshotWithContext(ctx, bestNode, cfg, "snapshot-", referenceSlot, state)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Full snapshot download cancelled")
				return
			}
			log.Fatalf("Failed to download full snapshot: %v", err)
		}
		log.Println("Full snapshot downloaded successfully")

		// Extract the slot from the downloaded full snapshot
		slot, err := snapshot.ExtractFullSnapshotSlot(downloadedPath)
		if err != nil {
			log.Printf("Warning: Could not extract slot from downloaded snapshot: %v", err)
		} else {
			fullSnapshotSlot = int64(slot)
			log.Printf("Downloaded full snapshot slot: %d", fullSnapshotSlot)
		}
	} else {
		// Get the slot from the existing full snapshot
		existingPath, existingSlot, err := snapshot.FindRecentFullSnapshot(cfg.SnapshotPath, referenceSlot, 0)
		if err == nil {
			fullSnapshotSlot = int64(existingSlot)
			log.Printf("Using existing full snapshot: %s (slot %d)", filepath.Base(existingPath), existingSlot)
		}
	}

	// Download incremental snapshot if needed
	if needIncremental {
		log.Println("Checking for matching incremental snapshot...")

		// Refresh incremental info from best node to ensure we have current data
		incInfo, err := rpc.RefreshIncrementalInfo(bestNode, 5000)
		var incrementalSource string
		var incrementalMatches bool

		if err != nil {
			log.Printf("Warning: Could not get incremental info from best node: %v", err)
		} else if incInfo.BaseSlot == fullSnapshotSlot {
			log.Printf("Best node has matching incremental (base=%d, end=%d)", incInfo.BaseSlot, incInfo.EndSlot)
			incrementalSource = bestNode
			incrementalMatches = true
		} else {
			log.Printf("Best node's incremental base (%d) doesn't match full snapshot slot (%d)",
				incInfo.BaseSlot, fullSnapshotSlot)
		}

		// If best node doesn't have a matching incremental, search other Stage 2 nodes
		if !incrementalMatches && len(rankedNodes) > 0 {
			log.Println("Searching other Stage 2 nodes for matching incremental...")
			matchInfo, err := rpc.FindMatchingIncremental(rankedNodes, fullSnapshotSlot, 5000)
			if err != nil {
				log.Printf("Warning: No matching incremental found: %v", err)
			} else {
				incrementalSource = matchInfo.NodeRPC
				incrementalMatches = true
				log.Printf("Found matching incremental at %s (base=%d, end=%d)",
					matchInfo.NodeRPC, matchInfo.BaseSlot, matchInfo.EndSlot)
			}
		}

		// If still no matching incremental, prompt user
		if !incrementalMatches {
			log.Println("")
			log.Println("========================================")
			log.Println("WARNING: No matching incremental snapshot found!")
			log.Printf("Full snapshot slot: %d", fullSnapshotSlot)
			log.Println("Available incrementals have different base slots.")
			log.Println("")
			log.Println("Options:")
			log.Println("  1. Continue without incremental (validator will take longer to start)")
			log.Println("  2. Quit and try again later (incrementals update frequently)")
			log.Println("========================================")

			if !promptUserChoice("Continue without incremental snapshot?") {
				log.Println("User chose to quit. Try again in a few minutes when new incrementals are available.")
				return
			}
			log.Println("Continuing without incremental snapshot...")
		} else {
			// Download incremental from the source that has the matching one
			log.Printf("Downloading incremental snapshot from %s...", incrementalSource)
			freshIncrementalPath, err := snapshot.DownloadSnapshotWithContext(ctx, incrementalSource, cfg, "incremental-", referenceSlot, state)
			if err != nil {
				if ctx.Err() != nil {
					log.Println("Incremental snapshot download cancelled")
					return
				}
				log.Fatalf("Failed to download incremental snapshot: %v", err)
			}
			log.Println("Incremental snapshot downloaded successfully")

			// Clean up the safety incremental if we downloaded one and now have a fresh one
			if safetyIncrementalPath != "" && freshIncrementalPath != safetyIncrementalPath {
				if err := os.Remove(safetyIncrementalPath); err != nil {
					log.Printf("Warning: Could not delete old safety incremental: %v", err)
				} else {
					log.Printf("Cleaned up old safety incremental: %s", filepath.Base(safetyIncrementalPath))
				}
			}
		}
	}

	// Clean up old snapshots based on age (if enabled)
	if cfg.DeleteOldSnapshots {
		if err := snapshot.CleanupOldSnapshots(cfg.SnapshotPath, referenceSlot, cfg.FullThreshold, cfg.IncrementalThreshold); err != nil {
			log.Printf("Failed to clean up old snapshots: %v", err)
		}
	}

	// Enforce retention limit asynchronously (don't block)
	go func() {
		snapshot.EnforceRetentionLimit(cfg.SnapshotPath, cfg.MaxFullSnapshots)
	}()

	// Log final disk space
	snapshot.LogDiskSpace(cfg.SnapshotPath)

	log.Println("All snapshots are up to date. Exiting...")
}
