package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// ExtractSnapshotSlots parses snapshot file names to extract the relevant slots.
func ExtractSnapshotSlots(fileName string) (int, int, error) {
	// Regex for full snapshot: snapshot-<slot>-<hash>.tar.zst
	fullSnapshotRegex := `snapshot-(\d+)-[a-zA-Z0-9]+\.tar\.zst`
	// Regex for incremental snapshot: incremental-snapshot-<slot1>-<slot2>-<hash>.tar.zst
	incrementalSnapshotRegex := `incremental-snapshot-(\d+)-(\d+)-[a-zA-Z0-9]+\.tar\.zst`

	if match := regexp.MustCompile(fullSnapshotRegex).FindStringSubmatch(fileName); match != nil {
		slot, err := strconv.Atoi(match[1])
		if err != nil {
			return 0, 0, fmt.Errorf("failed to parse slot from full snapshot: %v", err)
		}
		return slot, 0, nil
	}

	if match := regexp.MustCompile(incrementalSnapshotRegex).FindStringSubmatch(fileName); match != nil {
		slot1, err1 := strconv.Atoi(match[1])
		slot2, err2 := strconv.Atoi(match[2])
		if err1 != nil || err2 != nil {
			return 0, 0, fmt.Errorf("failed to parse slots from incremental snapshot: %v", err1)
		}
		return slot1, slot2, nil
	}

	return 0, 0, fmt.Errorf("file name does not match snapshot patterns")
}

func main() {
	config := ParseConfig()
	log.Println("Starting Solana Snapshot Finder...")

	// Get the current slot from the default RPC endpoint
	defaultSlot, err := GetSlot(config.RPCAddress)
	if err != nil {
		log.Fatalf("Failed to get slot from default endpoint: %v", err)
	}
	log.Printf("Current slot from default endpoint: %d", defaultSlot)

	// List all snapshot files in the snapshot directory
	files, err := os.ReadDir(config.SnapshotPath)
	if err != nil {
		log.Fatalf("Failed to read snapshot directory: %v", err)
	}

	// Parse snapshots and determine the highest incremental snapshot
	var snapshotFiles []string
	for _, file := range files {
		snapshotFiles = append(snapshotFiles, file.Name())
	}

	// Parse and handle incremental snapshots
	incrementalSnapshots, err := ParseIncrementalSnapshots(snapshotFiles)
	if err != nil {
		log.Fatalf("Failed to parse incremental snapshots: %v", err)
	}

	highestSnapshot, err := FindHighestAndTrimIncrementals(incrementalSnapshots, 5, config.SnapshotPath)
	if err != nil {
		log.Printf("No incremental snapshots found: %v", err)
	} else {
		log.Printf("Highest incremental snapshot: %s | SlotStart: %d | SlotEnd: %d", highestSnapshot.FileName, highestSnapshot.SlotStart, highestSnapshot.SlotEnd)
		log.Printf("Highest incremental snapshot is %d slots behind the current slot.", defaultSlot-highestSnapshot.SlotEnd)
	}

	// Fetch RPC nodes
	rpcs, err := GetRPCNodes(config.RPCAddress, 3) // Retry up to 3 times
	if err != nil {
		log.Fatalf("Failed to fetch RPC nodes: %v", err)
	}

	// Summary statistics
	totalNodes := len(rpcs)
	goodNodes := []struct {
		rpc     string
		speed   float64
		latency float64
		slot    int
		diff    int
	}{}
	slowNodes := 0
	badNodes := 0

	log.Printf("Found %d nodes. Starting to evaluate their speeds and latencies...", totalNodes)

	var wg sync.WaitGroup
	results := make(chan struct {
		rpc     string
		speed   float64
		latency float64
		slot    int
		diff    int
		status  string // "good", "slow", or "bad"
	}, len(rpcs))

	for _, rpc := range rpcs {
		wg.Add(1)
		go func(rpc string) {
			defer wg.Done()

			// Ensure the rpc has a scheme
			if !strings.HasPrefix(rpc, "http://") && !strings.HasPrefix(rpc, "https://") {
				rpc = "http://" + rpc
			}

			// Parse the RPC URL
			baseURL, err := url.Parse(rpc)
			if err != nil {
				results <- struct {
					rpc     string
					speed   float64
					latency float64
					slot    int
					diff    int
					status  string
				}{rpc, 0, 0, 0, 0, "bad"}
				return
			}

			// Construct the snapshot URL
			baseURL.Path = "/snapshot.tar.bz2"
			snapshotURL := baseURL.String()

			speed, latency, err := MeasureSpeed(snapshotURL, config.SleepBeforeRetry)
			if err != nil {
				results <- struct {
					rpc     string
					speed   float64
					latency float64
					slot    int
					diff    int
					status  string
				}{rpc, speed, latency, 0, 0, "slow"}
				return
			}

			// Fetch the slot from the RPC node
			slot, err := GetSlot(rpc)
			if err != nil {
				results <- struct {
					rpc     string
					speed   float64
					latency float64
					slot    int
					diff    int
					status  string
				}{rpc, speed, latency, 0, 0, "slow"}
				return
			}

			// Calculate the slot difference
			diff := defaultSlot - slot

			// Check thresholds
			if speed >= float64(config.MinDownloadSpeed) && latency <= float64(config.MaxLatency) && diff <= 100 {
				results <- struct {
					rpc     string
					speed   float64
					latency float64
					slot    int
					diff    int
					status  string
				}{rpc, speed, latency, slot, diff, "good"}
			} else {
				results <- struct {
					rpc     string
					speed   float64
					latency float64
					slot    int
					diff    int
					status  string
				}{rpc, speed, latency, slot, diff, "slow"}
			}
		}(rpc)
	}

	wg.Wait()
	close(results)

	// Aggregate results
	var bestRPC string
	var bestSpeed float64
	for result := range results {
		switch result.status {
		case "good":
			goodNodes = append(goodNodes, struct {
				rpc     string
				speed   float64
				latency float64
				slot    int
				diff    int
			}{result.rpc, result.speed, result.latency, result.slot, result.diff})
			if result.speed > bestSpeed {
				bestSpeed = result.speed
				bestRPC = result.rpc
			}
		case "slow":
			slowNodes++
		case "bad":
			badNodes++
		}
	}

	// Print the summary
	log.Printf("Node evaluation complete. Total nodes: %d | Good: %d | Slow: %d | Bad: %d", totalNodes, len(goodNodes), slowNodes, badNodes)

	// Print all good nodes with slot difference
	if len(goodNodes) > 0 {
		log.Println("List of good nodes:")
		for _, node := range goodNodes {
			log.Printf("Node: %s | Speed: %.2f MB/s | Latency: %.2f ms | Slot: %d | Diff: %d", node.rpc, node.speed, node.latency, node.slot, node.diff)
		}
	}

	// Start downloading from the best RPC
	if bestRPC != "" {
		log.Printf("Best RPC: %s with speed %.2f MB/s", bestRPC, bestSpeed)
		baseURL, _ := url.Parse(bestRPC)
		baseURL.Path = "/snapshot.tar.bz2"

		err := DownloadSnapshot(baseURL.String(), config.SnapshotPath)
		if err != nil {
			log.Fatalf("Failed to download snapshot: %v", err)
		}
		log.Println("Download complete.")
	} else {
		log.Println("No suitable RPC found.")
	}
}
