package rpc

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
)

func MeasureSpeed(url string, measureTime int) (float64, float64, error) {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	startTime := time.Now()
	resp, err := client.Get(url)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to fetch URL: %v", err)
	}
	defer resp.Body.Close()
	latency := time.Since(startTime).Milliseconds()

	buffer := make([]byte, 81920)
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

		elapsed := time.Since(lastTime).Seconds()
		if elapsed >= 1 {
			speed := float64(totalLoaded) / elapsed
			speeds = append(speeds, speed)
			lastTime = time.Now()
			totalLoaded = 0
		}
	}

	if len(speeds) == 0 {
		return 0, float64(latency), fmt.Errorf("no data collected during the measurement period")
	}

	medianSpeed := calculateMedian(speeds) / (1024 * 1024)
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

func EvaluateNodesWithVersions(nodes []RPCNode, cfg config.Config, defaultSlot int) []struct {
	rpc     string
	speed   float64
	latency float64
	slot    int
	diff    int
	version string
	status  string
} {
	var wg sync.WaitGroup
	results := make(chan struct {
		rpc     string
		speed   float64
		latency float64
		slot    int
		diff    int
		version string
		status  string
	}, len(nodes))
	done := make(chan bool)

	// Create a semaphore to limit concurrent goroutines
	sem := make(chan struct{}, cfg.WorkerCount)

	var processedNodes int32
	var goodNodes int32
	var slowNodes int32
	var badNodes int32

	ticker := time.NewTicker(5 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				processed := atomic.LoadInt32(&processedNodes)
				good := atomic.LoadInt32(&goodNodes)
				slow := atomic.LoadInt32(&slowNodes)
				bad := atomic.LoadInt32(&badNodes)
				log.Printf("Progress: %d/%d nodes processed (%.1f%%) | Good: %d, Slow: %d, Bad: %d",
					processed, len(nodes), float64(processed)/float64(len(nodes))*100, good, slow, bad)
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()

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

		if processed%50 == 0 {
			log.Printf("Milestone reached: %d/%d nodes processed", processed, len(nodes))
		}
	}

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

	for _, node := range nodes {
		wg.Add(1)
		go func(node RPCNode) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

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

			speed, latency, err := MeasureSpeed(snapshotURL, cfg.SleepBeforeRetry/2)
			if err != nil {
				appendResult(node, rpc, speed, latency, 0, 0, "slow")
				return
			}

			slot, err := GetReferenceSlot(rpc)
			if err != nil {
				appendResult(node, rpc, speed, latency, 0, 0, "slow")
				return
			}

			diff := defaultSlot - slot
			status := "slow"
			if speed >= float64(cfg.MinDownloadSpeed) && latency <= float64(cfg.MaxLatency) && diff <= 100 {
				status = "good"
			} else if speed == 0 || latency > float64(cfg.MaxLatency) {
				status = "bad"
			}

			appendResult(node, rpc, speed, latency, slot, diff, status)
		}(node)
	}

	wg.Wait()
	done <- true
	close(results)

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

	sort.Slice(evaluatedResults, func(i, j int) bool {
		return evaluatedResults[i].speed > evaluatedResults[j].speed
	})

	log.Printf("Node evaluation complete: %d/%d nodes processed | Good: %d, Slow: %d, Bad: %d",
		atomic.LoadInt32(&processedNodes), len(nodes),
		atomic.LoadInt32(&goodNodes),
		atomic.LoadInt32(&slowNodes),
		atomic.LoadInt32(&badNodes))

	return evaluatedResults
}

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

	log.Printf("Node evaluation complete. Total nodes: %d | Good: %d | Slow: %d | Bad: %d",
		totalNodes, goodNodes, slowNodes, badNodes)

	log.Println("List of good nodes:")
	for _, result := range results {
		if result.status == "good" {
			log.Printf("Node: %s | Speed: %.2f MB/s | Latency: %.2f ms | Slot: %d | Diff: %d | Version: %s",
				result.rpc, result.speed, result.latency, result.slot, result.diff, result.version)
		}
	}
}

func dumpGoodAndSlowNodesToFile(results []struct {
	rpc     string
	speed   float64
	latency float64
	slot    int
	diff    int
	version string
	status  string
}, outputFile string) {
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

	file, err := os.Create(outputFile)
	if err != nil {
		log.Printf("Error creating output file: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(filteredNodes); err != nil {
		log.Printf("Error writing to JSON file: %v", err)
		return
	}

	log.Printf("Good and slow nodes saved to %s", outputFile)
}

func SummarizeResultsWithVersions(results []struct {
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

	log.Printf("Node evaluation complete. Total nodes: %d | Good: %d | Slow: %d | Bad: %d",
		totalNodes, goodNodes, slowNodes, badNodes)

	log.Println("List of good nodes:")
	for _, result := range results {
		if result.status == "good" {
			log.Printf("Node: %s | Speed: %.2f MB/s | Latency: %.2f ms | Slot: %d | Diff: %d | Version: %s",
				result.rpc, result.speed, result.latency, result.slot, result.diff, result.version)
		}
	}
}

func DumpGoodAndSlowNodesToFile(results []struct {
	rpc     string
	speed   float64
	latency float64
	slot    int
	diff    int
	version string
	status  string
}, outputFile string) {
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

	file, err := os.Create(outputFile)
	if err != nil {
		log.Printf("Error creating output file: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(filteredNodes); err != nil {
		log.Printf("Error writing to JSON file: %v", err)
		return
	}

	log.Printf("Good and slow nodes saved to %s", outputFile)
}

func SelectBestRPC(results []struct {
	rpc     string
	speed   float64
	latency float64
	slot    int
	diff    int
	version string
	status  string
}) string {
	var bestGoodNode struct {
		rpc   string
		speed float64
	}
	var bestSlowNode struct {
		rpc   string
		speed float64
	}

	for _, result := range results {
		if result.status == "good" && result.speed > bestGoodNode.speed {
			bestGoodNode = struct {
				rpc   string
				speed float64
			}{rpc: result.rpc, speed: result.speed}
		}
		if result.status == "slow" && result.speed > bestSlowNode.speed {
			bestSlowNode = struct {
				rpc   string
				speed float64
			}{rpc: result.rpc, speed: result.speed}
		}
	}

	// Prioritize good nodes; fallback to the fastest slow node if no good nodes are available
	if bestGoodNode.rpc != "" {
		return bestGoodNode.rpc
	}
	if bestSlowNode.rpc != "" {
		log.Printf("No good nodes found. Falling back to the fastest slow node: %s with speed %.2f MB/s", bestSlowNode.rpc, bestSlowNode.speed)
		return bestSlowNode.rpc
	}

	log.Println("No suitable RPC nodes found.")
	return ""
}
