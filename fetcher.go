package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

var DEFAULT_HEADERS = map[string]string{
	"Content-Type": "application/json",
}

func GetRPCNodes(rpcAddress string, retries int) ([]string, error) {
	payload := []byte(`{"jsonrpc":"2.0", "id":1, "method":"getClusterNodes"}`)
	req, err := http.NewRequest("POST", rpcAddress, bytes.NewBuffer(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	// Add default headers
	for key, value := range DEFAULT_HEADERS {
		req.Header.Set(key, value)
	}

	client := &http.Client{Timeout: 15 * time.Second} // Adjust timeout as needed

	var resp *http.Response
	for attempt := 1; attempt <= retries; attempt++ {
		resp, err = client.Do(req)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second) // Add delay between retries
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch RPC nodes after %d retries: %v", retries, err)
	}
	defer resp.Body.Close()

	var result struct {
		Result []struct {
			RPC string `json:"rpc"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode RPC nodes response: %v", err)
	}

	rpcs := []string{}
	for _, node := range result.Result {
		if node.RPC != "" {
			rpcs = append(rpcs, node.RPC)
		}
	}
	return rpcs, nil
}

func GetSlot(rpcAddress string) (int, error) {
	payload := map[string]interface{}{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "getSlot",
		"params":  []interface{}{},
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", rpcAddress, bytes.NewBuffer(body))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch slot: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response body: %v", err)
	}

	var result struct {
		JSONRPC string `json:"jsonrpc"`
		Result  int    `json:"result"`
		ID      int    `json:"id"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return 0, fmt.Errorf("failed to parse response: %v", err)
	}

	return result.Result, nil
}

// Fetches the current slot from the default endpoint
func getDefaultSlot(config Config) int {
	defaultSlot, err := GetSlot(config.RPCAddress)
	if err != nil {
		log.Fatalf("Failed to get slot from default endpoint: %v", err)
	}
	log.Printf("Current slot from default endpoint: %d", defaultSlot)
	return defaultSlot
}

// Fetches RPC nodes
func fetchRPCNodes(config Config) []string {
	rpcs, err := GetRPCNodes(config.RPCAddress, 3) // Retry up to 3 times
	if err != nil {
		log.Fatalf("Failed to fetch RPC nodes: %v", err)
	}
	log.Printf("Found %d nodes. Starting to evaluate their speeds and latencies...", len(rpcs))
	return rpcs
}

// Selects the best RPC from the evaluated nodes
func selectBestRPC(results []struct {
	rpc     string
	speed   float64
	latency float64
	slot    int
	diff    int
	status  string
}) string {
	var bestRPC string
	var bestSpeed float64

	for _, result := range results {
		if result.status == "good" && result.speed > bestSpeed {
			bestSpeed = result.speed
			bestRPC = result.rpc
		}
	}
	return bestRPC
}
