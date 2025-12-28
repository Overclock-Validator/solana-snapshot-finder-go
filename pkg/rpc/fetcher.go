package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/config"
)

var DEFAULT_HEADERS = map[string]string{
	"Content-Type": "application/json",
}

type RPCNode struct {
	Address  string
	Version  string
	IsStatic bool // true for static snapshot URLs (e.g., avorio.network)
}

// isStaticSnapshotURL detects if a URL is a direct snapshot endpoint
func isStaticSnapshotURL(url string) bool {
	// Check if URL looks like a direct snapshot path
	return strings.HasSuffix(url, "/") ||
		strings.Contains(url, "/snapshot") ||
		strings.Contains(url, "/incremental")
}

// parseWhitelist converts whitelist entries into RPCNode structs
func parseWhitelist(whitelist []string) []RPCNode {
	var nodes []RPCNode
	for _, entry := range whitelist {
		if entry == "" {
			continue
		}
		// Detect if it's a static snapshot URL or RPC endpoint
		if isStaticSnapshotURL(entry) {
			nodes = append(nodes, RPCNode{
				Address:  entry,
				Version:  "whitelist-static",
				IsStatic: true,
			})
		} else {
			// Treat as regular RPC endpoint
			nodes = append(nodes, RPCNode{
				Address:  entry,
				Version:  "whitelist-rpc",
				IsStatic: false,
			})
		}
	}
	return nodes
}

// getPublicNodes fetches nodes from the Solana cluster
func getPublicNodes(rpcAddress string, retries int) ([]RPCNode, error) {
	payload := []byte(`{"jsonrpc":"2.0", "id":1, "method":"getClusterNodes"}`)
	req, err := http.NewRequest("POST", rpcAddress, bytes.NewBuffer(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	// Add default headers
	for key, value := range DEFAULT_HEADERS {
		req.Header.Set(key, value)
	}

	client := &http.Client{Timeout: 15 * time.Second}

	var resp *http.Response
	for attempt := 1; attempt <= retries; attempt++ {
		resp, err = client.Do(req)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch cluster nodes after %d retries: %v", retries, err)
	}
	defer resp.Body.Close()

	var result struct {
		Result []struct {
			RPC     string `json:"rpc"`
			Gossip  string `json:"gossip"`
			Version string `json:"version"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode cluster nodes response: %v", err)
	}

	var nodes []RPCNode
	for _, node := range result.Result {
		if node.RPC != "" {
			nodes = append(nodes, RPCNode{
				Address:  node.RPC,
				Version:  node.Version,
				IsStatic: false,
			})
		}
	}
	return nodes, nil
}

// filterBlacklist removes blacklisted nodes
func filterBlacklist(nodes []RPCNode, blacklist []string) []RPCNode {
	if len(blacklist) == 0 {
		return nodes
	}

	var filtered []RPCNode
	for _, node := range nodes {
		isBlacklisted := false
		nodeIP := strings.Split(node.Address, ":")[0]
		for _, blocked := range blacklist {
			if nodeIP == blocked || strings.Contains(node.Address, blocked) {
				isBlacklisted = true
				break
			}
		}
		if !isBlacklisted {
			filtered = append(filtered, node)
		}
	}
	return filtered
}

func GetRPCNodes(rpcAddress string, retries int, blacklist []string, enableBlacklist bool, whitelist []string, whitelistMode string) ([]RPCNode, []string, error) {
	var sources []RPCNode

	// Validate whitelist_mode
	validModes := map[string]bool{"only": true, "additional": true, "disabled": true}
	if !validModes[whitelistMode] {
		Logf("Warning: Invalid whitelist_mode '%s', defaulting to 'additional'\n", whitelistMode)
		whitelistMode = "additional"
	}

	// Handle whitelist mode
	switch whitelistMode {
	case "only":
		// Only process whitelist
		sources = parseWhitelist(whitelist)
		if len(sources) == 0 {
			return nil, nil, fmt.Errorf("whitelist_mode is 'only' but whitelist is empty")
		}
		Logf("Using whitelist-only mode with %d entries\n", len(sources))

	case "additional":
		// Get public nodes + whitelist
		publicNodes, err := getPublicNodes(rpcAddress, retries)
		if err != nil {
			Logf("Warning: Failed to fetch public nodes: %v\n", err)
		}
		whitelistNodes := parseWhitelist(whitelist)
		sources = append(publicNodes, whitelistNodes...)
		if len(whitelistNodes) > 0 {
			Logf("Using %d public nodes + %d whitelist entries\n", len(publicNodes), len(whitelistNodes))
		}

	case "disabled":
		// Only get public nodes
		publicNodes, err := getPublicNodes(rpcAddress, retries)
		if err != nil {
			return nil, nil, err
		}
		sources = publicNodes
	}

	// Apply blacklist filtering if enabled
	if enableBlacklist && len(blacklist) > 0 {
		beforeCount := len(sources)
		sources = filterBlacklist(sources, blacklist)
		filtered := beforeCount - len(sources)
		if filtered > 0 {
			Logf("Blacklist filtered out %d nodes\n", filtered)
		}
	} else if !enableBlacklist && len(blacklist) > 0 {
		Logf("Blacklist is disabled (contains %d entries but not filtering)\n", len(blacklist))
	}

	// Extract addresses for compatibility
	addresses := []string{}
	for _, node := range sources {
		addresses = append(addresses, node.Address)
	}

	return sources, addresses, nil
}

func GetReferenceSlot(rpcAddress string) (int, error) {
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

	client := &http.Client{Timeout: 4 * time.Second}
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

// GetReferenceSlotFromMultiple queries multiple RPC endpoints and returns the highest slot
// This handles the case where some RPCs might be behind or unavailable
func GetReferenceSlotFromMultiple(rpcAddresses []string) (int, string, error) {
	if len(rpcAddresses) == 0 {
		return 0, "", fmt.Errorf("no RPC addresses provided")
	}

	var highestSlot int
	var bestRPC string
	var lastErr error
	var successCount int

	for _, rpc := range rpcAddresses {
		slot, err := GetReferenceSlot(rpc)
		if err != nil {
			Logf("RPC %s failed to get slot: %v\n", rpc, err)
			lastErr = err
			continue
		}
		successCount++
		Logf("RPC %s returned slot: %d\n", rpc, slot)
		if slot > highestSlot {
			highestSlot = slot
			bestRPC = rpc
		}
	}

	if successCount == 0 {
		return 0, "", fmt.Errorf("all RPC endpoints failed, last error: %v", lastErr)
	}

	if successCount < len(rpcAddresses) {
		Logf("Warning: %d/%d RPC endpoints responded\n", successCount, len(rpcAddresses))
	}

	Logf("Using reference slot %d from %s (highest among %d sources)\n", highestSlot, bestRPC, successCount)
	return highestSlot, bestRPC, nil
}

// FetchClusterNodes fetches cluster nodes (validators/snapshot sources) via RPC
// preferredRPC is tried first if provided (e.g., the RPC that worked for getSlot)
func FetchClusterNodes(cfg config.Config, preferredRPC string) []RPCNode {
	var nodes []RPCNode
	var err error

	// Build ordered list of RPCs to try, with preferred first
	rpcAddresses := make([]string, 0, len(cfg.RPCAddresses))
	if preferredRPC != "" {
		rpcAddresses = append(rpcAddresses, preferredRPC)
	}
	for _, addr := range cfg.RPCAddresses {
		if addr != preferredRPC {
			rpcAddresses = append(rpcAddresses, addr)
		}
	}

	// Try each RPC address until one succeeds (no nested retries - getPublicNodes already retries)
	for _, rpcAddr := range rpcAddresses {
		nodes, _, err = GetRPCNodes(
			rpcAddr,
			cfg.NumOfRetries,
			cfg.Blacklist,
			cfg.EnableBlacklist,
			cfg.Whitelist,
			cfg.WhitelistMode,
		)
		if err == nil && len(nodes) > 0 {
			Logf("Fetched %d cluster nodes from %s\n", len(nodes), rpcAddr)
			return nodes
		}

		Logf("Failed to fetch cluster nodes from %s: %v\n", rpcAddr, err)
	}

	if err != nil {
		Logf("FATAL: Failed to fetch cluster nodes from any endpoint: %v\n", err)
		os.Exit(1)
	} else if len(nodes) == 0 {
		Logf("FATAL: No cluster nodes found from any endpoint.\n")
		os.Exit(1)
	}

	return nil // Should not reach here
}
