package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/config"
)

// Snapshot filename patterns
var (
	fullSnapshotRe = regexp.MustCompile(`^snapshot-(\d+)-([a-zA-Z0-9]+)\.tar\.(bz2|zst|gz|xz)$`)
	incSnapshotRe  = regexp.MustCompile(`^incremental-snapshot-(\d+)-(\d+)-([a-zA-Z0-9]+)\.tar\.(bz2|zst|gz|xz)$`)

	fullSnapshotPaths = []string{
		"/snapshot.tar.zst",
		"/snapshot.tar.bz2",
		"/snapshot.tar.gz",
		"/snapshot.tar.xz",
	}

	incSnapshotPaths = []string{
		"/incremental-snapshot.tar.zst",
		"/incremental-snapshot.tar.bz2",
		"/incremental-snapshot.tar.gz",
		"/incremental-snapshot.tar.xz",
	}
)

// Default TCP precheck timeout in milliseconds
// 500ms is generous enough for home internet while still filtering dead hosts quickly
const DefaultTCPTimeoutMs = 500

// HTTP client configurations for probing
// Timeouts increased for home internet compatibility (vs data center)
var (
	probeTransport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   500 * time.Millisecond, // Increased from 250ms for home internet
			KeepAlive: 10 * time.Second,
		}).DialContext,
		ResponseHeaderTimeout: 600 * time.Millisecond, // Increased from 300ms
		TLSHandshakeTimeout:   600 * time.Millisecond, // Increased from 300ms
		DisableKeepAlives:     false,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       10 * time.Second,
	}

	noRedirectClient = &http.Client{
		Timeout: 1500 * time.Millisecond, // Increased from 900ms
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: probeTransport,
	}

	// Download / sampling transport (more forgiving timeouts)
	downloadTransport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		DisableKeepAlives:     false,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       30 * time.Second,
	}
)

// TCPPrecheck performs a quick TCP connect to verify host is reachable
// Returns true if connection succeeds, false otherwise
func TCPPrecheck(host string, timeoutMs int) bool {
	if timeoutMs <= 0 {
		timeoutMs = DefaultTCPTimeoutMs
	}
	// Ensure host has port
	if !strings.Contains(host, ":") {
		host = host + ":80"
	}
	d := net.Dialer{Timeout: time.Duration(timeoutMs) * time.Millisecond}
	conn, err := d.Dial("tcp", host)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// extractHostPort extracts host:port from an address, handling URLs
func extractHostPort(addr string) string {
	// Remove protocol prefix if present
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	// Remove path if present
	if idx := strings.Index(addr, "/"); idx > 0 {
		addr = addr[:idx]
	}
	// Add default port if missing
	if !strings.Contains(addr, ":") {
		addr = addr + ":80"
	}
	return addr
}

// parseFullSnapshotSlot extracts the slot number from a full snapshot filename
func parseFullSnapshotSlot(filename string) (int64, error) {
	m := fullSnapshotRe.FindStringSubmatch(filename)
	if m == nil {
		return 0, fmt.Errorf("invalid full snapshot filename: %s", filename)
	}
	slot, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse slot from filename: %v", err)
	}
	return slot, nil
}

// parseIncrementalSnapshotSlots extracts base and inc slot numbers from incremental snapshot filename
func parseIncrementalSnapshotSlots(filename string) (baseSlot int64, incSlot int64, err error) {
	m := incSnapshotRe.FindStringSubmatch(filename)
	if m == nil {
		return 0, 0, fmt.Errorf("invalid incremental snapshot filename: %s", filename)
	}
	baseSlot, err = strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse base slot: %v", err)
	}
	incSlot, err = strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse inc slot: %v", err)
	}
	return baseSlot, incSlot, nil
}

// stripQuery removes query parameters from URL
func stripQuery(u string) string {
	if i := strings.Index(u, "?"); i >= 0 {
		return u[:i]
	}
	return u
}

// resolveSnapshotURL performs a HEAD request and handles redirects, returning final URL and filename
func resolveSnapshotURL(ctx context.Context, baseURLStr, path string) (finalURL string, filename string, err error) {
	base, err := url.Parse(baseURLStr)
	if err != nil {
		return "", "", err
	}
	target, err := url.Parse(path)
	if err != nil {
		return "", "", err
	}
	fullURL := base.ResolveReference(target).String()

	req, err := http.NewRequestWithContext(ctx, "HEAD", fullURL, nil)
	if err != nil {
		return "", "", err
	}

	resp, err := noRedirectClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	// Handle redirect
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		loc := resp.Header.Get("Location")
		if loc == "" {
			return "", "", fmt.Errorf("empty location header")
		}
		locURL, err := url.Parse(loc)
		if err != nil {
			return "", "", err
		}
		reqURL, _ := url.Parse(fullURL)
		resolvedURL := reqURL.ResolveReference(locURL).String()

		// Extract filename from resolved URL
		filename = filepath.Base(stripQuery(resolvedURL))
		return resolvedURL, filename, nil
	}

	// Generic endpoint without redirect
	if resp.StatusCode == 200 {
		return "", "", fmt.Errorf("200 OK without redirect (generic endpoint)")
	}

	return "", "", fmt.Errorf("status=%s", resp.Status)
}

// probeSnapshotEndpoints tries multiple snapshot paths and returns the first successful one
func probeSnapshotEndpoints(ctx context.Context, baseURL string, paths []string) (finalURL string, filename string, err error) {
	var lastErr error
	for _, path := range paths {
		u, f, err := resolveSnapshotURL(ctx, baseURL, path)
		if err == nil && u != "" && f != "" {
			return u, f, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no endpoint succeeded")
	}
	return "", "", lastErr
}

// NodeResult represents the evaluation result for a single node
type NodeResult struct {
	RPC       string
	Latency   float64
	FullSlot  int64
	IncBase   int64
	IncSlot   int64
	FullURL   string
	IncURL    string
	SlotDiff  int64
	Version   string
	HasInc    bool
	IncUsable bool
}

// FastSample holds Stage 1 (fast triage) results
type FastSample struct {
	MedianMBs float64 // Median download speed in MB/s
	MinMBs    float64 // Minimum download speed in MB/s
	Timeout   bool    // True if sampling timed out
	Err       string  // Error message if any
}

// Stage2Sample holds Stage 2 (accurate confirm) results
type Stage2Sample struct {
	AvgMBs   float64 // Average download speed in MB/s
	MinMBs   float64 // Minimum download speed in MB/s
	MaxMBs   float64 // Maximum download speed in MB/s
	ConnSec  float64 // Connection time in seconds (time to first byte)
	Timeout  bool    // True if sampling timed out
	ConnFail bool    // True if connection failed (distinct from timeout)
	Err      string  // Error message if any
}

// RankedNode combines node evaluation with speed samples
type RankedNode struct {
	Result NodeResult   // Initial evaluation (latency, slots, URLs)
	S1     FastSample   // Stage 1 speed sample
	S2     Stage2Sample // Stage 2 speed sample
}

// evaluateStaticSnapshotURL evaluates static whitelist URLs without slot verification
func evaluateStaticSnapshotURL(baseURL string, cfg config.Config) NodeResult {
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	result := NodeResult{RPC: baseURL}

	// For static URLs, just probe the endpoints without strict slot verification
	start := time.Now()

	// Try to probe incremental
	incURL, incFilename, incErr := probeSnapshotEndpoints(ctx, baseURL, incSnapshotPaths)
	result.Latency = float64(time.Since(start).Milliseconds())

	if incErr == nil && incFilename != "" {
		result.IncURL = incURL
		result.HasInc = true
		// Try to parse slots if filename has them, but don't fail if not
		incBase, incSlot, err := parseIncrementalSnapshotSlots(incFilename)
		if err == nil {
			result.IncBase = incBase
			result.IncSlot = incSlot
		}
		// Mark as usable for whitelisted sources (trusted)
		result.IncUsable = true
	}

	// Try to probe full snapshot
	fullURL, fullFilename, fullErr := probeSnapshotEndpoints(ctx, baseURL, fullSnapshotPaths)
	if fullErr == nil && fullFilename != "" {
		result.FullURL = fullURL
		// Try to parse slot if available
		fullSlot, err := parseFullSnapshotSlot(fullFilename)
		if err == nil {
			result.FullSlot = fullSlot
		} else {
			// Set a sentinel value to indicate "has full but no slot info"
			result.FullSlot = 1 // Non-zero to indicate snapshot exists
		}
	}

	// Don't calculate SlotDiff for static URLs (or set to 0)
	result.SlotDiff = 0

	return result
}

// EvaluateNodesWithVersionsAndStats evaluates nodes and returns results along with detailed stats
func EvaluateNodesWithVersionsAndStats(nodes []RPCNode, cfg config.Config, defaultSlot int) ([]NodeResult, *ProbeStats) {
	var wg sync.WaitGroup
	results := make(chan NodeResult, len(nodes))
	done := make(chan bool)

	// Initialize probe stats
	stats := NewProbeStats()
	stats.TotalNodes = int64(len(nodes))

	// Create a semaphore to limit concurrent goroutines
	sem := make(chan struct{}, cfg.WorkerCount)

	var processedNodes int32

	ticker := time.NewTicker(5 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				processed := atomic.LoadInt32(&processedNodes)
				withSnap := atomic.LoadInt64(&stats.HasAnySnapshot)
				usableInc := atomic.LoadInt64(&stats.WithUsableInc)
				Logf("Progress: %d/%d nodes processed (%.1f%%) | With snapshots: %d | With usable incremental: %d",
					processed, len(nodes), float64(processed)/float64(len(nodes))*100, withSnap, usableInc)
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()

	appendResult := func(node RPCNode, result NodeResult, hasSnapshot bool, hasInc bool, incUsable bool) {
		result.Version = node.Version
		results <- result

		atomic.AddInt32(&processedNodes, 1)

		// Record stats
		stats.RecordVersion(node.Version)
		stats.RecordProbeResult(hasSnapshot, false, false)
		stats.RecordIncremental(hasInc, incUsable)
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

			baseURL := rpc

			// Handle static snapshot URLs differently (skip TCP precheck)
			if node.IsStatic {
				result := evaluateStaticSnapshotURL(baseURL, cfg)
				hasSnapshot := result.FullSlot > 0 || result.HasInc
				appendResult(node, result, hasSnapshot, result.HasInc, result.IncUsable)
				return
			}

			// TCP Precheck: Quick elimination of dead hosts
			hostPort := extractHostPort(rpc)
			tcpTimeout := cfg.TCPTimeoutMs
			if tcpTimeout <= 0 {
				tcpTimeout = DefaultTCPTimeoutMs
			}
			if !TCPPrecheck(hostPort, tcpTimeout) {
				stats.RecordTCPFailed()
				atomic.AddInt32(&processedNodes, 1)
				stats.RecordProbeResult(false, false, true)
				return // Skip HTTP probing for unreachable hosts
			}

			// Regular node evaluation logic
			ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
			defer cancel()

			result := NodeResult{RPC: rpc}

			// Probe incremental first (also gets RTT)
			start := time.Now()
			incURL, incFilename, incErr := probeSnapshotEndpoints(ctx, baseURL, incSnapshotPaths)
			result.Latency = float64(time.Since(start).Milliseconds())

			// Record RTT
			stats.RecordRTT(result.Latency)

			if incErr == nil && incFilename != "" {
				incBase, incSlot, err := parseIncrementalSnapshotSlots(incFilename)
				if err == nil && incSlot > 0 {
					result.IncURL = incURL
					result.IncBase = incBase
					result.IncSlot = incSlot
					result.HasInc = true

					// Record incremental distance
					incDist := int64(defaultSlot) - incSlot
					stats.RecordIncrementalDistance(incDist)
				}
			}

			// Probe full snapshot
			fullURL, fullFilename, fullErr := probeSnapshotEndpoints(ctx, baseURL, fullSnapshotPaths)
			if fullErr == nil && fullFilename != "" {
				fullSlot, err := parseFullSnapshotSlot(fullFilename)
				if err == nil && fullSlot > 0 {
					result.FullURL = fullURL
					result.FullSlot = fullSlot
				}
			}

			// Check incremental usability
			// Requires: incremental base matches full snapshot slot (so they work together)
			if result.HasInc && result.IncBase == result.FullSlot && result.FullSlot > 0 {
				incDist := int64(defaultSlot) - result.IncSlot
				absIncDist := incDist
				if absIncDist < 0 {
					absIncDist = -absIncDist
				}
				if absIncDist < int64(cfg.IncrementalThreshold) {
					result.IncUsable = true
				}
			}

			// Calculate slot diff based on best available snapshot
			targetSlot := result.FullSlot
			if result.HasInc && result.IncSlot > 0 {
				targetSlot = result.IncSlot
			}
			if targetSlot > 0 {
				result.SlotDiff = int64(defaultSlot) - targetSlot
			}

			hasSnapshot := result.FullSlot > 0 || result.HasInc
			appendResult(node, result, hasSnapshot, result.HasInc, result.IncUsable)
		}(node)
	}

	wg.Wait()
	done <- true
	close(results)

	var evaluatedResults []NodeResult
	for result := range results {
		evaluatedResults = append(evaluatedResults, result)
	}

	// Sort by slot freshness first, then latency
	sort.Slice(evaluatedResults, func(i, j int) bool {
		// Nodes without snapshots go to the end
		if evaluatedResults[i].FullSlot == 0 && evaluatedResults[j].FullSlot == 0 {
			return evaluatedResults[i].Latency < evaluatedResults[j].Latency
		}
		if evaluatedResults[i].FullSlot == 0 {
			return false
		}
		if evaluatedResults[j].FullSlot == 0 {
			return true
		}

		// Sort by slot freshness
		if evaluatedResults[i].SlotDiff != evaluatedResults[j].SlotDiff {
			return evaluatedResults[i].SlotDiff < evaluatedResults[j].SlotDiff
		}
		// If slots are equally fresh, use latency as tiebreaker
		return evaluatedResults[i].Latency < evaluatedResults[j].Latency
	})

	return evaluatedResults, stats
}

// EvaluateNodesWithVersions is the original function signature for backward compatibility
func EvaluateNodesWithVersions(nodes []RPCNode, cfg config.Config, defaultSlot int) []NodeResult {
	results, _ := EvaluateNodesWithVersionsAndStats(nodes, cfg, defaultSlot)
	return results
}

func SummarizeResultsWithVersions(results []NodeResult) {
	totalNodes := len(results)
	withSnapshots := 0
	withInc := 0
	withUsableInc := 0

	for _, result := range results {
		if result.FullSlot > 0 || result.HasInc {
			withSnapshots++
		}
		if result.HasInc {
			withInc++
		}
		if result.IncUsable {
			withUsableInc++
		}
	}

	Logf("Node evaluation complete. Total: %d | With snapshots: %d | With incremental: %d | With usable incremental: %d",
		totalNodes, withSnapshots, withInc, withUsableInc)

	Logln("Top nodes by snapshot freshness:")
	count := 0
	for _, result := range results {
		if result.FullSlot > 0 && count < 10 {
			Logf("Node: %s | Latency: %.0fms | FullSlot: %d | IncSlot: %d | SlotDiff: %d | IncUsable: %t | Version: %s",
				result.RPC, result.Latency, result.FullSlot, result.IncSlot, result.SlotDiff, result.IncUsable, result.Version)
			count++
		}
	}
}

func DumpGoodAndSlowNodesToFile(results []NodeResult, outputFile string) {
	var filteredNodes []struct {
		RPC       string  `json:"rpc"`
		Latency   float64 `json:"latency"`
		FullSlot  int64   `json:"full_slot"`
		IncBase   int64   `json:"inc_base"`
		IncSlot   int64   `json:"inc_slot"`
		SlotDiff  int64   `json:"slot_diff"`
		Version   string  `json:"version"`
		HasInc    bool    `json:"has_inc"`
		IncUsable bool    `json:"inc_usable"`
	}

	for _, result := range results {
		// Only include nodes with snapshots
		if result.FullSlot > 0 || result.HasInc {
			filteredNodes = append(filteredNodes, struct {
				RPC       string  `json:"rpc"`
				Latency   float64 `json:"latency"`
				FullSlot  int64   `json:"full_slot"`
				IncBase   int64   `json:"inc_base"`
				IncSlot   int64   `json:"inc_slot"`
				SlotDiff  int64   `json:"slot_diff"`
				Version   string  `json:"version"`
				HasInc    bool    `json:"has_inc"`
				IncUsable bool    `json:"inc_usable"`
			}{
				RPC:       result.RPC,
				Latency:   result.Latency,
				FullSlot:  result.FullSlot,
				IncBase:   result.IncBase,
				IncSlot:   result.IncSlot,
				SlotDiff:  result.SlotDiff,
				Version:   result.Version,
				HasInc:    result.HasInc,
				IncUsable: result.IncUsable,
			})
		}
	}

	file, err := os.Create(outputFile)
	if err != nil {
		Logf("Error creating output file: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(filteredNodes); err != nil {
		Logf("Error writing to JSON file: %v", err)
		return
	}

	Logf("Nodes with snapshots saved to %s", outputFile)
}

// Helper functions for speed sampling

// isTimeoutErr checks if an error is a network timeout error
func isTimeoutErr(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// classifyFastErr classifies errors from Stage 1 sampling
func classifyFastErr(err error) FastSample {
	if err == nil {
		return FastSample{}
	}
	if isTimeoutErr(err) || errors.Is(err, context.Canceled) {
		return FastSample{Timeout: true, Err: err.Error()}
	}
	return FastSample{Err: err.Error()}
}

// classifyStage2Err classifies errors from Stage 2 sampling
// connPhase indicates if error occurred during connection (before first byte)
func classifyStage2Err(err error, connPhase bool) Stage2Sample {
	if err == nil {
		return Stage2Sample{}
	}
	if isTimeoutErr(err) || errors.Is(err, context.DeadlineExceeded) {
		return Stage2Sample{Timeout: true, Err: err.Error()}
	}
	if errors.Is(err, context.Canceled) {
		return Stage2Sample{Timeout: true, Err: err.Error()}
	}
	// Connection phase errors are marked as ConnFail
	if connPhase {
		return Stage2Sample{ConnFail: true, Err: err.Error()}
	}
	return Stage2Sample{Err: err.Error()}
}

// mean calculates the mean of a float64 slice
func mean(a []float64) float64 {
	if len(a) == 0 {
		return 0
	}
	var s float64
	for _, v := range a {
		s += v
	}
	return s / float64(len(a))
}

// minmax finds the minimum and maximum values in a float64 slice
func minmax(a []float64) (float64, float64) {
	if len(a) == 0 {
		return 0, 0
	}
	minv, maxv := a[0], a[0]
	for _, v := range a[1:] {
		if v < minv {
			minv = v
		}
		if v > maxv {
			maxv = v
		}
	}
	return minv, maxv
}

// parseVersion extracts major.minor.patch from version string
// Handles formats like "2.2.0", "v2.2.0", "2.2", etc.
func parseVersion(version string) (major, minor, patch int) {
	// Remove 'v' prefix if present
	version = strings.TrimPrefix(version, "v")

	// Split by dots
	parts := strings.Split(version, ".")
	if len(parts) >= 1 {
		major, _ = strconv.Atoi(parts[0])
	}
	if len(parts) >= 2 {
		minor, _ = strconv.Atoi(parts[1])
	}
	if len(parts) >= 3 {
		patch, _ = strconv.Atoi(parts[2])
	}
	return
}

// compareVersions returns -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
func compareVersions(v1, v2 string) int {
	maj1, min1, pat1 := parseVersion(v1)
	maj2, min2, pat2 := parseVersion(v2)

	if maj1 != maj2 {
		if maj1 < maj2 {
			return -1
		}
		return 1
	}
	if min1 != min2 {
		if min1 < min2 {
			return -1
		}
		return 1
	}
	if pat1 != pat2 {
		if pat1 < pat2 {
			return -1
		}
		return 1
	}
	return 0
}

// versionMatchesFilter checks if version matches filtering criteria
func versionMatchesFilter(version string, minVersion string, allowedVersions []string) bool {
	// If allowed versions list is specified, check exact match
	if len(allowedVersions) > 0 {
		for _, allowed := range allowedVersions {
			if version == allowed || strings.HasPrefix(version, allowed+".") {
				return true
			}
		}
		return false
	}

	// Otherwise use minimum version check
	if minVersion == "" {
		return true // No filtering
	}

	return compareVersions(version, minVersion) >= 0
}

// Stage 1: Fast Triage Sampling

// fastDiscardSample performs a quick download speed test using HTTP Range requests
func fastDiscardSample(ctx context.Context, urlStr string, cfg config.Config) FastSample {
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Stage1TimeoutMS)*time.Millisecond)
	defer cancel()

	warm := cfg.Stage1WarmKiB * 1024
	win := cfg.Stage1WindowKiB * 1024
	windows := cfg.Stage1Windows
	if windows <= 0 {
		windows = 2
	}
	total := warm + int64(windows)*win
	if total <= 0 {
		return FastSample{Err: "bad sample sizing"}
	}

	req, err := http.NewRequestWithContext(timeoutCtx, "GET", urlStr, nil)
	if err != nil {
		return FastSample{Err: err.Error()}
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", total-1))

	client := &http.Client{Transport: downloadTransport}
	resp, err := client.Do(req)
	if err != nil {
		return classifyFastErr(err)
	}
	defer resp.Body.Close()

	// Accept 206 Partial Content or 200 OK
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return FastSample{Err: fmt.Sprintf("status=%s", resp.Status)}
	}

	br := bufio.NewReaderSize(resp.Body, 64*1024)

	// Discard warmup bytes
	if _, err := io.CopyN(io.Discard, br, warm); err != nil {
		return classifyFastErr(err)
	}

	// Time each window
	var bps []float64
	for i := 0; i < windows; i++ {
		start := time.Now()
		if _, err := io.CopyN(io.Discard, br, win); err != nil {
			return classifyFastErr(err)
		}
		secs := time.Since(start).Seconds()
		if secs > 0 {
			bps = append(bps, float64(win)/secs)
		}
	}

	if len(bps) == 0 {
		return FastSample{Err: "no samples"}
	}

	// Calculate median and min
	sort.Float64s(bps)
	median := bps[len(bps)/2] / 1_000_000.0 // Convert to MB/s
	minSpeed := bps[0] / 1_000_000.0        // First element after sort is min
	return FastSample{MedianMBs: median, MinMBs: minSpeed}
}

// stage1Triage performs Stage 1 fast triage on all candidates
func stage1Triage(ctx context.Context, candidates []NodeResult, cfg config.Config) []RankedNode {
	type item struct {
		result NodeResult
		sample FastSample
	}
	items := make([]item, 0, len(candidates))

	// Auto-calculate concurrency if not specified
	concurrency := cfg.Stage1Concurrency
	if concurrency <= 0 {
		// Default: 5 concurrent downloads (balance between speed and not saturating connection)
		concurrency = 5
	}

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex

	if !cfg.Quiet {
		Logf("  Stage 1 config: conc=%d warmup=%dKiB window=%dKiB×%d timeout=%dms",
			concurrency, cfg.Stage1WarmKiB, cfg.Stage1WindowKiB, cfg.Stage1Windows, cfg.Stage1TimeoutMS)
		Logf("  Stage 1 input: %d candidates", len(candidates))
	}

	var tested int64
	for _, cand := range candidates {
		cand := cand
		testURL := cand.FullURL
		if testURL == "" {
			testURL = cand.IncURL
		}
		if testURL == "" {
			continue
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			sample := fastDiscardSample(ctx, testURL, cfg)
			atomic.AddInt64(&tested, 1)

			mu.Lock()
			items = append(items, item{result: cand, sample: sample})
			mu.Unlock()
		}()
	}
	wg.Wait()

	// Count errors and timeouts
	var timeouts, errorsOnly int
	for _, it := range items {
		if it.sample.Timeout {
			timeouts++
		}
		if it.sample.Err != "" && !it.sample.Timeout {
			errorsOnly++
		}
	}

	// Keep only successful samples
	var ranked []RankedNode
	for _, it := range items {
		if it.sample.Timeout || it.sample.Err != "" || it.sample.MedianMBs <= 0 {
			continue
		}
		ranked = append(ranked, RankedNode{
			Result: it.result,
			S1:     it.sample,
		})
	}

	// Sort by Stage 1 minimum speed (highest min first) - min speed is more representative of sustained throughput
	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].S1.MinMBs > ranked[j].S1.MinMBs
	})

	if !cfg.Quiet {
		Logf("  Stage 1 result: tested=%d passed=%d timeouts=%d errors=%d",
			tested, len(ranked), timeouts, errorsOnly)
	}

	return ranked
}

// Stage 2: Accurate Confirmation Sampling

// stage2Sample performs accurate download speed measurement
// Warmup period discards data, then measures for the full measurement period.
// Internally uses 500ms buckets to track min/max for stability detection (collapse guard).
func stage2Sample(ctx context.Context, urlStr string, cfg config.Config) Stage2Sample {
	// Time-based configuration
	warm := time.Duration(cfg.Stage2WarmSec) * time.Second
	if warm < 0 {
		warm = 0
	}
	measure := time.Duration(cfg.Stage2MeasureSec) * time.Second
	if measure <= 0 {
		measure = 3 * time.Second
	}

	// Internal bucket size for min/max tracking (not user-configurable)
	const bucketDuration = 500 * time.Millisecond

	// Auto-calculate timeout: warm + measure + 5s for connection overhead
	timeout := warm + measure + 5*time.Second
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Cap requested bytes to 64 MiB (more than enough for short measurement)
	capBytes := int64(64 * 1024 * 1024)

	req, err := http.NewRequestWithContext(timeoutCtx, "GET", urlStr, nil)
	if err != nil {
		return Stage2Sample{Err: err.Error()}
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", capBytes-1))

	// Track connection time (time to first byte)
	connStart := time.Now()
	client := &http.Client{Transport: downloadTransport}
	resp, err := client.Do(req)
	connTime := time.Since(connStart).Seconds()
	if err != nil {
		return classifyStage2Err(err, true) // Connection phase error
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return Stage2Sample{ConnSec: connTime, ConnFail: true, Err: fmt.Sprintf("status=%s", resp.Status)}
	}

	br := bufio.NewReaderSize(resp.Body, 128*1024)
	readBuf := make([]byte, 64*1024)

	// Warmup phase - discard data
	if warm > 0 {
		wUntil := time.Now().Add(warm)
		for time.Now().Before(wUntil) && timeoutCtx.Err() == nil {
			_, err := br.Read(readBuf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				s := classifyStage2Err(err, false)
				s.ConnSec = connTime
				return s
			}
		}
	}

	// Measurement phase - track total bytes and per-bucket speeds for min/max
	measureStart := time.Now()
	measureEnd := measureStart.Add(measure)
	var totalBytes int64
	var bucketSpeeds []float64

	bucketStart := measureStart
	var bucketBytes int64

	for time.Now().Before(measureEnd) && timeoutCtx.Err() == nil {
		n, err := br.Read(readBuf)
		if n > 0 {
			totalBytes += int64(n)
			bucketBytes += int64(n)
		}

		// Check if current bucket is complete
		if time.Since(bucketStart) >= bucketDuration {
			bucketSecs := time.Since(bucketStart).Seconds()
			if bucketSecs > 0 {
				bucketSpeeds = append(bucketSpeeds, (float64(bucketBytes)/bucketSecs)/1_000_000.0)
			}
			bucketStart = time.Now()
			bucketBytes = 0
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			s := classifyStage2Err(err, false)
			s.ConnSec = connTime
			return s
		}
	}

	// Add final partial bucket if any data was read
	if bucketBytes > 0 {
		bucketSecs := time.Since(bucketStart).Seconds()
		if bucketSecs > 0 {
			bucketSpeeds = append(bucketSpeeds, (float64(bucketBytes)/bucketSecs)/1_000_000.0)
		}
	}

	// Calculate overall average from total bytes / total time
	totalSecs := time.Since(measureStart).Seconds()
	if totalSecs <= 0 {
		totalSecs = measure.Seconds()
	}
	avgMBs := (float64(totalBytes) / totalSecs) / 1_000_000.0

	// Get min/max from bucket speeds for stability detection
	var minMBs, maxMBs float64
	if len(bucketSpeeds) > 0 {
		minMBs, maxMBs = minmax(bucketSpeeds)
	} else {
		minMBs, maxMBs = avgMBs, avgMBs
	}

	return Stage2Sample{AvgMBs: avgMBs, MinMBs: minMBs, MaxMBs: maxMBs, ConnSec: connTime}
}

// stage2Confirm performs Stage 2 confirmation on top candidates from Stage 1
// currentSlot is used to calculate fresh slot diff for logging
func stage2Confirm(ctx context.Context, ranked []RankedNode, cfg config.Config, currentSlot int) []RankedNode {
	// Take top K from stage 1
	top := ranked
	if cfg.Stage2TopK > 0 && len(top) > cfg.Stage2TopK {
		top = top[:cfg.Stage2TopK]
	}

	if !cfg.Quiet {
		Logf("  Stage 2 config: warm=%ds measure=%ds minRatio=%.2f minAbs=%.1fMB/s",
			cfg.Stage2WarmSec, cfg.Stage2MeasureSec, cfg.Stage2MinRatio, cfg.Stage2MinAbsMBs)
		Logf("  Stage 2 input: %d candidates (top %d from Stage 1, tested sequentially)", len(top), cfg.Stage2TopK)
	}

	// Run tests sequentially (concurrency=1) for accurate speed measurements on home internet
	sem := make(chan struct{}, 1)
	var wg sync.WaitGroup
	var mu sync.Mutex

	out := make([]RankedNode, 0, len(top))
	var tested, collapsed, timeouts, connFails int64

	for _, r := range top {
		r := r
		urlStr := r.Result.FullURL
		if urlStr == "" {
			urlStr = r.Result.IncURL
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			s2 := stage2Sample(ctx, urlStr, cfg)
			atomic.AddInt64(&tested, 1)

			// Track failure types
			if s2.Timeout {
				atomic.AddInt64(&timeouts, 1)
				return
			}
			if s2.ConnFail {
				atomic.AddInt64(&connFails, 1)
				return
			}
			if s2.Err != "" || s2.AvgMBs <= 0 {
				return
			}

			// Collapse guard: reject nodes with unstable speed (ratio check)
			if s2.MinMBs > 0 && s2.MinMBs < cfg.Stage2MinRatio*s2.AvgMBs {
				atomic.AddInt64(&collapsed, 1)
				return
			}

			// Absolute minimum speed check
			if cfg.Stage2MinAbsMBs > 0 && s2.AvgMBs < cfg.Stage2MinAbsMBs {
				atomic.AddInt64(&collapsed, 1)
				return
			}

			mu.Lock()
			r.S2 = s2
			out = append(out, r)
			mu.Unlock()
		}()
	}
	wg.Wait()

	otherErrors := int(tested) - len(out) - int(collapsed) - int(timeouts) - int(connFails)
	if !cfg.Quiet {
		Logf("  Stage 2 result: tested=%d passed=%d collapsed=%d timeouts=%d conn_fails=%d errors=%d",
			tested, len(out), collapsed, timeouts, connFails, otherErrors)
	}

	// Sort by Stage 2 minimum speed (highest min first) - min speed is more representative of sustained throughput
	sort.Slice(out, func(i, j int) bool {
		return out[i].S2.MinMBs > out[j].S2.MinMBs
	})

	// Log detailed info for each passing node (helps identify problematic sources)
	if len(out) > 0 && !cfg.Quiet {
		Logf("  Stage 2 candidates (sorted by min speed):")
		for i, r := range out {
			// Calculate distance from current slot to full snapshot slot
			fullsnapDist := int64(currentSlot) - r.Result.FullSlot
			Logf("    %d. %s | v%s | %.1f MB/s (min=%.1f max=%.1f) | rtt=%.0fms | conn=%.1fs | fullsnap_dist=%d",
				i+1, r.Result.RPC, r.Result.Version, r.S2.AvgMBs, r.S2.MinMBs, r.S2.MaxMBs, r.Result.Latency, r.S2.ConnSec, fullsnapDist)
		}
	}

	return out
}

// SelectBestNode selects the best snapshot source from evaluated nodes
func SelectBestNode(results []NodeResult) string {
	// Find the node with the freshest snapshot and usable incremental
	for _, result := range results {
		if result.IncUsable && result.FullSlot > 0 {
			return result.RPC
		}
	}

	// Fallback: find the node with the freshest snapshot (even without usable incremental)
	for _, result := range results {
		if result.FullSlot > 0 {
			Logf("No nodes with usable incremental found. Using node with fresh full snapshot: %s (slotDiff: %d, latency: %.0f ms)",
				result.RPC, result.SlotDiff, result.Latency)
			return result.RPC
		}
	}

	Logln("No suitable snapshot sources found.")
	return ""
}

// SortBestNodes filters and ranks snapshot sources by speed
// Returns addresses of nodes sorted by download speed (fastest first)
// referenceSlot is used for calculating current slot diff in Stage 2 output
func SortBestNodes(results []NodeResult, cfg config.Config, referenceSlot int) []string {
	if len(results) == 0 {
		Logln("No nodes passed initial evaluation")
		return nil
	}

	// Stats for detailed logging
	initialCount := len(results)
	var withSnapshots, afterFullAge, afterIncAge, afterRTT, afterVersion int

	// Count nodes with snapshots
	for _, r := range results {
		if r.FullSlot > 0 || r.HasInc {
			withSnapshots++
		}
	}

	Logf("=== Snapshot Source Selection Pipeline ===")
	Logf("  Initial nodes: %d", initialCount)
	Logf("  With snapshots: %d", withSnapshots)

	// Filter based on age and latency thresholds
	var afterFullAgeNodes []NodeResult
	for _, r := range results {
		// Full snapshot age check (use actual full snapshot slot, not SlotDiff)
		if r.FullSlot <= 0 {
			continue // No full snapshot
		}
		fullDist := int64(referenceSlot) - r.FullSlot
		if fullDist < 0 {
			fullDist = -fullDist
		}
		if fullDist <= int64(cfg.FullThreshold) {
			afterFullAgeNodes = append(afterFullAgeNodes, r)
		}
	}
	afterFullAge = len(afterFullAgeNodes)
	Logf("  After full_threshold (%d slots): %d", cfg.FullThreshold, afterFullAge)

	// Incremental snapshot age filter
	var afterIncAgeNodes []NodeResult
	for _, r := range afterFullAgeNodes {
		if r.HasInc && r.IncUsable {
			incDiff := r.SlotDiff
			if incDiff < 0 {
				incDiff = -incDiff
			}
			if incDiff > int64(cfg.IncrementalThreshold) {
				continue
			}
		}
		afterIncAgeNodes = append(afterIncAgeNodes, r)
	}
	afterIncAge = len(afterIncAgeNodes)
	Logf("  After incremental_threshold (%d slots): %d", cfg.IncrementalThreshold, afterIncAge)

	// RTT filter (if enabled)
	var afterRTTNodes []NodeResult
	if cfg.MaxRTTMs > 0 {
		for _, r := range afterIncAgeNodes {
			if r.Latency <= float64(cfg.MaxRTTMs) {
				afterRTTNodes = append(afterRTTNodes, r)
			}
		}
		afterRTT = len(afterRTTNodes)
		Logf("  After max_rtt_ms (%d ms): %d", cfg.MaxRTTMs, afterRTT)
	} else {
		afterRTTNodes = afterIncAgeNodes
		afterRTT = afterIncAge
		Logf("  RTT filter: disabled")
	}

	// Version filter
	var eligible []NodeResult
	if cfg.MinNodeVersion != "" || len(cfg.AllowedNodeVersions) > 0 {
		for _, r := range afterRTTNodes {
			if versionMatchesFilter(r.Version, cfg.MinNodeVersion, cfg.AllowedNodeVersions) {
				eligible = append(eligible, r)
			}
		}
		afterVersion = len(eligible)
		if len(cfg.AllowedNodeVersions) > 0 {
			Logf("  After version filter (allowed: %v): %d", cfg.AllowedNodeVersions, afterVersion)
		} else {
			Logf("  After version filter (min: %s): %d", cfg.MinNodeVersion, afterVersion)
		}
	} else {
		eligible = afterRTTNodes
		afterVersion = afterRTT
		Logf("  Version filter: disabled")
	}

	if len(eligible) == 0 {
		Logln("No nodes passed age/latency/RTT/version filters")
		return nil
	}

	// Stage 1: Fast triage download speed test
	ctx := context.Background()
	ranked := stage1Triage(ctx, eligible, cfg)

	if len(ranked) == 0 {
		Logln("No nodes passed Stage 1 speed sampling")
		return nil
	}

	// Stage 2: Confirm top candidates with accurate measurement
	final := stage2Confirm(ctx, ranked, cfg, referenceSlot)

	// Fallback to Stage 1 results if Stage 2 fails completely
	if len(final) == 0 {
		if !cfg.Quiet {
			Logln("  Stage 2 failed completely - falling back to Stage 1 results")
		}
		// Use Stage 1 results directly (already sorted by median speed)
		var rpcs []string
		for _, r := range ranked {
			rpcs = append(rpcs, r.Result.RPC)
		}
		if len(rpcs) > 0 && !cfg.Quiet {
			Logf("=== Selection Complete (Stage 1 Fallback) ===")
			Logf("  Selected: %s (v%s)", rpcs[0], ranked[0].Result.Version)
			Logf("  Stage 1 median: %.2f MB/s", ranked[0].S1.MedianMBs)
			Logf("  Total candidates: %d", len(rpcs))
		}
		return rpcs
	}

	// Extract node addresses from RankedNodes (already sorted by S2 speed)
	var nodes []string
	for _, r := range final {
		nodes = append(nodes, r.Result.RPC)
	}

	if !cfg.Quiet {
		Logf("=== Selection Complete ===")
		Logf("  Selected: %s (v%s)", nodes[0], final[0].Result.Version)
		Logf("  Stage 1 median: %.2f MB/s", final[0].S1.MedianMBs)
		Logf("  Stage 2 avg: %.2f MB/s (min: %.2f, max: %.2f)",
			final[0].S2.AvgMBs, final[0].S2.MinMBs, final[0].S2.MaxMBs)
		Logf("  Total candidates: %d", len(nodes))
	}

	return nodes
}

// SortBestNodesWithStats filters and ranks snapshot sources by speed, updating stats
// Returns addresses of nodes sorted by download speed (fastest first) and the ranked nodes for fallback search
// referenceSlot is used for calculating current slot diff in Stage 2 output
func SortBestNodesWithStats(results []NodeResult, cfg config.Config, stats *ProbeStats, referenceSlot int) ([]string, []RankedNode) {
	if len(results) == 0 {
		Logln("No nodes passed initial evaluation")
		return nil, nil
	}

	// Count initial nodes with snapshots
	var withSnapshotsNodes []NodeResult
	for _, r := range results {
		if r.FullSlot > 0 || r.HasInc {
			withSnapshotsNodes = append(withSnapshotsNodes, r)
		}
	}
	stats.InitialWithSnapshots = int64(len(withSnapshotsNodes))

	// Version filter FIRST (as requested - after detecting nodes with incrementals)
	var afterVersionNodes []NodeResult
	if cfg.MinNodeVersion != "" || len(cfg.AllowedNodeVersions) > 0 {
		for _, r := range withSnapshotsNodes {
			if versionMatchesFilter(r.Version, cfg.MinNodeVersion, cfg.AllowedNodeVersions) {
				afterVersionNodes = append(afterVersionNodes, r)
			}
		}
	} else {
		afterVersionNodes = withSnapshotsNodes
	}
	stats.AfterVersionFilter = int64(len(afterVersionNodes))

	// RTT filter (if enabled)
	var afterRTTNodes []NodeResult
	if cfg.MaxRTTMs > 0 {
		for _, r := range afterVersionNodes {
			if r.Latency <= float64(cfg.MaxRTTMs) {
				afterRTTNodes = append(afterRTTNodes, r)
			}
		}
	} else {
		afterRTTNodes = afterVersionNodes
	}
	stats.AfterRTTFilter = int64(len(afterRTTNodes))

	// Full snapshot age filter (based on actual full snapshot slot, not SlotDiff which could be from incremental)
	var afterFullAgeNodes []NodeResult
	for _, r := range afterRTTNodes {
		if r.FullSlot <= 0 {
			continue // No full snapshot
		}
		fullDist := int64(referenceSlot) - r.FullSlot
		if fullDist < 0 {
			fullDist = -fullDist
		}
		if fullDist <= int64(cfg.FullThreshold) {
			afterFullAgeNodes = append(afterFullAgeNodes, r)
		}
	}
	stats.AfterFullAgeFilter = int64(len(afterFullAgeNodes))

	// Incremental snapshot age filter
	var afterIncAgeNodes []NodeResult
	for _, r := range afterFullAgeNodes {
		if r.HasInc && r.IncUsable {
			incDiff := r.SlotDiff
			if incDiff < 0 {
				incDiff = -incDiff
			}
			if incDiff > int64(cfg.IncrementalThreshold) {
				continue
			}
		}
		afterIncAgeNodes = append(afterIncAgeNodes, r)
	}
	stats.AfterIncAgeFilter = int64(len(afterIncAgeNodes))
	stats.Eligible = int64(len(afterIncAgeNodes))

	if len(afterIncAgeNodes) == 0 {
		Logln("No nodes passed all filters")
		return nil, nil
	}

	// Stage 1: Fast triage download speed test
	ctx := context.Background()
	ranked := stage1Triage(ctx, afterIncAgeNodes, cfg)

	if len(ranked) == 0 {
		Logln("No nodes passed Stage 1 speed sampling")
		return nil, nil
	}

	// Stage 2: Confirm top candidates with accurate measurement
	final := stage2Confirm(ctx, ranked, cfg, referenceSlot)

	// Fallback to Stage 1 results if Stage 2 fails completely
	if len(final) == 0 {
		if !cfg.Quiet {
			Logln("  Stage 2 failed completely - falling back to Stage 1 results")
		}
		var rpcs []string
		for _, r := range ranked {
			rpcs = append(rpcs, r.Result.RPC)
		}
		if len(rpcs) > 0 && !cfg.Quiet {
			Logf("=== Selection Complete (Stage 1 Fallback) ===")
			Logf("  Selected: %s (v%s)", rpcs[0], ranked[0].Result.Version)
			Logf("  Stage 1 median: %.2f MB/s", ranked[0].S1.MedianMBs)
			Logf("  Total candidates: %d", len(rpcs))
		}
		return rpcs, ranked
	}

	// Extract node addresses from RankedNodes (already sorted by S2 speed)
	var nodes []string
	for _, r := range final {
		nodes = append(nodes, r.Result.RPC)
	}

	if !cfg.Quiet {
		Logf("=== Selection Complete ===")
		Logf("  Selected: %s (v%s)", nodes[0], final[0].Result.Version)
		Logf("  Stage 1 median: %.2f MB/s", final[0].S1.MedianMBs)
		Logf("  Stage 2 avg: %.2f MB/s (min: %.2f, max: %.2f)",
			final[0].S2.AvgMBs, final[0].S2.MinMBs, final[0].S2.MaxMBs)
		Logf("  Total candidates: %d", len(nodes))
	}

	return nodes, final
}

// IncrementalInfo holds information about an incremental snapshot
type IncrementalInfo struct {
	URL      string
	Filename string
	BaseSlot int64
	EndSlot  int64
	NodeRPC  string
}

// RefreshIncrementalInfo probes a node to get the current incremental snapshot info
// This should be called after downloading a full snapshot to ensure we get a fresh incremental URL
func RefreshIncrementalInfo(rpcAddress string, timeoutMs int) (*IncrementalInfo, error) {
	if timeoutMs <= 0 {
		timeoutMs = 5000
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	// Try all incremental paths
	incURL, incFilename, err := probeSnapshotEndpoints(ctx, rpcAddress, incSnapshotPaths)
	if err != nil {
		return nil, fmt.Errorf("failed to probe incremental: %v", err)
	}

	// Parse the filename to extract slots
	matches := incSnapshotRe.FindStringSubmatch(incFilename)
	if matches == nil {
		return nil, fmt.Errorf("invalid incremental filename: %s", incFilename)
	}

	baseSlot, _ := strconv.ParseInt(matches[1], 10, 64)
	endSlot, _ := strconv.ParseInt(matches[2], 10, 64)

	return &IncrementalInfo{
		URL:      incURL,
		Filename: incFilename,
		BaseSlot: baseSlot,
		EndSlot:  endSlot,
		NodeRPC:  rpcAddress,
	}, nil
}

// FindMatchingIncremental searches through a list of ranked nodes to find one with an incremental
// that matches the given full snapshot slot. Returns the first matching incremental info,
// or nil if no match is found.
func FindMatchingIncremental(rankedNodes []RankedNode, fullSnapshotSlot int64, timeoutMs int) (*IncrementalInfo, error) {
	if timeoutMs <= 0 {
		timeoutMs = 5000
	}

	Logf("Searching for incremental with base slot %d across %d nodes...", fullSnapshotSlot, len(rankedNodes))

	// First, check cached data from initial evaluation
	for i, node := range rankedNodes {
		if node.Result.HasInc && node.Result.IncBase == fullSnapshotSlot {
			Logf("  Node %d/%d (%s): cached match found (base=%d, end=%d)",
				i+1, len(rankedNodes), node.Result.RPC, node.Result.IncBase, node.Result.IncSlot)

			// Verify by re-probing to get fresh URL
			info, err := RefreshIncrementalInfo(node.Result.RPC, timeoutMs)
			if err != nil {
				Logf("    Warning: re-probe failed: %v, trying next node", err)
				continue
			}

			if info.BaseSlot == fullSnapshotSlot {
				Logf("  Found matching incremental at %s (base=%d, end=%d)",
					node.Result.RPC, info.BaseSlot, info.EndSlot)
				return info, nil
			}
			Logf("    Warning: cached data was stale (now base=%d), trying next node", info.BaseSlot)
		}
	}

	// Second pass: re-probe all nodes (their incrementals may have been updated)
	Logf("  No cached matches, re-probing all nodes...")
	for i, node := range rankedNodes {
		if !node.Result.HasInc {
			continue
		}

		info, err := RefreshIncrementalInfo(node.Result.RPC, timeoutMs)
		if err != nil {
			continue
		}

		if info.BaseSlot == fullSnapshotSlot {
			Logf("  Found matching incremental at %s (base=%d, end=%d) after re-probe",
				node.Result.RPC, info.BaseSlot, info.EndSlot)
			return info, nil
		}

		Logf("  Node %d/%d (%s): base=%d (not a match)",
			i+1, len(rankedNodes), node.Result.RPC, info.BaseSlot)
	}

	return nil, fmt.Errorf("no incremental with base slot %d found across %d nodes", fullSnapshotSlot, len(rankedNodes))
}

// SortBestRPCsFilteredBySlot filters nodes to only include those with full snapshot at or after minSlot
// This is useful when you need nodes that have a specific full snapshot slot
func SortBestRPCsFilteredBySlot(results []NodeResult, cfg config.Config, stats *ProbeStats, minSlot int64, referenceSlot int) ([]string, []RankedNode) {
	nodes, ranked := SortBestNodesWithStats(results, cfg, stats, referenceSlot)

	// Filter to only include nodes with full snapshot at or after minSlot
	if minSlot > 0 {
		var filtered []string
		var filteredRanked []RankedNode
		for i, rpc := range nodes {
			if ranked[i].Result.FullSlot >= minSlot {
				filtered = append(filtered, rpc)
				filteredRanked = append(filteredRanked, ranked[i])
			}
		}
		if len(filtered) > 0 {
			return filtered, filteredRanked
		}
	}

	return nodes, ranked
}
