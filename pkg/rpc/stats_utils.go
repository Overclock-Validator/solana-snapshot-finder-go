package rpc

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/term"
)

var programStartTime = time.Now()

// relPrefix returns the relative time prefix
func relPrefix() string {
	d := time.Since(programStartTime).Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	var timeStr string
	if h > 0 {
		timeStr = fmt.Sprintf("%2dh%02dm", h, m)
	} else if m > 0 {
		timeStr = fmt.Sprintf("%2dm%02ds", m, s)
	} else {
		timeStr = fmt.Sprintf("%5ds", s)
	}
	return fmt.Sprintf("(+%s) ", timeStr)
}

// Logf prints with relative timestamp from program start (exported for use by other packages)
func Logf(format string, args ...interface{}) {
	fmt.Printf("%s%s", relPrefix(), fmt.Sprintf(format, args...))
}

// Logln prints with relative timestamp from program start (exported for use by other packages)
func Logln(args ...interface{}) {
	fmt.Print(relPrefix())
	fmt.Println(args...)
}

// ANSI color codes for terminal output
const (
	colorTeal  = "\x1b[38;5;85m"
	colorReset = "\x1b[0m"
)

// ProbeStats holds statistics for node probing and filtering
type ProbeStats struct {
	mu sync.Mutex

	// Probing stats
	TotalNodes     int64
	TCPFailed      int64
	HasAnySnapshot int64
	NoSnapshot     int64
	ProbeTimeouts  int64
	ProbeOtherErr  int64

	// RTT histogram bins (ms)
	RTT0To100   int64
	RTT100To200 int64
	RTT200To300 int64
	RTT300To400 int64
	RTTOver400  int64

	// Version histogram (version string -> count)
	VersionCounts map[string]int64

	// Incremental presence
	WithIncremental    int64
	WithoutIncremental int64

	// Incremental distance histogram (slots from tip)
	// Negative means ahead of tip
	IncAhead      int64 // Negative distance (ahead of tip)
	Inc0To100     int64
	Inc100To200   int64
	Inc200To1000  int64
	Inc1000Plus   int64
	MaxAheadSlots int64 // Maximum ahead distance seen

	// Incremental usability
	WithUsableInc int64

	// Filter stats at each step (nodes remaining)
	InitialWithSnapshots int64
	AfterVersionFilter   int64
	AfterRTTFilter       int64
	AfterFullAgeFilter   int64
	AfterIncAgeFilter    int64

	Eligible int64
}

// NewProbeStats creates a new ProbeStats instance
func NewProbeStats() *ProbeStats {
	return &ProbeStats{
		VersionCounts: make(map[string]int64),
	}
}

// RecordRTT records RTT into histogram bins (thread-safe)
func (s *ProbeStats) RecordRTT(rtt float64) {
	switch {
	case rtt >= 0 && rtt < 100:
		atomic.AddInt64(&s.RTT0To100, 1)
	case rtt >= 100 && rtt < 200:
		atomic.AddInt64(&s.RTT100To200, 1)
	case rtt >= 200 && rtt < 300:
		atomic.AddInt64(&s.RTT200To300, 1)
	case rtt >= 300 && rtt <= 400:
		atomic.AddInt64(&s.RTT300To400, 1)
	default:
		atomic.AddInt64(&s.RTTOver400, 1)
	}
}

// RecordVersion records a node version (thread-safe)
func (s *ProbeStats) RecordVersion(version string) {
	if version == "" {
		version = "unknown"
	}
	s.mu.Lock()
	s.VersionCounts[version]++
	s.mu.Unlock()
}

// RecordIncrementalDistance records incremental snapshot distance from tip (thread-safe)
// distance = tipSlot - incSlot (negative means ahead of tip)
func (s *ProbeStats) RecordIncrementalDistance(distance int64) {
	if distance < 0 {
		atomic.AddInt64(&s.IncAhead, 1)
		// Track maximum ahead distance
		absDistance := -distance
		s.mu.Lock()
		if absDistance > s.MaxAheadSlots {
			s.MaxAheadSlots = absDistance
		}
		s.mu.Unlock()
	} else if distance <= 100 {
		atomic.AddInt64(&s.Inc0To100, 1)
	} else if distance <= 200 {
		atomic.AddInt64(&s.Inc100To200, 1)
	} else if distance <= 1000 {
		atomic.AddInt64(&s.Inc200To1000, 1)
	} else {
		atomic.AddInt64(&s.Inc1000Plus, 1)
	}
}

// RecordTCPFailed records a TCP precheck failure (thread-safe)
func (s *ProbeStats) RecordTCPFailed() {
	atomic.AddInt64(&s.TCPFailed, 1)
}

// RecordProbeResult records probe outcome (thread-safe)
func (s *ProbeStats) RecordProbeResult(hasSnapshot, timeout bool, hasError bool) {
	if hasSnapshot {
		atomic.AddInt64(&s.HasAnySnapshot, 1)
	} else {
		atomic.AddInt64(&s.NoSnapshot, 1)
	}
	if timeout {
		atomic.AddInt64(&s.ProbeTimeouts, 1)
	}
	if hasError && !timeout {
		atomic.AddInt64(&s.ProbeOtherErr, 1)
	}
}

// RecordIncremental records whether node has incremental (thread-safe)
func (s *ProbeStats) RecordIncremental(hasInc bool, isUsable bool) {
	if hasInc {
		atomic.AddInt64(&s.WithIncremental, 1)
		if isUsable {
			atomic.AddInt64(&s.WithUsableInc, 1)
		}
	} else {
		atomic.AddInt64(&s.WithoutIncremental, 1)
	}
}

// getTopVersions returns the top N versions sorted by count
func (s *ProbeStats) getTopVersions(n int) []struct {
	Version string
	Count   int64
} {
	s.mu.Lock()
	defer s.mu.Unlock()

	type versionCount struct {
		Version string
		Count   int64
	}
	var versions []versionCount
	for v, c := range s.VersionCounts {
		versions = append(versions, versionCount{v, c})
	}
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].Count > versions[j].Count
	})
	if len(versions) > n {
		versions = versions[:n]
	}

	result := make([]struct {
		Version string
		Count   int64
	}, len(versions))
	for i, v := range versions {
		result[i] = struct {
			Version string
			Count   int64
		}{v.Version, v.Count}
	}
	return result
}

// PrintReport prints a comprehensive statistics report (no timestamps on tables)
// This is the original combined report - prefer using PrintNodeDiscoveryReport + PrintFilterPipeline separately
func (s *ProbeStats) PrintReport(cfg FilterConfig) {
	s.PrintNodeDiscoveryReport()
	s.PrintFilterPipeline(cfg, nil)
}

// PrintNodeDiscoveryReport prints the node discovery sections (without filter pipeline)
// This should be printed BEFORE speed testing starts
func (s *ProbeStats) PrintNodeDiscoveryReport() {
	// Check if terminal supports colors
	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	c := ""  // color start
	r := ""  // color reset
	if useColor {
		c = colorTeal
		r = colorReset
	}

	// 10-space indent to align with text after relative timestamps "(+    0s) "
	ind := "          "

	// Header (no timestamps for tables - they're visual reports)
	fmt.Println()
	fmt.Printf("%s%s╔═════════════════════════════════════════════════════════════════╗%s\n", ind, c, r)
	fmt.Printf("%s%s║                    NODE DISCOVERY REPORT                        ║%s\n", ind, c, r)
	fmt.Printf("%s%s╚═════════════════════════════════════════════════════════════════╝%s\n", ind, c, r)
	fmt.Println()

	// Probing Summary
	fmt.Printf("%s%s┌───────────────────────────────────────────────────────────────┐%s\n", ind, c, r)
	fmt.Printf("%s%s│%s PROBING SUMMARY                                               %s│%s\n", ind, c, r, c, r)
	fmt.Printf("%s%s├───────────────────────────────────────────────────────────────┤%s\n", ind, c, r)
	fmt.Printf("%s%s│%s Total nodes discovered: %-38d%s│%s\n", ind, c, r, atomic.LoadInt64(&s.TotalNodes), c, r)
	fmt.Printf("%s%s│%s TCP precheck failed:    %-38d%s│%s\n", ind, c, r, atomic.LoadInt64(&s.TCPFailed), c, r)
	fmt.Printf("%s%s│%s With snapshots:         %-38d%s│%s\n", ind, c, r, atomic.LoadInt64(&s.HasAnySnapshot), c, r)
	fmt.Printf("%s%s│%s Without snapshots:      %-38d%s│%s\n", ind, c, r, atomic.LoadInt64(&s.NoSnapshot), c, r)
	fmt.Printf("%s%s│%s Probe timeouts:         %-38d%s│%s\n", ind, c, r, atomic.LoadInt64(&s.ProbeTimeouts), c, r)
	fmt.Printf("%s%s│%s Other probe errors:     %-38d%s│%s\n", ind, c, r, atomic.LoadInt64(&s.ProbeOtherErr), c, r)
	fmt.Printf("%s%s└───────────────────────────────────────────────────────────────┘%s\n", ind, c, r)
	fmt.Println()

	// RTT Histogram
	fmt.Printf("%s%s┌───────────────────────────────────────────────────────────────┐%s\n", ind, c, r)
	fmt.Printf("%s%s│%s RTT Distance Histogram (milliseconds)                         %s│%s\n", ind, c, r, c, r)
	fmt.Printf("%s%s├───────────────────────────────────────────────────────────────┤%s\n", ind, c, r)
	s.printHistogramBar("0-100", atomic.LoadInt64(&s.RTT0To100), s.TotalNodes, c, r, ind)
	s.printHistogramBar("100-200", atomic.LoadInt64(&s.RTT100To200), s.TotalNodes, c, r, ind)
	s.printHistogramBar("200-300", atomic.LoadInt64(&s.RTT200To300), s.TotalNodes, c, r, ind)
	s.printHistogramBar("300-400", atomic.LoadInt64(&s.RTT300To400), s.TotalNodes, c, r, ind)
	s.printHistogramBar(">400", atomic.LoadInt64(&s.RTTOver400), s.TotalNodes, c, r, ind)
	fmt.Printf("%s%s└───────────────────────────────────────────────────────────────┘%s\n", ind, c, r)
	fmt.Println()

	// Version Distribution
	fmt.Printf("%s%s┌───────────────────────────────────────────────────────────────┐%s\n", ind, c, r)
	fmt.Printf("%s%s│%s NODE VERSION DISTRIBUTION (top 10)                            %s│%s\n", ind, c, r, c, r)
	fmt.Printf("%s%s├───────────────────────────────────────────────────────────────┤%s\n", ind, c, r)
	topVersions := s.getTopVersions(10)
	for _, v := range topVersions {
		s.printHistogramBar(v.Version, v.Count, s.HasAnySnapshot, c, r, ind)
	}
	fmt.Printf("%s%s└───────────────────────────────────────────────────────────────┘%s\n", ind, c, r)
	fmt.Println()

	// Incremental Snapshot Stats
	fmt.Printf("%s%s┌───────────────────────────────────────────────────────────────┐%s\n", ind, c, r)
	fmt.Printf("%s%s│%s INCREMENTAL SNAPSHOT DISTRIBUTION                             %s│%s\n", ind, c, r, c, r)
	fmt.Printf("%s%s├───────────────────────────────────────────────────────────────┤%s\n", ind, c, r)
	withInc := atomic.LoadInt64(&s.WithIncremental)
	withoutInc := atomic.LoadInt64(&s.WithoutIncremental)
	fmt.Printf("%s%s│%s With incremental:    %-41d%s│%s\n", ind, c, r, withInc, c, r)
	fmt.Printf("%s%s│%s Without incremental: %-41d%s│%s\n", ind, c, r, withoutInc, c, r)
	fmt.Printf("%s%s│%s With usable inc:     %-41d%s│%s\n", ind, c, r, atomic.LoadInt64(&s.WithUsableInc), c, r)
	fmt.Printf("%s%s│%s                                                               %s│%s\n", ind, c, r, c, r)
	fmt.Printf("%s%s│%s Distance from tip (slots):                                    %s│%s\n", ind, c, r, c, r)
	s.printHistogramBar("ahead", atomic.LoadInt64(&s.IncAhead), withInc, c, r, ind)
	if s.MaxAheadSlots > 0 {
		fmt.Printf("%s%s│%s   (max ahead: %d slots)%-40s%s│%s\n", ind, c, r, s.MaxAheadSlots, "", c, r)
	}
	s.printHistogramBar("0-100", atomic.LoadInt64(&s.Inc0To100), withInc, c, r, ind)
	s.printHistogramBar("100-200", atomic.LoadInt64(&s.Inc100To200), withInc, c, r, ind)
	s.printHistogramBar("200-1000", atomic.LoadInt64(&s.Inc200To1000), withInc, c, r, ind)
	s.printHistogramBar("1000+", atomic.LoadInt64(&s.Inc1000Plus), withInc, c, r, ind)
	fmt.Printf("%s%s└───────────────────────────────────────────────────────────────┘%s\n", ind, c, r)
	fmt.Println()
}

// PrintFilterPipeline prints the filter pipeline section with optional speed test stats
// This should be printed AFTER speed testing completes, before final source selection
func (s *ProbeStats) PrintFilterPipeline(cfg FilterConfig, speedStats *SpeedTestStats) {
	// Check if terminal supports colors
	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	c := ""  // color start
	r := ""  // color reset
	if useColor {
		c = colorTeal
		r = colorReset
	}

	// 10-space indent to align with text after relative timestamps "(+    0s) "
	ind := "          "

	// Filter Pipeline
	fmt.Printf("%s%s┌───────────────────────────────────────────────────────────────┐%s\n", ind, c, r)
	fmt.Printf("%s%s│%s FILTER PIPELINE                                               %s│%s\n", ind, c, r, c, r)
	fmt.Printf("%s%s├───────────────────────────────────────────────────────────────┤%s\n", ind, c, r)
	s.printFilterRow("Initial (with snapshots)", s.InitialWithSnapshots, 0, c, r, ind)

	if cfg.MinVersion != "" || len(cfg.AllowedVersions) > 0 {
		versionDesc := cfg.MinVersion
		if len(cfg.AllowedVersions) > 0 {
			versionDesc = fmt.Sprintf("[%s]", strings.Join(cfg.AllowedVersions, ", "))
		}
		filtered := s.InitialWithSnapshots - s.AfterVersionFilter
		label := fmt.Sprintf("After version filter (%s)", versionDesc)
		s.printFilterRow(label, s.AfterVersionFilter, filtered, c, r, ind)
	}

	if cfg.MaxRTTMs > 0 {
		filtered := s.AfterVersionFilter - s.AfterRTTFilter
		if s.AfterVersionFilter == 0 {
			filtered = s.InitialWithSnapshots - s.AfterRTTFilter
		}
		label := fmt.Sprintf("After RTT filter (%dms)", cfg.MaxRTTMs)
		s.printFilterRow(label, s.AfterRTTFilter, filtered, c, r, ind)
	}

	prevCount := s.AfterRTTFilter
	if prevCount == 0 {
		prevCount = s.AfterVersionFilter
	}
	if prevCount == 0 {
		prevCount = s.InitialWithSnapshots
	}
	filtered := prevCount - s.AfterFullAgeFilter
	label := fmt.Sprintf("After full_threshold (%d slots)", cfg.FullThreshold)
	s.printFilterRow(label, s.AfterFullAgeFilter, filtered, c, r, ind)

	filtered = s.AfterFullAgeFilter - s.AfterIncAgeFilter
	label = fmt.Sprintf("After inc_threshold (%d slots)", cfg.IncThreshold)
	s.printFilterRow(label, s.AfterIncAgeFilter, filtered, c, r, ind)

	s.printFilterRow("Eligible for speed testing", atomic.LoadInt64(&s.Eligible), 0, c, r, ind)

	// Add speed test stats if provided
	if speedStats != nil {
		fmt.Printf("%s%s│%s                                                               %s│%s\n", ind, c, r, c, r)
		s1Filtered := speedStats.Stage1Tested - speedStats.Stage1Passed
		s.printFilterRow("After Stage 1 (fast triage)", speedStats.Stage1Passed, s1Filtered, c, r, ind)
		s2Filtered := speedStats.Stage2Tested - speedStats.Stage2Passed
		s.printFilterRow("After Stage 2 (sustained test)", speedStats.Stage2Passed, s2Filtered, c, r, ind)
	}

	fmt.Printf("%s%s└───────────────────────────────────────────────────────────────┘%s\n", ind, c, r)
	fmt.Println()
}

// printHistogramBar prints a histogram bar for the report (no timestamp)
func (s *ProbeStats) printHistogramBar(label string, count int64, total int64, colorStart, colorReset, indent string) {
	maxBarWidth := 28
	pct := float64(0)
	if total > 0 {
		pct = float64(count) / float64(total) * 100
	}
	barWidth := int(float64(maxBarWidth) * float64(count) / float64(max(total, 1)))
	bar := strings.Repeat("█", barWidth) + strings.Repeat("░", maxBarWidth-barWidth)

	// Pad label to 10 chars
	paddedLabel := fmt.Sprintf("%-10s", label)
	if len(paddedLabel) > 10 {
		paddedLabel = paddedLabel[:10]
	}

	// Format: space + label(10) + space + bar(28) + space + count(5) + " (" + pct(5) + "%)" + padding
	// Total inner width = 61 chars (to fit in 63-char box with "│ " and " │")
	fmt.Printf("%s%s│%s %s %s %5d (%5.1f%%)      %s│%s\n", indent, colorStart, colorReset, paddedLabel, bar, count, pct, colorStart, colorReset)
}

// printFilterRow prints a row for the filter pipeline section (no timestamp)
// If filtered is 0, it shows just the count; otherwise shows "count (-filtered)"
func (s *ProbeStats) printFilterRow(label string, count int64, filtered int64, colorStart, colorReset, indent string) {
	const innerWidth = 61 // Usable content width between "│ " and " │"

	var valueStr string
	if filtered == 0 {
		valueStr = fmt.Sprintf("%d", count)
	} else {
		valueStr = fmt.Sprintf("%d (-%d)", count, filtered)
	}

	// Truncate label if too long
	maxLabelLen := innerWidth - len(valueStr) - 1 // -1 for at least one space
	if len(label) > maxLabelLen {
		label = label[:maxLabelLen]
	}

	// Calculate padding to right-align value
	padding := innerWidth - len(label) - len(valueStr)
	if padding < 1 {
		padding = 1
	}

	fmt.Printf("%s%s│%s %s%s%s %s│%s\n", indent, colorStart, colorReset, label, strings.Repeat(" ", padding), valueStr, colorStart, colorReset)
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// FilterConfig holds filter configuration for stats reporting
type FilterConfig struct {
	MaxRTTMs        int
	FullThreshold   int
	IncThreshold    int
	MinVersion      string
	AllowedVersions []string
}

// SpeedTestStats holds statistics for speed testing stages
type SpeedTestStats struct {
	// Stage 1
	Stage1Tested   int64
	Stage1Passed   int64
	Stage1Timeouts int64
	Stage1Errors   int64

	// Stage 2
	Stage2Tested    int64
	Stage2Passed    int64
	Stage2Collapsed int64 // Failed collapse guard
	Stage2TooSlow   int64 // Below minimum absolute speed
	Stage2Errors    int64
}

// PrintSpeedTestReport prints a formatted speed test statistics report (no timestamps)
func (s *SpeedTestStats) PrintSpeedTestReport() {
	// Check if terminal supports colors
	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	c := ""  // color start
	r := ""  // color reset
	if useColor {
		c = colorTeal
		r = colorReset
	}

	// 10-space indent to align with text after relative timestamps "(+    0s) "
	ind := "          "

	fmt.Println()
	fmt.Printf("%s%s┌───────────────────────────────────────────────────────────────┐%s\n", ind, c, r)
	fmt.Printf("%s%s│%s SPEED TEST RESULTS                                            %s│%s\n", ind, c, r, c, r)
	fmt.Printf("%s%s├───────────────────────────────────────────────────────────────┤%s\n", ind, c, r)
	fmt.Printf("%s%s│%s Stage 1 (Fast Triage):                                        %s│%s\n", ind, c, r, c, r)
	fmt.Printf("%s%s│%s   Tested: %-5d  Passed: %-5d  Timeouts: %-5d  Errors: %-4d %s│%s\n",
		ind, c, r, s.Stage1Tested, s.Stage1Passed, s.Stage1Timeouts, s.Stage1Errors, c, r)
	fmt.Printf("%s%s│%s Stage 2 (Confirm):                                            %s│%s\n", ind, c, r, c, r)
	fmt.Printf("%s%s│%s   Tested: %-5d  Passed: %-5d  Collapsed: %-4d  Slow: %-5d  %s│%s\n",
		ind, c, r, s.Stage2Tested, s.Stage2Passed, s.Stage2Collapsed, s.Stage2TooSlow, c, r)
	fmt.Printf("%s%s└───────────────────────────────────────────────────────────────┘%s\n", ind, c, r)
}

// RankedNodeInfo holds info about a ranked node for display purposes
type RankedNodeInfo struct {
	Rank    int
	RPC     string
	Version string
	SpeedS1 float64 // Stage 1 median speed
	SpeedS2 float64 // Stage 2 min speed (0 if not tested)
}

// PrintStage2CandidatesTable prints the Stage 2 candidates in a table format (no timestamps)
func PrintStage2CandidatesTable(candidates []RankedNodeInfo) {
	if len(candidates) == 0 {
		return
	}

	// Check if terminal supports colors
	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	c := ""  // color start
	r := ""  // color reset
	if useColor {
		c = colorTeal
		r = colorReset
	}

	// 10-space indent to align with text after relative timestamps "(+    0s) "
	ind := "          "

	fmt.Println()
	fmt.Printf("%s%s┌───────────────────────────────────────────────────────────────┐%s\n", ind, c, r)
	fmt.Printf("%s%s│%s STAGE 2 CANDIDATES (top %d by speed)                           %s│%s\n", ind, c, r, len(candidates), c, r)
	fmt.Printf("%s%s├────┬──────────────────────────┬──────────┬────────────────────┤%s\n", ind, c, r)
	fmt.Printf("%s%s│%s #  │ Node IP                  │ Version  │ Speed (MB/s)       %s│%s\n", ind, c, r, c, r)
	fmt.Printf("%s%s├────┼──────────────────────────┼──────────┼────────────────────┤%s\n", ind, c, r)

	for _, node := range candidates {
		// Truncate IP to fit column
		ip := node.RPC
		if len(ip) > 24 {
			ip = ip[:21] + "..."
		}
		// Truncate version to fit
		version := node.Version
		if len(version) > 8 {
			version = version[:8]
		}

		// Show S2 speed if available, otherwise S1
		speed := node.SpeedS2
		speedLabel := "S2"
		if speed == 0 {
			speed = node.SpeedS1
			speedLabel = "S1"
		}

		fmt.Printf("%s%s│%s %-2d │ %-24s │ %-8s │ %6.1f (%s)       %s│%s\n",
			ind, c, r, node.Rank, ip, version, speed, speedLabel, c, r)
	}

	fmt.Printf("%s%s└────┴──────────────────────────┴──────────┴────────────────────┘%s\n", ind, c, r)
	fmt.Println()
}
