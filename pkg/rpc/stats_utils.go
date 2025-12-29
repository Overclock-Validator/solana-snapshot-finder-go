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
	c := "" // color start
	r := "" // color reset
	if useColor {
		c = colorTeal
		r = colorReset
	}

	// Header (no timestamps for tables - they're visual reports)
	// Box width = 80 chars to match Mithril banner width
	fmt.Println()
	fmt.Printf("%s╔══════════════════════════════════════════════════════════════════════════════╗%s\n", c, r)
	fmt.Printf("%s║%s                           NODE DISCOVERY REPORT                              %s║%s\n", c, r, c, r)
	fmt.Printf("%s╚══════════════════════════════════════════════════════════════════════════════╝%s\n", c, r)
	fmt.Println()

	// Probing Summary
	fmt.Printf("%s┌──────────────────────────────────────────────────────────────────────────────┐%s\n", c, r)
	fmt.Printf("%s│%s PROBING SUMMARY                                                              %s│%s\n", c, r, c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	fmt.Printf("%s│%s Total nodes discovered: %-53d%s│%s\n", c, r, atomic.LoadInt64(&s.TotalNodes), c, r)
	fmt.Printf("%s│%s TCP precheck failed:    %-53d%s│%s\n", c, r, atomic.LoadInt64(&s.TCPFailed), c, r)
	fmt.Printf("%s│%s With snapshots:         %-53d%s│%s\n", c, r, atomic.LoadInt64(&s.HasAnySnapshot), c, r)
	fmt.Printf("%s│%s Without snapshots:      %-53d%s│%s\n", c, r, atomic.LoadInt64(&s.NoSnapshot), c, r)
	fmt.Printf("%s│%s Probe timeouts:         %-53d%s│%s\n", c, r, atomic.LoadInt64(&s.ProbeTimeouts), c, r)
	fmt.Printf("%s│%s Other probe errors:     %-53d%s│%s\n", c, r, atomic.LoadInt64(&s.ProbeOtherErr), c, r)
	fmt.Printf("%s└──────────────────────────────────────────────────────────────────────────────┘%s\n", c, r)
	fmt.Println()

	// RTT Histogram
	fmt.Printf("%s┌──────────────────────────────────────────────────────────────────────────────┐%s\n", c, r)
	fmt.Printf("%s│%s RTT DISTANCE HISTOGRAM (milliseconds)                                        %s│%s\n", c, r, c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	s.printHistogramBarPadded("0-100", atomic.LoadInt64(&s.RTT0To100), s.TotalNodes, c, r)
	s.printHistogramBarPadded("100-200", atomic.LoadInt64(&s.RTT100To200), s.TotalNodes, c, r)
	s.printHistogramBarPadded("200-300", atomic.LoadInt64(&s.RTT200To300), s.TotalNodes, c, r)
	s.printHistogramBarPadded("300-400", atomic.LoadInt64(&s.RTT300To400), s.TotalNodes, c, r)
	s.printHistogramBarPadded(">400", atomic.LoadInt64(&s.RTTOver400), s.TotalNodes, c, r)
	fmt.Printf("%s└──────────────────────────────────────────────────────────────────────────────┘%s\n", c, r)
	fmt.Println()

	// Version Distribution
	fmt.Printf("%s┌──────────────────────────────────────────────────────────────────────────────┐%s\n", c, r)
	fmt.Printf("%s│%s NODE VERSION DISTRIBUTION (top 10)                                           %s│%s\n", c, r, c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	topVersions := s.getTopVersions(10)
	for _, v := range topVersions {
		s.printHistogramBarPadded(v.Version, v.Count, s.HasAnySnapshot, c, r)
	}
	fmt.Printf("%s└──────────────────────────────────────────────────────────────────────────────┘%s\n", c, r)
	fmt.Println()

	// Incremental Snapshot Stats
	fmt.Printf("%s┌──────────────────────────────────────────────────────────────────────────────┐%s\n", c, r)
	fmt.Printf("%s│%s INCREMENTAL SNAPSHOT DISTRIBUTION                                            %s│%s\n", c, r, c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	withInc := atomic.LoadInt64(&s.WithIncremental)
	withoutInc := atomic.LoadInt64(&s.WithoutIncremental)
	fmt.Printf("%s│%s With incremental:    %-56d%s│%s\n", c, r, withInc, c, r)
	fmt.Printf("%s│%s Without incremental: %-56d%s│%s\n", c, r, withoutInc, c, r)
	fmt.Printf("%s│%s With usable inc:     %-56d%s│%s\n", c, r, atomic.LoadInt64(&s.WithUsableInc), c, r)
	fmt.Printf("%s│%s                                                                              %s│%s\n", c, r, c, r)
	fmt.Printf("%s│%s Distance from tip (slots):                                                   %s│%s\n", c, r, c, r)
	s.printHistogramBarPadded("ahead", atomic.LoadInt64(&s.IncAhead), withInc, c, r)
	if s.MaxAheadSlots > 0 {
		fmt.Printf("%s│%s   (max ahead: %d slots)%-55s%s│%s\n", c, r, s.MaxAheadSlots, "", c, r)
	}
	s.printHistogramBarPadded("0-100", atomic.LoadInt64(&s.Inc0To100), withInc, c, r)
	s.printHistogramBarPadded("100-200", atomic.LoadInt64(&s.Inc100To200), withInc, c, r)
	s.printHistogramBarPadded("200-1000", atomic.LoadInt64(&s.Inc200To1000), withInc, c, r)
	s.printHistogramBarPadded("1000+", atomic.LoadInt64(&s.Inc1000Plus), withInc, c, r)
	fmt.Printf("%s└──────────────────────────────────────────────────────────────────────────────┘%s\n", c, r)
	fmt.Println()
}

// PrintFilterPipeline prints the filter pipeline section with optional speed test stats
// This should be printed AFTER speed testing completes, before final source selection
func (s *ProbeStats) PrintFilterPipeline(cfg FilterConfig, speedStats *SpeedTestStats) {
	// Check if terminal supports colors
	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	c := "" // color start
	r := "" // color reset
	if useColor {
		c = colorTeal
		r = colorReset
	}

	// Filter Pipeline - 80 char width to match banner
	fmt.Printf("%s┌──────────────────────────────────────────────────────────────────────────────┐%s\n", c, r)
	fmt.Printf("%s│%s FILTER PIPELINE                                                              %s│%s\n", c, r, c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	s.printFilterRowPadded("Initial (with snapshots)", s.InitialWithSnapshots, 0, c, r)

	if cfg.MinVersion != "" || len(cfg.AllowedVersions) > 0 {
		versionDesc := cfg.MinVersion
		if len(cfg.AllowedVersions) > 0 {
			versionDesc = fmt.Sprintf("[%s]", strings.Join(cfg.AllowedVersions, ", "))
		}
		filtered := s.InitialWithSnapshots - s.AfterVersionFilter
		label := fmt.Sprintf("After version filter (%s)", versionDesc)
		s.printFilterRowPadded(label, s.AfterVersionFilter, filtered, c, r)
	}

	if cfg.MaxRTTMs > 0 {
		filtered := s.AfterVersionFilter - s.AfterRTTFilter
		if s.AfterVersionFilter == 0 {
			filtered = s.InitialWithSnapshots - s.AfterRTTFilter
		}
		label := fmt.Sprintf("After RTT filter (%dms)", cfg.MaxRTTMs)
		s.printFilterRowPadded(label, s.AfterRTTFilter, filtered, c, r)
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
	s.printFilterRowPadded(label, s.AfterFullAgeFilter, filtered, c, r)

	filtered = s.AfterFullAgeFilter - s.AfterIncAgeFilter
	label = fmt.Sprintf("After inc_threshold (%d slots)", cfg.IncThreshold)
	s.printFilterRowPadded(label, s.AfterIncAgeFilter, filtered, c, r)

	s.printFilterRowPadded("Eligible for speed testing", atomic.LoadInt64(&s.Eligible), 0, c, r)

	// Add speed test stats if provided
	if speedStats != nil {
		fmt.Printf("%s│%s                                                                              %s│%s\n", c, r, c, r)
		s1Filtered := speedStats.Stage1Tested - speedStats.Stage1Passed
		s.printFilterRowPadded("After Stage 1 (fast triage)", speedStats.Stage1Passed, s1Filtered, c, r)
		s2Filtered := speedStats.Stage2Tested - speedStats.Stage2Passed
		s.printFilterRowPadded("After Stage 2 (sustained test)", speedStats.Stage2Passed, s2Filtered, c, r)
	}

	fmt.Printf("%s└──────────────────────────────────────────────────────────────────────────────┘%s\n", c, r)
	fmt.Println()
}

// printHistogramBar prints a histogram bar for the report (no timestamp)
// Box width = 80 chars, inner content = 78 chars
func (s *ProbeStats) printHistogramBar(label string, count int64, total int64, colorStart, colorReset string) {
	s.printHistogramBarPadded(label, count, total, colorStart, colorReset)
}

// printHistogramBarPadded prints a histogram bar for alignment with 80-char box
// Box width = 80 chars, inner content = 78 chars
func (s *ProbeStats) printHistogramBarPadded(label string, count int64, total int64, colorStart, colorReset string) {
	maxBarWidth := 36 // Increased for wider box
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

	// Format: space + label(10) + space + bar(36) + space + count(5) + " (" + pct(5) + "%)" + padding
	// Total inner width = 78 chars (to fit in 80-char box with │ on each side)
	// 1 + 10 + 1 + 36 + 1 + 5 + 2 + 5 + 2 + 15 = 78
	fmt.Printf("%s│%s %s %s %5d (%5.1f%%)               %s│%s\n", colorStart, colorReset, paddedLabel, bar, count, pct, colorStart, colorReset)
}

// printFilterRow prints a row for the filter pipeline section (no timestamp)
// If filtered is 0, it shows just the count; otherwise shows "count (-filtered)"
// Box width = 80 chars, inner content = 78 chars
func (s *ProbeStats) printFilterRow(label string, count int64, filtered int64, colorStart, colorReset string) {
	s.printFilterRowPadded(label, count, filtered, colorStart, colorReset)
}

// printFilterRowPadded prints a row for alignment with 80-char box
// Box width = 80 chars, inner content = 78 chars
func (s *ProbeStats) printFilterRowPadded(label string, count int64, filtered int64, colorStart, colorReset string) {
	const innerWidth = 76 // Usable content width between "│ " and " │" for 80-char box

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

	fmt.Printf("%s│%s %s%s%s %s│%s\n", colorStart, colorReset, label, strings.Repeat(" ", padding), valueStr, colorStart, colorReset)
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

// WriteSpeedTestLog writes detailed speed test statistics to a log file
// The file is created in the specified directory with a timestamped name
func (s *SpeedTestStats) WriteSpeedTestLog(logDir string, cfg FilterConfig) error {
	if s == nil {
		return nil
	}

	// Create log directory if it doesn't exist
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	// Create log file with timestamp
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	logPath := fmt.Sprintf("%s/snapshot-search-%s.log", logDir, timestamp)
	file, err := os.Create(logPath)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	defer file.Close()

	// Write header
	fmt.Fprintf(file, "=== Snapshot Source Search Log ===\n")
	fmt.Fprintf(file, "Generated: %s\n\n", time.Now().Format(time.RFC3339))

	// Write filter configuration
	fmt.Fprintf(file, "=== Filter Configuration ===\n")
	if cfg.MinVersion != "" {
		fmt.Fprintf(file, "Minimum Version: %s\n", cfg.MinVersion)
	}
	if len(cfg.AllowedVersions) > 0 {
		fmt.Fprintf(file, "Allowed Versions: %v\n", cfg.AllowedVersions)
	}
	if cfg.MaxRTTMs > 0 {
		fmt.Fprintf(file, "Max RTT: %d ms\n", cfg.MaxRTTMs)
	}
	fmt.Fprintf(file, "Full Threshold: %d slots\n", cfg.FullThreshold)
	fmt.Fprintf(file, "Incremental Threshold: %d slots\n\n", cfg.IncThreshold)

	// Write Stage 1 details
	fmt.Fprintf(file, "=== Stage 1: Fast Triage ===\n")
	fmt.Fprintf(file, "Tested:   %d nodes\n", s.Stage1Tested)
	fmt.Fprintf(file, "Passed:   %d nodes\n", s.Stage1Passed)
	fmt.Fprintf(file, "Filtered: %d nodes\n", s.Stage1Tested-s.Stage1Passed)
	fmt.Fprintf(file, "\nBreakdown of filtered nodes:\n")
	fmt.Fprintf(file, "  Timeouts:     %d (%.1f%%)\n", s.Stage1Timeouts, pct(s.Stage1Timeouts, s.Stage1Tested))
	fmt.Fprintf(file, "  Errors:       %d (%.1f%%)\n", s.Stage1Errors, pct(s.Stage1Errors, s.Stage1Tested))
	s1ZeroSpeed := s.Stage1Tested - s.Stage1Passed - s.Stage1Timeouts - s.Stage1Errors
	if s1ZeroSpeed < 0 {
		s1ZeroSpeed = 0
	}
	fmt.Fprintf(file, "  Zero speed:   %d (%.1f%%)\n\n", s1ZeroSpeed, pct(s1ZeroSpeed, s.Stage1Tested))

	// Write Stage 2 details
	fmt.Fprintf(file, "=== Stage 2: Sustained Speed Test ===\n")
	fmt.Fprintf(file, "Tested:   %d nodes\n", s.Stage2Tested)
	fmt.Fprintf(file, "Passed:   %d nodes\n", s.Stage2Passed)
	fmt.Fprintf(file, "Filtered: %d nodes\n", s.Stage2Tested-s.Stage2Passed)
	fmt.Fprintf(file, "\nBreakdown of filtered nodes:\n")
	fmt.Fprintf(file, "  Collapsed (unstable speed): %d (%.1f%%)\n", s.Stage2Collapsed, pct(s.Stage2Collapsed, s.Stage2Tested))
	fmt.Fprintf(file, "  Below min speed:            %d (%.1f%%)\n", s.Stage2TooSlow, pct(s.Stage2TooSlow, s.Stage2Tested))
	fmt.Fprintf(file, "  Errors/Connection fails:    %d (%.1f%%)\n", s.Stage2Errors, pct(s.Stage2Errors, s.Stage2Tested))

	// Calculate Stage 2 timeouts (not explicitly tracked, estimate from difference)
	s2Other := s.Stage2Tested - s.Stage2Passed - s.Stage2Collapsed - s.Stage2TooSlow - s.Stage2Errors
	if s2Other > 0 {
		fmt.Fprintf(file, "  Timeouts/Other:             %d (%.1f%%)\n", s2Other, pct(s2Other, s.Stage2Tested))
	}

	fmt.Fprintf(file, "\n=== Summary ===\n")
	fmt.Fprintf(file, "Total input to speed testing: %d nodes\n", s.Stage1Tested)
	fmt.Fprintf(file, "Final candidates after all filtering: %d nodes\n", s.Stage2Passed)
	totalFiltered := s.Stage1Tested - s.Stage2Passed
	fmt.Fprintf(file, "Total filtered out: %d nodes (%.1f%%)\n", totalFiltered, pct(totalFiltered, s.Stage1Tested))

	Logf("Detailed speed test log written to: %s", logPath)
	return nil
}

// pct calculates percentage, handling divide by zero
func pct(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

// PrintSpeedTestReport prints a formatted speed test statistics report (no timestamps)
// Box width = 80 chars to match Mithril banner
func (s *SpeedTestStats) PrintSpeedTestReport() {
	// Check if terminal supports colors
	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	c := "" // color start
	r := "" // color reset
	if useColor {
		c = colorTeal
		r = colorReset
	}

	fmt.Println()
	fmt.Printf("%s┌──────────────────────────────────────────────────────────────────────────────┐%s\n", c, r)
	fmt.Printf("%s│%s SPEED TEST RESULTS                                                           %s│%s\n", c, r, c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	fmt.Printf("%s│%s Stage 1 (Fast Triage):                                                       %s│%s\n", c, r, c, r)
	fmt.Printf("%s│%s   Tested: %-5d  Passed: %-5d  Timeouts: %-5d  Errors: %-5d               %s│%s\n",
		c, r, s.Stage1Tested, s.Stage1Passed, s.Stage1Timeouts, s.Stage1Errors, c, r)
	fmt.Printf("%s│%s Stage 2 (Confirm):                                                           %s│%s\n", c, r, c, r)
	fmt.Printf("%s│%s   Tested: %-5d  Passed: %-5d  Collapsed: %-4d  Slow: %-5d                 %s│%s\n",
		c, r, s.Stage2Tested, s.Stage2Passed, s.Stage2Collapsed, s.Stage2TooSlow, c, r)
	fmt.Printf("%s└──────────────────────────────────────────────────────────────────────────────┘%s\n", c, r)
}

// RankedNodeInfo holds info about a ranked node for display purposes
type RankedNodeInfo struct {
	Rank    int
	RPC     string
	Version string
	RTTMs   int     // RTT in milliseconds
	SpeedS1 float64 // Stage 1 median speed
	SpeedS2 float64 // Stage 2 min speed (0 if not tested)
}

// PrintStage2CandidatesTable prints the staged speed candidates in a table format (no timestamps)
// Box width = 80 chars to match Mithril banner
func PrintStage2CandidatesTable(candidates []RankedNodeInfo) {
	if len(candidates) == 0 {
		return
	}

	// Check if terminal supports colors
	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	c := "" // color start
	r := "" // color reset
	if useColor {
		c = colorTeal
		r = colorReset
	}

	fmt.Println()
	fmt.Printf("%s┌──────────────────────────────────────────────────────────────────────────────┐%s\n", c, r)
	fmt.Printf("%s│%s STAGED SPEED CANDIDATES (top %d by speed)                                     %s│%s\n", c, r, len(candidates), c, r)
	fmt.Printf("%s├────┬─────────────────────────┬─────────┬───────┬─────────────────────────────┤%s\n", c, r)
	fmt.Printf("%s│%s #  │ Node IP                 │ Version │ RTT ms│ Speed (MB/s)                %s│%s\n", c, r, c, r)
	fmt.Printf("%s├────┼─────────────────────────┼─────────┼───────┼─────────────────────────────┤%s\n", c, r)

	for _, node := range candidates {
		// Truncate IP to fit column (23 chars)
		ip := node.RPC
		if len(ip) > 23 {
			ip = ip[:20] + "..."
		}
		// Truncate version to fit (7 chars)
		version := node.Version
		if len(version) > 7 {
			version = version[:7]
		}

		// Show S2 speed if available, otherwise S1
		speed := node.SpeedS2
		speedLabel := "S2"
		if speed == 0 {
			speed = node.SpeedS1
			speedLabel = "S1"
		}

		// Format RTT
		rttStr := fmt.Sprintf("%d", node.RTTMs)

		// Columns: # (4) + Node IP (25) + Version (9) + RTT (7) + Speed (29) = 74 inner + 6 borders = 80
		fmt.Printf("%s│%s %-2d │ %-23s │ %-7s │ %5s │ %6.1f (%s)                 %s│%s\n",
			c, r, node.Rank, ip, version, rttStr, speed, speedLabel, c, r)
	}

	fmt.Printf("%s└────┴─────────────────────────┴─────────┴───────┴─────────────────────────────┘%s\n", c, r)
	fmt.Println()
}
