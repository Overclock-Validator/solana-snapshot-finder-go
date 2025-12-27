package rpc

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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

// PrintReport logs a comprehensive statistics report
func (s *ProbeStats) PrintReport(cfg FilterConfig) {
	// Header
	log.Println("")
	log.Println("╔══════════════════════════════════════════════════════════════════╗")
	log.Println("║                    NODE DISCOVERY REPORT                         ║")
	log.Println("╚══════════════════════════════════════════════════════════════════╝")
	log.Println("")

	// Probing Summary
	log.Println("┌─────────────────────────────────────────────────────────────────┐")
	log.Println("│ PROBING SUMMARY                                                 │")
	log.Println("├─────────────────────────────────────────────────────────────────┤")
	log.Printf("│ Total nodes discovered: %-40d│\n", atomic.LoadInt64(&s.TotalNodes))
	log.Printf("│ TCP precheck failed:    %-40d│\n", atomic.LoadInt64(&s.TCPFailed))
	log.Printf("│ With snapshots:         %-40d│\n", atomic.LoadInt64(&s.HasAnySnapshot))
	log.Printf("│ Without snapshots:      %-40d│\n", atomic.LoadInt64(&s.NoSnapshot))
	log.Printf("│ Probe timeouts:         %-40d│\n", atomic.LoadInt64(&s.ProbeTimeouts))
	log.Printf("│ Other probe errors:     %-40d│\n", atomic.LoadInt64(&s.ProbeOtherErr))
	log.Println("└─────────────────────────────────────────────────────────────────┘")
	log.Println("")

	// RTT Histogram
	log.Println("┌─────────────────────────────────────────────────────────────────┐")
	log.Println("│ RTT HISTOGRAM (milliseconds)                                    │")
	log.Println("├─────────────────────────────────────────────────────────────────┤")
	s.printHistogramBar("0-100", atomic.LoadInt64(&s.RTT0To100), s.TotalNodes)
	s.printHistogramBar("100-200", atomic.LoadInt64(&s.RTT100To200), s.TotalNodes)
	s.printHistogramBar("200-300", atomic.LoadInt64(&s.RTT200To300), s.TotalNodes)
	s.printHistogramBar("300-400", atomic.LoadInt64(&s.RTT300To400), s.TotalNodes)
	s.printHistogramBar(">400", atomic.LoadInt64(&s.RTTOver400), s.TotalNodes)
	log.Println("└─────────────────────────────────────────────────────────────────┘")
	log.Println("")

	// Version Distribution
	log.Println("┌─────────────────────────────────────────────────────────────────┐")
	log.Println("│ NODE VERSION DISTRIBUTION (top 10)                              │")
	log.Println("├─────────────────────────────────────────────────────────────────┤")
	topVersions := s.getTopVersions(10)
	for _, v := range topVersions {
		s.printHistogramBar(v.Version, v.Count, s.HasAnySnapshot)
	}
	log.Println("└─────────────────────────────────────────────────────────────────┘")
	log.Println("")

	// Incremental Snapshot Stats
	log.Println("┌─────────────────────────────────────────────────────────────────┐")
	log.Println("│ INCREMENTAL SNAPSHOT DISTRIBUTION                               │")
	log.Println("├─────────────────────────────────────────────────────────────────┤")
	withInc := atomic.LoadInt64(&s.WithIncremental)
	withoutInc := atomic.LoadInt64(&s.WithoutIncremental)
	log.Printf("│ With incremental:    %-44d│\n", withInc)
	log.Printf("│ Without incremental: %-44d│\n", withoutInc)
	log.Printf("│ With usable inc:     %-44d│\n", atomic.LoadInt64(&s.WithUsableInc))
	log.Println("│                                                                 │")
	log.Println("│ Distance from tip (slots):                                      │")
	s.printHistogramBar("ahead", atomic.LoadInt64(&s.IncAhead), withInc)
	if s.MaxAheadSlots > 0 {
		log.Printf("│   (max ahead: %d slots)%s│\n", s.MaxAheadSlots, strings.Repeat(" ", 43-len(fmt.Sprintf("%d", s.MaxAheadSlots))))
	}
	s.printHistogramBar("0-100", atomic.LoadInt64(&s.Inc0To100), withInc)
	s.printHistogramBar("100-200", atomic.LoadInt64(&s.Inc100To200), withInc)
	s.printHistogramBar("200-1000", atomic.LoadInt64(&s.Inc200To1000), withInc)
	s.printHistogramBar("1000+", atomic.LoadInt64(&s.Inc1000Plus), withInc)
	log.Println("└─────────────────────────────────────────────────────────────────┘")
	log.Println("")

	// Filter Pipeline
	log.Println("┌─────────────────────────────────────────────────────────────────┐")
	log.Println("│ FILTER PIPELINE                                                 │")
	log.Println("├─────────────────────────────────────────────────────────────────┤")
	s.printFilterRow("Initial (with snapshots)", s.InitialWithSnapshots, 0)

	if cfg.MinVersion != "" || len(cfg.AllowedVersions) > 0 {
		versionDesc := cfg.MinVersion
		if len(cfg.AllowedVersions) > 0 {
			versionDesc = fmt.Sprintf("[%s]", strings.Join(cfg.AllowedVersions, ", "))
		}
		filtered := s.InitialWithSnapshots - s.AfterVersionFilter
		label := fmt.Sprintf("After version filter (%s)", versionDesc)
		s.printFilterRow(label, s.AfterVersionFilter, filtered)
	}

	if cfg.MaxRTTMs > 0 {
		filtered := s.AfterVersionFilter - s.AfterRTTFilter
		if s.AfterVersionFilter == 0 {
			filtered = s.InitialWithSnapshots - s.AfterRTTFilter
		}
		label := fmt.Sprintf("After RTT filter (%dms)", cfg.MaxRTTMs)
		s.printFilterRow(label, s.AfterRTTFilter, filtered)
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
	s.printFilterRow(label, s.AfterFullAgeFilter, filtered)

	filtered = s.AfterFullAgeFilter - s.AfterIncAgeFilter
	label = fmt.Sprintf("After inc_threshold (%d slots)", cfg.IncThreshold)
	s.printFilterRow(label, s.AfterIncAgeFilter, filtered)

	s.printFilterRow("Eligible for speed testing", atomic.LoadInt64(&s.Eligible), 0)
	log.Println("└─────────────────────────────────────────────────────────────────┘")
	log.Println("")
}

// printHistogramBar prints a histogram bar for the report
func (s *ProbeStats) printHistogramBar(label string, count int64, total int64) {
	maxBarWidth := 30
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

	log.Printf("│ %s %s %5d (%5.1f%%)      │\n", paddedLabel, bar, count, pct)
}

// printFilterRow prints a row for the filter pipeline section
// If filtered is 0, it shows just the count; otherwise shows "count (-filtered)"
func (s *ProbeStats) printFilterRow(label string, count int64, filtered int64) {
	// Box inner width is 65 chars (between the │ borders)
	// Format: "│ " + label + value_part + " │"
	// So content area is 63 chars (65 - 2 for "│ " and " │" at the end is wrong, let me recalculate)
	// Actually: "│ " (2) + content (63) + "│" (1) = 66 chars, but box is 67 including both borders
	// Border line has 65 dashes, so inner content is 65 chars
	// Format: "│" (1) + " " (1) + content (62) + " " (1) + "│" (1) = 66? Let me just use fixed format

	const innerWidth = 63 // Usable content width between "│ " and " │"

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

	log.Printf("│ %s%s%s │\n", label, strings.Repeat(" ", padding), valueStr)
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

// PrintSpeedTestReport logs a formatted speed test statistics report
func (s *SpeedTestStats) PrintSpeedTestReport() {
	log.Println("")
	log.Println("┌─────────────────────────────────────────────────────────────────┐")
	log.Println("│ SPEED TEST RESULTS                                              │")
	log.Println("├─────────────────────────────────────────────────────────────────┤")
	log.Printf("│ Stage 1 (Fast Triage):                                          │\n")
	log.Printf("│   Tested: %-5d  Passed: %-5d  Timeouts: %-5d  Errors: %-5d   │\n",
		s.Stage1Tested, s.Stage1Passed, s.Stage1Timeouts, s.Stage1Errors)
	log.Printf("│ Stage 2 (Confirm):                                              │\n")
	log.Printf("│   Tested: %-5d  Passed: %-5d  Collapsed: %-4d  Too slow: %-4d │\n",
		s.Stage2Tested, s.Stage2Passed, s.Stage2Collapsed, s.Stage2TooSlow)
	log.Println("└─────────────────────────────────────────────────────────────────┘")
}
