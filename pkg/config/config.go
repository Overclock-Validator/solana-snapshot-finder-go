package config

import "github.com/spf13/viper"

type Config struct {
	RPCAddresses         []string `mapstructure:"rpc_addresses"` // Multiple RPC endpoints to query for reference slot
	SnapshotPath         string   `mapstructure:"snapshot_path"`
	NumOfRetries         int      `mapstructure:"num_of_retries"`
	SleepBeforeRetry     int      `mapstructure:"sleep_before_retry"`
	Blacklist            []string `mapstructure:"blacklist"`
	EnableBlacklist      bool     `mapstructure:"enable_blacklist"`
	Whitelist            []string `mapstructure:"whitelist"`
	WhitelistMode        string   `mapstructure:"whitelist_mode"`
	WorkerCount          int      `mapstructure:"worker_count"`
	FullThreshold        int      `mapstructure:"full_threshold"`
	IncrementalThreshold int      `mapstructure:"incremental_threshold"`

	// Stage 1 (Fast Triage) configuration
	Stage1WarmKiB     int64 `mapstructure:"stage1_warm_kib"`
	Stage1WindowKiB   int64 `mapstructure:"stage1_window_kib"`
	Stage1Windows     int   `mapstructure:"stage1_windows"`
	Stage1TimeoutMS   int64 `mapstructure:"stage1_timeout_ms"`
	Stage1Concurrency int   `mapstructure:"stage1_concurrency"`

	// Stage 2 (Confirm) configuration
	Stage2TopK       int     `mapstructure:"stage2_top_k"`
	Stage2WarmSec    int     `mapstructure:"stage2_warm_sec"`
	Stage2MeasureSec int     `mapstructure:"stage2_measure_sec"`
	Stage2MinRatio   float64 `mapstructure:"stage2_min_ratio"`
	Stage2MinAbsMBs  float64 `mapstructure:"stage2_min_abs_mbs"` // Minimum absolute speed in MB/s

	// RTT filtering (before speed tests)
	MaxRTTMs int `mapstructure:"max_rtt_ms"` // Max RTT in ms to proceed to speed tests (0 = no limit)

	// TCP precheck timeout
	TCPTimeoutMs int `mapstructure:"tcp_timeout_ms"` // TCP precheck timeout in ms (default: 1000)

	// Version filtering
	MinNodeVersion      string   `mapstructure:"min_node_version"`      // Minimum node version (e.g., "2.2.0")
	AllowedNodeVersions []string `mapstructure:"allowed_node_versions"` // Specific versions to allow (empty = use min_node_version)

	// Snapshot retention - controls how many snapshots are kept on disk
	MaxFullSnapshots   int  `mapstructure:"max_full_snapshots"`   // Max full snapshots to keep (0=unlimited, default: 2). Oldest are deleted when limit exceeded.
	DeleteOldSnapshots bool `mapstructure:"delete_old_snapshots"` // Enable age-based deletion (default: false). When false, only retention limit applies.

	// Download safety - download incremental before full to ensure valid snapshot during long full downloads
	DownloadIncrementalFirst bool `mapstructure:"download_incremental_first"` // Download incremental before full snapshot (default: true). Ensures valid incremental exists if full download exceeds expiry window.

	// Safety margin for expiration warnings
	SafetyMarginSlots int `mapstructure:"safety_margin_slots"` // Slots before expiration to trigger warnings (default: 2000). Uses full_threshold as validity window.

	// Logging control
	Quiet bool `mapstructure:"quiet"` // Suppress stage 1/2 detailed logging (default: false)
}

func LoadConfig(configPath string) (Config, error) {
	var config Config

	// Configure viper
	viper.SetConfigFile(configPath) // Use the full path to the configuration file

	// Set defaults
	viper.SetDefault("rpc_addresses", []string{"https://api.mainnet-beta.solana.com"})
	viper.SetDefault("snapshot_path", "./snapshots")
	viper.SetDefault("num_of_retries", 3)
	viper.SetDefault("sleep_before_retry", 5)
	viper.SetDefault("blacklist", []string{})
	viper.SetDefault("enable_blacklist", true)
	viper.SetDefault("whitelist", []string{})
	viper.SetDefault("whitelist_mode", "additional")
	viper.SetDefault("worker_count", 100)
	viper.SetDefault("full_threshold", 100000)       // Agave 2.0: incrementals valid for 25k slots after full. Agave 3.0: 100k after full
	viper.SetDefault("incremental_threshold", 200) // Allow slightly ahead incrementals

	// Stage 1 (Fast Triage) defaults
	viper.SetDefault("stage1_warm_kib", 256)
	viper.SetDefault("stage1_window_kib", 512)
	viper.SetDefault("stage1_windows", 2)
	viper.SetDefault("stage1_timeout_ms", 5000)
	viper.SetDefault("stage1_concurrency", 0) // 0 = auto-calculate

	// Stage 2 (Confirm) defaults - tests run sequentially for accurate measurements
	// Timeout is auto-calculated as warm + measure + 5s connection overhead
	viper.SetDefault("stage2_top_k", 5)
	viper.SetDefault("stage2_warm_sec", 2)
	viper.SetDefault("stage2_measure_sec", 3)
	viper.SetDefault("stage2_min_ratio", 0.6)
	viper.SetDefault("stage2_min_abs_mbs", 0) // Minimum absolute speed in MB/s (0 = disabled)

	// RTT filtering defaults
	viper.SetDefault("max_rtt_ms", 200) // Max RTT in ms (0 = no limit)

	// TCP precheck timeout defaults
	viper.SetDefault("tcp_timeout_ms", 1000) // 1000ms for home internet compatibility

	// Version filtering defaults
	viper.SetDefault("min_node_version", "2.2.0")         // Minimum Agave version
	viper.SetDefault("allowed_node_versions", []string{}) // Empty = use min_node_version

	// Snapshot retention defaults
	viper.SetDefault("max_full_snapshots", 2)   // Keep up to 2 full snapshots
	viper.SetDefault("delete_old_snapshots", false) // Don't auto-delete by age, only by retention limit

	// Download safety defaults
	viper.SetDefault("download_incremental_first", true) // Download incremental before full for safety

	// Safety margin defaults (uses full_threshold as validity window)
	viper.SetDefault("safety_margin_slots", 2000) // Warn when 2000 slots from expiration

	// Logging control defaults
	viper.SetDefault("quiet", false) // Show detailed stage logging by default

	// Read the config
	err := viper.ReadInConfig()
	if err != nil {
		return config, err
	}

	// Unmarshal the config into the struct
	err = viper.Unmarshal(&config)
	if err != nil {
		return config, err
	}

	return config, nil
}
