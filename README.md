# Solana Snapshot Finder Go

## Overview

Solana Snapshot Finder is a Go utility designed to efficiently manage Solana blockchain snapshots. It automates the process of finding, downloading, and maintaining up-to-date snapshots for Solana validators, helping to reduce node restart times and ensure reliable operation.

## Key Features

- **Smart Snapshot Detection**: Identifies when new snapshots are needed based on configurable thresholds
- **Redundant Download Prevention**: Compares remote snapshot slots with local snapshots to avoid unnecessary downloads
- **RPC Node Selection**: Evaluates and selects the best RPC nodes for downloads based on latency and download speed
- **Automated Cleanup**: Removes outdated snapshots to save disk space while maintaining necessary backups
- **Full & Incremental Support**: Handles both full and incremental snapshots with proper slot validation
- **Graceful Cancellation**: Ctrl+C cleanly stops downloads and removes incomplete files
- **Disk Space Reporting**: Shows total disk space, space used by snapshots, and available space
- **Snapshot Retention**: Configurable limit on how many full snapshots to keep (oldest are automatically deleted)
- **Expiration Safety**: Warns when snapshots approach expiration and can pause full downloads to fetch incrementals first

## Configuration

The tool uses a YAML configuration file with the following parameters:

```yaml
rpc_addresses:                                      # Multiple RPC endpoints (uses highest slot)
  - "https://api.mainnet-beta.solana.com"
  # - "https://backup-rpc.example.com"
snapshot_path: "./snapshots"                        # Base directory to store snapshots
max_latency: 200                                    # Maximum acceptable latency (ms)
num_of_retries: 3                                   # Number of download retry attempts
sleep_before_retry: 5                               # Time between retries (seconds)
blacklist: []                                       # RPC nodes to exclude
enable_blacklist: true                              # Enable/disable blacklist filtering
whitelist: []                                       # Custom snapshot sources (URLs or RPC endpoints)
whitelist_mode: "additional"                        # "only", "additional", or "disabled"
worker_count: 100                                   # Number of concurrent evaluation workers
full_threshold: 20000                               # Slots behind for full snapshots (Agave 2.0: ~25k validity)
incremental_threshold: 200                          # Slots diff for incrementals (allows slightly ahead)

# Stage 1 (Fast Triage) - Quick download speed sampling
stage1_warm_kib: 256                                # Warmup data in KiB before timing windows
stage1_window_kib: 512                              # Size of each measurement window in KiB
stage1_windows: 2                                   # Number of measurement windows
stage1_timeout_ms: 5000                             # Timeout for fast sampling in milliseconds
stage1_concurrency: 0                               # Concurrent tests (0 = auto-calculate)

# Stage 2 (Confirm) - Accurate download speed measurement (tests run sequentially)
stage2_top_k: 5                                     # Number of top candidates to test in Stage 2
stage2_warm_sec: 2                                  # Warmup duration in seconds (discards data)
stage2_measure_sec: 3                               # Measurement duration in seconds (all data counts)
stage2_min_ratio: 0.6                               # Collapse guard: reject if min_speed < ratio * avg_speed
# stage2_min_abs_mbs: 0                             # Minimum absolute speed in MB/s (0 = disabled, default)

# RTT filtering (after snapshot filters, before speed tests)
max_rtt_ms: 0                                       # Max RTT in ms to proceed to speed tests (0 = disabled)

# Version filtering (filters nodes by Solana/Agave version)
min_node_version: "2.2.0"                           # Minimum node version (empty = no minimum)
allowed_node_versions: []                           # Specific versions to allow (empty = use min_node_version)

# Snapshot retention - controls how many snapshots are kept on disk
max_full_snapshots: 2                               # Max full snapshots to keep (0 = unlimited)
delete_old_snapshots: false                         # Enable age-based deletion (default: false)

# Download safety - ensures valid incremental exists during slow full downloads
download_incremental_first: true                    # Download incremental before full snapshot (default: true)

# Safety margin for expiration warnings (uses full_threshold as validity window)
safety_margin_slots: 2000                           # Warning window before expiration (shows time remaining)
```

### Whitelist and Blacklist Configuration

**Whitelist**: Custom snapshot sources that can be either:
- Static snapshot URLs (e.g., `https://snapshots.avorio.network/mainnet-beta/`)
- Regular RPC endpoints (e.g., `http://custom-node:8899`)

**Whitelist Modes**:
- `"only"` - Use only whitelist sources, skip public node discovery
- `"additional"` (default) - Combine whitelist with public nodes
- `"disabled"` - Ignore whitelist, use only public nodes

**Blacklist**: IP addresses or hostnames to exclude from selection. Enable/disable with `enable_blacklist` boolean.

## How It Works

1. **Snapshot Evaluation**:
   - Checks if existing snapshots are outdated by comparing slot differences
   - Full snapshots are updated if their slot differs from the reference by more than `full_threshold`
   - Incremental snapshots are updated if they differ by more than `incremental_threshold`

2. **RPC Selection**:
   - Fetches available RPC nodes from the Solana cluster (or whitelist)
   - Probes each node for snapshot availability using HEAD requests
   - Filters nodes based on latency and snapshot age thresholds
   - **Stage 1 (Fast Triage)**: Quick download speed sampling on all eligible nodes
     - Downloads small sample (warmup + measurement windows)
     - Calculates median speed to identify fast candidates
     - Uses HTTP Range requests to minimize data transfer
   - **Stage 2 (Accurate Confirmation)**: Detailed measurement on top performers
     - Tests top K candidates with longer sampling duration
     - Uses time-based buckets to measure stable average speed
     - Applies collapse guard to reject unstable connections
   - Selects the node with the fastest confirmed download speed

3. **Download Optimization**:
   - Makes a HEAD request to check remote snapshot details before downloading
   - Extracts the filename and slot information from response headers
   - Skips downloads if the remote snapshot is not newer than the local snapshot
   - Downloads to a temporary file first, then copies to final location
   - Maintains a backup during the process for safety

4. **Snapshot Management**:
   - Stores snapshots in organized directories (`snapshot_path` and `snapshot_path/remote`)
   - Maintains temporary and backup files for resilience
   - Cleans up old snapshots based on configurable thresholds

## Usage

When integrated with Solana validator startup, the tool will:

1. Check if current snapshots are up-to-date
2. Find and evaluate the best RPC nodes for snapshot downloads
3. Efficiently download only the necessary snapshots
4. Clean up outdated snapshots to manage disk space

## Implementation Details

- Uses standard Go HTTP client with proper timeout and redirect handling
- Implements progress tracking for large downloads with estimated time remaining
- Employs regex pattern matching to extract slot information from filenames
- Maintains backup files to prevent data loss during download operations
- Performs verification at multiple stages to ensure snapshot integrity
- Supports graceful shutdown via context cancellation (Ctrl+C)
- Async cleanup to avoid blocking downloads

### Graceful Shutdown

Press Ctrl+C at any time to gracefully stop the process:
- Any incomplete downloads are automatically cleaned up
- Temporary files are removed
- The process exits with code 130

### Snapshot Retention

The tool can automatically manage disk space by limiting how many full snapshots are kept:
- Set `max_full_snapshots` to the maximum number of full snapshots to retain
- When the limit is exceeded, the oldest full snapshots are deleted
- Associated incremental snapshots are also deleted when their base full snapshot is removed
- Set to 0 for unlimited retention

### Expiration Safety

The tool warns when current snapshots are approaching expiration:
- If snapshots are within the safety margin (default 2000 slots), a warning is displayed
- The warning shows approximate time remaining (assuming ~400ms per slot)
- Users are informed they can press Ctrl+C to cancel if the download won't complete in time
- After the full download completes, a fresh incremental snapshot is automatically downloaded

## Best Practices

- Adjust thresholds based on your network's slot advancement rate and restart frequency
- Use a dedicated disk with sufficient space for snapshots (95GB+ for full snapshots)

This tool significantly improves Solana validator startup times by intelligently managing snapshots and preventing redundant downloads of large files. 