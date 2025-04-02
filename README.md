# Solana Snapshot Finder Go

## Overview

Solana Snapshot Finder is a Go utility designed to efficiently manage Solana blockchain snapshots. It automates the process of finding, downloading, and maintaining up-to-date snapshots for Solana validators, helping to reduce node restart times and ensure reliable operation.

## Key Features

- **Smart Snapshot Detection**: Identifies when new snapshots are needed based on configurable thresholds
- **Redundant Download Prevention**: Compares remote snapshot slots with local snapshots to avoid unnecessary downloads
- **RPC Node Selection**: Evaluates and selects the best RPC nodes for downloads based on latency and download speed
- **Automated Cleanup**: Removes outdated snapshots to save disk space while maintaining necessary backups
- **Full & Incremental Support**: Handles both full and incremental snapshots with proper slot validation

## Configuration

The tool uses a YAML configuration file with the following parameters:

```yaml
rpc_address: "https://api.mainnet-beta.solana.com"  # Default RPC if no better nodes found
snapshot_path: "./snapshots"                        # Base directory to store snapshots
min_download_speed: 100                             # Minimum acceptable download speed (MB/s)
max_latency: 200                                    # Maximum acceptable latency (ms)
num_of_retries: 3                                   # Number of download retry attempts
sleep_before_retry: 5                               # Time between retries (seconds)
blacklist: []                                       # RPC nodes to exclude
private_rpc: false                                  # Whether to use private RPCs
worker_count: 100                                   # Number of concurrent evaluation workers
full_threshold: 25000                               # Slots threshold for full snapshot updates
incremental_threshold: 1000                         # Slots threshold for incremental updates
```

## How It Works

1. **Snapshot Evaluation**:
   - Checks if existing snapshots are outdated by comparing slot differences
   - Full snapshots are updated if their slot differs from the reference by more than `full_threshold`
   - Incremental snapshots are updated if they differ by more than `incremental_threshold`

2. **Download Optimization**:
   - Makes a HEAD request to check remote snapshot details before downloading
   - Extracts the filename and slot information from response headers
   - Skips downloads if the remote snapshot is not newer than the local snapshot
   - Downloads to a temporary file first, then copies to final location
   - Maintains a backup during the process for safety

3. **RPC Selection**:
   - Evaluates multiple RPC nodes for performance metrics
   - Selects the fastest and most reliable RPC node for downloads
   - Supports blacklisting of problematic nodes

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

## Best Practices

- Adjust thresholds based on your network's slot advancement rate and restart frequency
- Use a dedicated disk with sufficient space for snapshots (95GB+ for full snapshots)

This tool significantly improves Solana validator startup times by intelligently managing snapshots and preventing redundant downloads of large files. 