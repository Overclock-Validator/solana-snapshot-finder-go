package main

import (
	"flag"
)

type Config struct {
	ThreadsCount     int
	RPCAddress       string
	Slot             int
	Version          string
	MaxSnapshotAge   int
	MinDownloadSpeed int
	MaxLatency       int
	SnapshotPath     string
	NumOfRetries     int
	SleepBeforeRetry int
}

func ParseConfig() Config {
	config := Config{}
	flag.IntVar(&config.ThreadsCount, "threads-count", 1000, "Number of concurrent threads")
	flag.StringVar(&config.RPCAddress, "rpc-address", "https://api.mainnet-beta.solana.com", "RPC node address")
	flag.IntVar(&config.Slot, "slot", 0, "Search for a specific slot")
	flag.StringVar(&config.Version, "version", "", "Snapshot version")
	flag.IntVar(&config.MaxSnapshotAge, "max-snapshot-age", 1300, "Max snapshot age in slots")
	flag.IntVar(&config.MinDownloadSpeed, "min-download-speed", 60, "Min download speed in MB/s")
	flag.IntVar(&config.MaxLatency, "max-latency", 100, "Max latency in ms")
	flag.StringVar(&config.SnapshotPath, "snapshot-path", "./snapshots", "Snapshot download path")
	flag.IntVar(&config.NumOfRetries, "num-of-retries", 5, "Number of retries")
	flag.IntVar(&config.SleepBeforeRetry, "sleep", 7, "Sleep duration between retries")
	flag.Parse()

	return config
}
