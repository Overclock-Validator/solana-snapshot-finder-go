package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/config"
)

var version string

func main() {
	// Parse command-line arguments
	configPath := flag.String("config", "./config.yaml", "Path to the configuration file directory")
	versionFlag := flag.Bool("version", false, "Show version information")
	flag.Parse()

	// If version flag is passed, print the version and exit
	if *versionFlag {
		if version == "" {
			version = "unknown" // In case version is not set, fallback to 'unknown'
		}
		fmt.Printf("Solana Snapshot Finder, Version: %s\n", version)
		os.Exit(0)
	}

	// Load the configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Println("Starting Solana Snapshot Finder...")
	log.Printf("Loaded Config: %+v", cfg)

	// Manage snapshots, including fetching the reference slot internally
	processSnapshots(cfg)
}
