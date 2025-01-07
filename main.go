package main

import (
	"flag"
	"log"
)

func main() {
	// Parse command-line arguments
	configPath := flag.String("config", "./config.yaml", "Path to the configuration file directory")
	flag.Parse()

	// Load the configuration
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Println("Starting Solana Snapshot Finder...")
	log.Printf("Loaded Config: %+v", config)

	// Manage snapshots, including fetching the reference slot internally
	processSnapshots(config)
}
