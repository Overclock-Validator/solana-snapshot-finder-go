package main

import "github.com/spf13/viper"

type Config struct {
	RPCAddress       string   `mapstructure:"rpc_address"`
	SnapshotPath     string   `mapstructure:"snapshot_path"`
	MinDownloadSpeed int      `mapstructure:"min_download_speed"`
	MaxLatency       int      `mapstructure:"max_latency"`
	NumOfRetries     int      `mapstructure:"num_of_retries"`
	SleepBeforeRetry int      `mapstructure:"sleep_before_retry"`
	Blacklist        []string `mapstructure:"blacklist"` // Add blacklist support
}

func LoadConfig(configPath string) (Config, error) {
	var config Config

	// Configure viper
	viper.SetConfigName("config")   // Name of the file (without extension)
	viper.SetConfigType("yaml")     // Type of the file (yaml, json, etc.)
	viper.AddConfigPath(configPath) // Path to look for the config file
	viper.AddConfigPath(".")        // Current directory fallback

	// Set defaults
	viper.SetDefault("rpc_address", "https://api.mainnet-beta.solana.com")
	viper.SetDefault("snapshot_path", "./snapshots")
	viper.SetDefault("min_download_speed", 100)
	viper.SetDefault("max_latency", 200)
	viper.SetDefault("num_of_retries", 3)
	viper.SetDefault("sleep_before_retry", 5)
	viper.SetDefault("blacklist", []string{}) // Default to an empty blacklist

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
