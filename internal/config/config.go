package config

import (
	"fmt"
	"os"
)


type Config struct {
	DatabaseURL string
	ServerPort  string
	LogLevel    string
}

// Load reads config from environment variables.
// Returns an error if any required variable is missing.
func Load() (*Config, error) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, fmt.Errorf("DATABASE_URL environment variable is required")
	}

	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080" // sensible default
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	return &Config{
		DatabaseURL: dbURL,
		ServerPort:  port,
		LogLevel:    logLevel,
	}, nil
}
