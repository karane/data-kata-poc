package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config holds all configuration for the application.
type Config struct {
	Server  ServerConfig
	Watcher WatcherConfig
	Log     LogConfig
}

// ServerConfig contains HTTP server settings.
type ServerConfig struct {
	Port         int           `envconfig:"SERVER_PORT" default:"8080"`
	ReadTimeout  time.Duration `envconfig:"SERVER_READ_TIMEOUT" default:"30s"`
	WriteTimeout time.Duration `envconfig:"SERVER_WRITE_TIMEOUT" default:"30s"`
}

// WatcherConfig contains file watcher settings.
type WatcherConfig struct {
	WatchDir     string        `envconfig:"WATCH_DIR" default:"./data/inbox"`
	ProcessedDir string        `envconfig:"PROCESSED_DIR" default:"./data/processed"`
	FailedDir    string        `envconfig:"FAILED_DIR" default:"./data/failed"`
	PollInterval time.Duration `envconfig:"POLL_INTERVAL" default:"5s"`
}

// LogConfig contains logging settings.
type LogConfig struct {
	Level  string `envconfig:"LOG_LEVEL" default:"info"`
	Format string `envconfig:"LOG_FORMAT" default:"json"`
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
