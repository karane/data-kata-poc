package watcher

import (
	"context"
	"time"
)

// FileEvent represents a detected file in the watch directory.
type FileEvent struct {
	Path      string
	Timestamp time.Time
}

// Watcher interface for file system monitoring.
type Watcher interface {
	Start(ctx context.Context) error
	Stop() error
	Events() <-chan FileEvent
	Errors() <-chan error
}

// Config for watcher behavior.
type Config struct {
	WatchDir     string
	ProcessedDir string
	FailedDir    string
	PollInterval time.Duration
}
