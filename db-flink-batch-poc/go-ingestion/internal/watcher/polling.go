package watcher

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/data-kata-poc/go-ingestion/internal/domain"
	"github.com/data-kata-poc/go-ingestion/internal/service"
	"github.com/data-kata-poc/go-ingestion/pkg/parser"
)

// PollingWatcher polls a directory for new files.
type PollingWatcher struct {
	config       Config
	events       chan FileEvent
	errors       chan error
	done         chan struct{}
	wg           sync.WaitGroup
	seenFiles    map[string]struct{}
	mu           sync.Mutex
	parserReg    *parser.Registry
	salesService *service.SalesService
}

// NewPollingWatcher creates a new polling-based file watcher.
func NewPollingWatcher(cfg Config, parserReg *parser.Registry, salesService *service.SalesService) *PollingWatcher {
	return &PollingWatcher{
		config:       cfg,
		events:       make(chan FileEvent, 100),
		errors:       make(chan error, 10),
		done:         make(chan struct{}),
		seenFiles:    make(map[string]struct{}),
		parserReg:    parserReg,
		salesService: salesService,
	}
}

// Start begins watching the directory.
func (w *PollingWatcher) Start(ctx context.Context) error {
	// Ensure directories exist
	for _, dir := range []string{w.config.WatchDir, w.config.ProcessedDir, w.config.FailedDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	w.wg.Add(1)
	go w.pollLoop(ctx)

	log.Info().
		Str("watch_dir", w.config.WatchDir).
		Dur("interval", w.config.PollInterval).
		Msg("file watcher started")

	return nil
}

// Stop stops the watcher.
func (w *PollingWatcher) Stop() error {
	close(w.done)
	w.wg.Wait()
	close(w.events)
	close(w.errors)
	return nil
}

// Events returns the channel of file events.
func (w *PollingWatcher) Events() <-chan FileEvent {
	return w.events
}

// Errors returns the channel of errors.
func (w *PollingWatcher) Errors() <-chan error {
	return w.errors
}

func (w *PollingWatcher) pollLoop(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	// Do an initial scan
	w.scanDirectory()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		case <-ticker.C:
			w.scanDirectory()
		}
	}
}

func (w *PollingWatcher) scanDirectory() {
	entries, err := os.ReadDir(w.config.WatchDir)
	if err != nil {
		w.errors <- err
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		fullPath := filepath.Join(w.config.WatchDir, filename)

		// Check if we've already processed this file
		w.mu.Lock()
		_, seen := w.seenFiles[filename]
		if !seen {
			w.seenFiles[filename] = struct{}{}
		}
		w.mu.Unlock()

		if seen {
			continue
		}

		// Check if we have a parser for this file type
		ext := strings.ToLower(filepath.Ext(filename))
		if ext != ".csv" && ext != ".json" && ext != ".jsonl" {
			log.Debug().Str("file", filename).Msg("skipping unsupported file type")
			continue
		}

		// Process the file
		w.processFile(fullPath)
	}
}

func (w *PollingWatcher) processFile(path string) {
	filename := filepath.Base(path)
	log.Info().Str("file", filename).Msg("processing file")

	// Get parser
	p, err := w.parserReg.ParserFor(filename)
	if err != nil {
		log.Error().Err(err).Str("file", filename).Msg("no parser available")
		w.moveToFailed(path)
		return
	}

	// Open and parse file
	file, err := os.Open(path)
	if err != nil {
		log.Error().Err(err).Str("file", filename).Msg("failed to open file")
		w.moveToFailed(path)
		return
	}
	defer file.Close()

	records, err := p.Parse(file)
	if err != nil {
		log.Error().Err(err).Str("file", filename).Msg("failed to parse file")
		w.moveToFailed(path)
		return
	}

	// Convert to SaleEvents
	sales := make([]domain.SaleEvent, len(records))
	for i, r := range records {
		sales[i] = r.ToSaleEvent()
	}

	// Ingest
	if err := w.salesService.IngestBatch(sales); err != nil {
		log.Error().Err(err).Str("file", filename).Msg("failed to ingest sales")
		w.moveToFailed(path)
		return
	}

	log.Info().
		Str("file", filename).
		Int("records", len(sales)).
		Msg("successfully ingested file")

	// Move to processed
	w.moveToProcessed(path)

	// Send event
	w.events <- FileEvent{
		Path:      path,
		Timestamp: time.Now(),
	}
}

func (w *PollingWatcher) moveToProcessed(path string) {
	filename := filepath.Base(path)
	dest := filepath.Join(w.config.ProcessedDir, filename)
	if err := os.Rename(path, dest); err != nil {
		log.Error().Err(err).Str("file", filename).Msg("failed to move to processed")
	}
}

func (w *PollingWatcher) moveToFailed(path string) {
	filename := filepath.Base(path)
	dest := filepath.Join(w.config.FailedDir, filename)
	if err := os.Rename(path, dest); err != nil {
		log.Error().Err(err).Str("file", filename).Msg("failed to move to failed")
	}
}
