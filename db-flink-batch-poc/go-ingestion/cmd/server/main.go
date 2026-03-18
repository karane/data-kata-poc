package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/data-kata-poc/go-ingestion/internal/api"
	"github.com/data-kata-poc/go-ingestion/internal/config"
	"github.com/data-kata-poc/go-ingestion/internal/repository"
	"github.com/data-kata-poc/go-ingestion/internal/service"
	"github.com/data-kata-poc/go-ingestion/internal/watcher"
	"github.com/data-kata-poc/go-ingestion/pkg/parser"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load configuration")
	}

	// Setup logging
	setupLogging(cfg.Log)

	log.Info().Msg("starting go-ingestion service")

	// Initialize repositories
	salesRepo := repository.NewInMemorySalesRepo()
	productRepo := repository.NewInMemoryProductRepo()

	// Initialize services
	salesService := service.NewSalesService(salesRepo)
	productService := service.NewProductService(productRepo)

	// Initialize parser registry
	parserReg := parser.NewRegistry()

	// Initialize file watcher
	watcherCfg := watcher.Config{
		WatchDir:     cfg.Watcher.WatchDir,
		ProcessedDir: cfg.Watcher.ProcessedDir,
		FailedDir:    cfg.Watcher.FailedDir,
		PollInterval: cfg.Watcher.PollInterval,
	}
	fileWatcher := watcher.NewPollingWatcher(watcherCfg, parserReg, salesService)

	// Start file watcher
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := fileWatcher.Start(ctx); err != nil {
		log.Fatal().Err(err).Msg("failed to start file watcher")
	}

	// Initialize router
	router := api.NewRouter(salesService, productService)
	router.Setup()

	// Create HTTP server
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      router.Engine(),
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// Start server in goroutine
	go func() {
		log.Info().Int("port", cfg.Server.Port).Msg("HTTP server starting")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server error")
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info().Msg("shutting down...")

	// Stop file watcher
	cancel()
	if err := fileWatcher.Stop(); err != nil {
		log.Error().Err(err).Msg("error stopping file watcher")
	}

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("error during server shutdown")
	}

	log.Info().Msg("server stopped")
}

func setupLogging(cfg config.LogConfig) {
	// Set log level
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	// Set output format
	if cfg.Format == "console" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	// Add timestamp
	zerolog.TimeFieldFormat = time.RFC3339
}
