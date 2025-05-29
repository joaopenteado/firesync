// Package main is the main package for the firesync application.
//
// It's sole purpose is to manage the lifecycle of the service, including
// starting and stopping the service, and handling the graceful shutdown of the
// application.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/joaopenteado/firesync/internal/service"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := run(ctx); err != nil {
		// Ensure the root context is called, since os.Exit() does not call
		// deferred functions.
		cancel()

		log.Fatal().Err(err).Msg("failed to run service")
	}
}

func run(ctx context.Context) error {
	// GracefulShutdownTimeout represents how long the service has to gracefully
	// terminate after receiving a SIGTERM or SIGINT signal.
	// Cloud Run will forcefully terminate the application after 10 seconds.
	// https://cloud.google.com/run/docs/reference/container-contract#instance-shutdown
	gracefulShutdownTimeout := 8 * time.Second
	if timeout := os.Getenv("GRACEFUL_SHUTDOWN_TIMEOUT"); timeout != "" {
		parsedTimeout, err := time.ParseDuration(timeout)
		if err != nil {
			return fmt.Errorf("failed to parse GRACEFUL_SHUTDOWN_TIMEOUT: %w", err)
		}
		gracefulShutdownTimeout = parsedTimeout
	}

	svc, err := service.New(ctx)
	if err != nil {
		return err
	}

	errCh := make(chan error)
	go func() {
		defer close(errCh)
		errCh <- svc.Start(ctx)
	}()

	sig, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		if err != nil {
			return err // Server failed to start
		}
	case <-sig.Done(): // Graceful shutdown signal received
	}

	log.Info().Msg("starting graceful shutdown")

	// Remove the signal handler immediately to ensure following signals
	// forcefully terminate the application.
	stop()

	ctx, cancel := context.WithTimeout(ctx, gracefulShutdownTimeout)
	defer cancel()

	if err := svc.Stop(ctx); err != nil {
		return err
	}

	return nil
}
