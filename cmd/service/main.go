// Package main is the main package for the firesync application.
//
// It's sole purpose is to manage the lifecycle of the service, including
// starting and stopping the service, and handling the graceful shutdown of the
// application.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/profiler"
	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/joaopenteado/firesync/internal/cloudlogging"
	"github.com/joaopenteado/firesync/internal/config"
	"github.com/joaopenteado/firesync/internal/handler"
	"github.com/joaopenteado/firesync/internal/router"
	"github.com/joaopenteado/firesync/internal/service"
	"github.com/joaopenteado/firesync/internal/telemetry"
)

const (
	InitializationTimeout = 5 * time.Second
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := run(ctx); err != nil {
		cancel() // Cancel root context when os.Exit() is called.
		log.Fatal().Err(err).Msg("failed to run service")
	}
}

func run(ctx context.Context) error {
	var shutdownSignalDeadline time.Time

	initCtx, initCancel := context.WithTimeout(ctx, InitializationTimeout)
	defer initCancel()

	// Configuration
	cfg, err := config.Load(initCtx)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Logging
	zerolog.SetGlobalLevel(cfg.LogLevel)
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.LevelFieldName = "severity"
	zerolog.LevelFieldMarshalFunc = cloudlogging.LevelFieldMarshalFunc
	if cfg.Environment == "local" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	} else {
		log.Logger = log.Hook(cloudlogging.Hook(cfg.ProjectID))
	}

	// Profiling
	if cfg.ProfilingEnabled {
		profCfg := profiler.Config{
			Service:        cfg.ServiceName,
			ServiceVersion: cfg.ServiceRevision,
			ProjectID:      cfg.ProjectID,
			Instance:       cfg.InstanceID,
			Zone:           cfg.Region,
		}
		if err := profiler.Start(profCfg); err != nil {
			log.Err(err).Msg("failed to start profiler")
		}
	}

	// Telemetry
	telemetryManager := telemetry.NewManager(initCtx, telemetry.Options{
		ProjectID:        cfg.ProjectID,
		ServiceName:      cfg.ServiceName,
		ServiceRevision:  cfg.ServiceRevision,
		InstanceID:       cfg.InstanceID,
		TracingEnabled:   cfg.TracingEnabled,
		MetricsEnabled:   cfg.MetricsEnabled,
		Environment:      cfg.Environment,
		TraceSampleRatio: cfg.TraceSampleRatio,
	})
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	otel.SetTracerProvider(telemetryManager.TracerProvider())
	otel.SetMeterProvider(telemetryManager.MeterProvider())
	defer func() {
		ctx, cancel := context.WithDeadline(ctx, shutdownSignalDeadline)
		defer cancel()
		if err := telemetryManager.Shutdown(ctx); err != nil {
			log.Warn().Err(err).Msg("failed to shutdown telemetry manager")
		}
	}()
	meter := telemetryManager.MeterProvider().Meter("github.com/joaopenteado/firesync")

	pubsubClient, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return fmt.Errorf("failed to create pubsub client: %w", err)
	}
	defer func() {
		if err := pubsubClient.Close(); err != nil {
			log.Err(err).Msg("failed to close pubsub client")
		}
	}()

	firestoreClient, err := firestore.NewClientWithDatabase(ctx, cfg.DatabaseProjectID(), cfg.DatabaseID())
	if err != nil {
		return fmt.Errorf("failed to create firestore client: %w", err)
	}
	defer func() {
		if err := firestoreClient.Close(); err != nil {
			log.Err(err).Msg("failed to close firestore client")
		}
	}()

	propagator := service.NewPropagator(pubsubClient.Topic(cfg.Topic), meter)
	replicator := service.NewReplicator(meter, firestoreClient)

	r := router.New(router.Config{
		PropagateHandler: handler.Propagate(propagator),
		ReplicateHandler: handler.Replicate(replicator),
		ServiceName:      cfg.ServiceName,
		TracingEnabled:   cfg.TracingEnabled,
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: r,
	}

	errCh := make(chan error)
	go func() {
		defer close(errCh)
		log.Debug().Msg("starting service")
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	sig, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		if err != nil {
			return err // Server failed to start
		}
	case <-sig.Done(): // Graceful shutdown signal received
		shutdownSignalDeadline = time.Now().Add(cfg.ServerGracefulShutdownTimeout)
	}

	log.Debug().
		Dur("timeout", cfg.ServerGracefulShutdownTimeout).
		Msg("initiating graceful shutdown")

	// Remove the signal handler immediately to ensure following signals
	// forcefully terminate the application.
	stop()

	shutdownCtx, cancel := context.WithDeadline(ctx, shutdownSignalDeadline)
	defer cancel()

	errCh = make(chan error)
	go func() {
		defer close(errCh)
		if err := srv.Shutdown(shutdownCtx); err != nil {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("failed to gracefully shutdown server: %w", err)
		}
	case <-ctx.Done():
		return fmt.Errorf("failed to gracefully shutdown server: %w", ctx.Err())
	}

	return nil
}
