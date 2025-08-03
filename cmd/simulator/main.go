package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/joaopenteado/firesync/internal/simulator"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/sethvargo/go-envconfig"
)

type config struct {
	ProjectID            string        `env:"GOOGLE_CLOUD_PROJECT, default=firesync"`
	TopicID              string        `env:"TOPIC, default=firesync"`
	ReplicatorURLs       []string      `env:"REPLICATOR_URLS, default=http://firesync:8080/v1/replicate"`
	PropagatorURLs       []string      `env:"PROPAGATOR_URLS, default=http://firesync:8080/v1/propagate"`
	FirestoreCollections []string      `env:"FIRESTORE_COLLECTIONS, default=projects/firesync/databases/(default)/documents/test"`
	ShutdownTimeout      time.Duration `env:"SHUTDOWN_TIMEOUT, default=10s"`
}

var collectionPathRegex = regexp.MustCompile(`^projects/([^/]+)/databases/([^/]+)/documents/(.+)`)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal().Err(err).Msg("failed to run simulator")
	}
}

func run(ctx context.Context) error {
	var shutdownDeadline time.Time
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	initCtx, initCancel := context.WithTimeout(ctx, 10*time.Second)
	defer initCancel()

	log.Debug().Msg("processing environment variables")
	var cfg config
	if err := envconfig.Process(initCtx, &cfg); err != nil {
		return fmt.Errorf("failed to process environment variables: %w", err)
	}

	log.Debug().Str("project_id", cfg.ProjectID).Msg("initializing pubsub client")
	pubsubClient, err := pubsub.NewClient(initCtx, cfg.ProjectID)
	if err != nil {
		return fmt.Errorf("failed to create pubsub client: %w", err)
	}
	defer pubsubClient.Close()

	for i, collectionPath := range cfg.FirestoreCollections {
		matches := collectionPathRegex.FindStringSubmatch(collectionPath)
		if len(matches) != 4 {
			return fmt.Errorf("invalid firestore collection path format: %s", collectionPath)
		}
		projectID := matches[1]
		databaseID := matches[2]
		path := matches[3]

		firestoreClient, err := firestore.NewClientWithDatabase(initCtx, projectID, databaseID)
		if err != nil {
			return fmt.Errorf("failed to create firestore client for project %s database %s: %w", projectID, databaseID, err)
		}
		defer func(client *firestore.Client, projectID string, databaseID string) {
			log.Debug().
				Str("project_id", projectID).
				Str("database_id", databaseID).
				Msg("closing firestore client")
			if err := client.Close(); err != nil {
				log.Error().
					Str("project_id", projectID).
					Str("database_id", databaseID).
					Err(err).
					Msg("failed to close firestore client")
			}
		}(firestoreClient, projectID, databaseID)

		watcher, err := simulator.WatchFirestoreCollection(initCtx, pubsubClient, projectID, databaseID, firestoreClient.Collection(path), cfg.PropagatorURLs[i])
		if err != nil {
			return fmt.Errorf("failed to watch firestore collection %s: %w", collectionPath, err)
		}
		if err := watcher.Start(ctx); err != nil {
			return fmt.Errorf("failed to start watcher: %w", err)
		}
		defer func(watcher *simulator.Watcher) {
			ctx, cancel := context.WithDeadline(ctx, shutdownDeadline)
			defer cancel()
			if err := watcher.Close(ctx); err != nil {
				log.Error().Err(err).Msg("failed to close watcher")
			}
		}(watcher)

		go func() {
			time.Sleep(10 * time.Second)
			log.Info().
				Str("propagator_url", cfg.PropagatorURLs[i]).
				Str("project_id", projectID).
				Str("database_id", databaseID).
				Str("collection_path", path).
				Msg("setting test document")
			_, err := firestoreClient.Collection(path).Doc("test").Set(ctx, map[string]interface{}{
				"test": "test",
			})
			if err != nil {
				log.Error().Err(err).Msg("failed to set document")
			}
		}()
	}

	shutdownSig, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info().Msg("simulator up and running")
	<-shutdownSig.Done()
	log.Info().Msg("shutdown signal received, waiting for cleanup")
	shutdownDeadline = time.Now().Add(cfg.ShutdownTimeout)

	return nil
}
