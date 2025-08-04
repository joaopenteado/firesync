package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/sethvargo/go-envconfig"
)

type config struct {
	Seed          int64         `env:"SEED,required"`
	ProjectID     string        `env:"GOOGLE_CLOUD_PROJECT, default=firesync"`
	FirestorePath string        `env:"FIRESTORE_COLLECTION,required"`
	DocumentCount int           `env:"DOCUMENT_COUNT, default=100"`
	Timeout       time.Duration `env:"TIMEOUT, default=30s"`
}

var collectionPathRegex = regexp.MustCompile(`^projects/([^/]+)/databases/([^/]+)/documents/(.+)`)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal().Err(err).Msg("failed to run stress util")
	}
}

func run(ctx context.Context) error {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	var cfg config
	if err := envconfig.Process(ctx, &cfg); err != nil {
		return fmt.Errorf("failed to process environment variables: %w", err)
	}

	matches := collectionPathRegex.FindStringSubmatch(cfg.FirestorePath)
	if len(matches) != 4 {
		return fmt.Errorf("invalid firestore collection path format: %s", cfg.FirestorePath)
	}
	projectID := matches[1]
	databaseID := matches[2]
	path := matches[3]

	initCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	client, err := firestore.NewClientWithDatabase(initCtx, projectID, databaseID)
	if err != nil {
		return fmt.Errorf("failed to create firestore client: %w", err)
	}
	defer client.Close()

	// Deterministic list of document IDs based on provided seed.
	seedRand := rand.New(rand.NewSource(cfg.Seed))
	ids := make([]string, cfg.DocumentCount)
	for i := 0; i < cfg.DocumentCount; i++ {
		ids[i] = fmt.Sprintf("doc-%d", seedRand.Int63())
	}

	// Randomize order and operations using a different random source.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })

	for _, id := range ids {
		doc := client.Collection(path).Doc(id)
		switch r.Intn(3) {
		case 0: // create
			data := map[string]interface{}{"id": uuid.NewString()}
			if _, err := doc.Set(ctx, data); err != nil {
				log.Error().Err(err).Str("doc_id", id).Msg("create failed")
			} else {
				log.Info().Str("doc_id", id).Msg("created document")
			}
		case 1: // update
			if r.Intn(5) == 0 { // 1/5 chance to add a new property
				propName := fmt.Sprintf("extra_%d", r.Int())
				update := []firestore.Update{{Path: propName, Value: uuid.NewString()}}
				if _, err := doc.Update(ctx, update); err != nil {
					log.Error().Err(err).Str("doc_id", id).Msg("add property failed")
				} else {
					log.Info().Str("doc_id", id).Str("property", propName).Msg("added property")
				}
			} else { // otherwise update id field
				newID := uuid.NewString()
				if _, err := doc.Set(ctx, map[string]interface{}{"id": newID}, firestore.MergeAll); err != nil {
					log.Error().Err(err).Str("doc_id", id).Msg("update failed")
				} else {
					log.Info().Str("doc_id", id).Msg("updated document")
				}
			}
		case 2: // delete
			if _, err := doc.Delete(ctx); err != nil {
				log.Error().Err(err).Str("doc_id", id).Msg("delete failed")
			} else {
				log.Info().Str("doc_id", id).Msg("deleted document")
			}
		}
	}

	return nil
}
