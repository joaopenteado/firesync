package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/iterator"
)

var dbPathRegex = regexp.MustCompile(`^projects/([^/]+)/databases/([^/]+)$`)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	if len(os.Args) < 2 {
		log.Fatal().Msg("no firestore databases provided")
	}
	if err := run(context.Background(), os.Args[1:]); err != nil {
		log.Error().Err(err).Msg("integrity check failed")
		os.Exit(1)
	}
}

func run(ctx context.Context, dbPaths []string) error {
	if len(dbPaths) < 2 {
		return fmt.Errorf("need at least two databases to compare")
	}

	type database struct {
		path   string
		client *firestore.Client
		docs   map[string]map[string]interface{}
	}

	dbs := make([]database, len(dbPaths))
	for i, p := range dbPaths {
		projectID, databaseID, err := parseDBPath(p)
		if err != nil {
			return err
		}
		client, err := firestore.NewClientWithDatabase(ctx, projectID, databaseID)
		if err != nil {
			return fmt.Errorf("failed to create firestore client for %s: %w", p, err)
		}
		defer client.Close()
		docs, err := fetchAllDocs(ctx, client)
		if err != nil {
			return fmt.Errorf("failed to fetch documents for %s: %w", p, err)
		}
		dbs[i] = database{path: p, client: client, docs: docs}
	}

	discrepancies := 0
	ref := dbs[0]
	for path, data := range ref.docs {
		for _, other := range dbs[1:] {
			d, ok := other.docs[path]
			if !ok {
				discrepancies++
				log.Error().Str("document", path).Str("database", other.path).Msg("missing document")
				continue
			}
			if !reflect.DeepEqual(data, d) {
				discrepancies++
				log.Error().Str("document", path).Str("database", other.path).Msg("mismatched document")
			}
		}
	}

	for _, other := range dbs[1:] {
		for path := range other.docs {
			if _, ok := ref.docs[path]; !ok {
				discrepancies++
				log.Error().Str("document", path).Str("database", other.path).Msg("extra document")
			}
		}
	}

	if discrepancies > 0 {
		return fmt.Errorf("%d discrepancies found", discrepancies)
	}
	return nil
}

func parseDBPath(p string) (string, string, error) {
	matches := dbPathRegex.FindStringSubmatch(p)
	if len(matches) != 3 {
		return "", "", fmt.Errorf("invalid firestore database path: %s", p)
	}
	return matches[1], matches[2], nil
}

func fetchAllDocs(ctx context.Context, client *firestore.Client) (map[string]map[string]interface{}, error) {
	docs := make(map[string]map[string]interface{})
	iter := client.Collections(ctx)
	for {
		coll, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list collections: %w", err)
		}
		if err := fetchCollectionDocs(ctx, coll, docs); err != nil {
			return nil, err
		}
	}
	return docs, nil
}

func fetchCollectionDocs(ctx context.Context, coll *firestore.CollectionRef, docs map[string]map[string]interface{}) error {
	docIter := coll.Documents(ctx)
	for {
		snap, err := docIter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to iterate documents: %w", err)
		}
		docs[snap.Ref.Path] = snap.Data()

		subIter := snap.Ref.Collections(ctx)
		for {
			sub, err := subIter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return fmt.Errorf("failed to list subcollections: %w", err)
			}
			if err := fetchCollectionDocs(ctx, sub, docs); err != nil {
				return err
			}
		}
	}
	return nil
}
