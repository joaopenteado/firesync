package propagator

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// propagator propagates changes from the local database to the Pub/Sub topic
// for other replicators to consume. It skipps propagating changes resulted
// from changes replicated from other sources.
type propagator struct {
	topic *pubsub.Topic
}

func New(ctx context.Context, topic *pubsub.Topic) http.Handler {
	return &propagator{topic: topic}
}

func (svc *propagator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// https://cloud.google.com/eventarc/docs/cloudevents#firestore
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Error().Err(err).Msg("failed to read request body")
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	log.Debug().
		Str("method", r.Method).
		Str("path", r.URL.Path).
		Dict("headers", zerolog.Dict().
			Str("ce_id", r.Header.Get("ce-id")).
			Str("ce_source", r.Header.Get("ce-source")).
			Str("ce_specversion", r.Header.Get("ce-specversion")).
			Str("ce_type", r.Header.Get("ce-type")).
			Str("ce_time", r.Header.Get("ce-time")),
		).
		Str("body", base64.StdEncoding.EncodeToString(bodyBytes)).
		Msg("propagator")

	w.WriteHeader(http.StatusNoContent)
	w.Write([]byte("propagator"))
}
