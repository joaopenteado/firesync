package propagator

import (
	"context"
	"io"
	"net/http"
	"strings"

	"cloud.google.com/go/pubsub"
	"github.com/googleapis/google-cloudevents-go/cloud/firestoredata"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.32.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
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
	ctx := r.Context()
	logger := zerolog.Ctx(ctx)

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error().Err(err).Msg("failed to read request body")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	headers := zerolog.Dict()
	for k, v := range r.Header {
		headers.Str(k, strings.Join(v, ","))
	}

	logger.Debug().
		Dict("headers", headers).
		Msg("propagator")

	// https://cloud.google.com/eventarc/docs/cloudevents#firestore
	event := firestoredata.DocumentEventData{}
	if err := proto.Unmarshal(bodyBytes, &event); err != nil {
		logger.Error().Err(err).Msg("failed to unmarshal request body")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	eventType := getEventType(&event)

	if eventType == eventTypeDelete {
		name := event.GetOldValue().GetName()
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("gcp.firestore.document.name", name),
		)

		// propagate set ttl event
		logger.Debug().
			Msg("delete event")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if _, ok := event.GetValue().GetFields()["_firesync"]; ok {
		// skip replicated changes
		logger.Debug().Msg("skipping replicated change")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	switch getEventType(&event) {
	case eventTypeCreate:
		logger.Info().Msg("create event")
	case eventTypeUpdate:
		logger.Info().Msg("update event")
	case eventTypeDelete:
		logger.Info().Msg("delete event")
		// todo set tll
	}

	// Propagate changes to the Pub/Sub topic if not a replicated change
	// (i.e. the change originated in this region)

	// TODO for deleted documents, set the ttl for now

	w.WriteHeader(http.StatusNoContent)
	w.Write([]byte("propagator"))
}
