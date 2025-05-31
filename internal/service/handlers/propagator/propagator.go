package propagator

import (
	"context"
	"io"
	"net/http"

	"cloud.google.com/go/pubsub"
	"github.com/googleapis/google-cloudevents-go/cloud/firestoredata"
	"github.com/rs/zerolog"
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

	// https://cloud.google.com/eventarc/docs/cloudevents#firestore
	event := firestoredata.DocumentEventData{}
	if err := proto.Unmarshal(bodyBytes, &event); err != nil {
		logger.Error().Err(err).Msg("failed to unmarshal request body")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	eventType := getEventType(&event)

	if eventType == eventTypeDelete {
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

	w.WriteHeader(http.StatusCreated)
}
