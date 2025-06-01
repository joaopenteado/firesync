package propagator

import (
	"context"
	"io"
	"net/http"
	"strings"

	"cloud.google.com/go/pubsub"
	"github.com/googleapis/google-cloudevents-go/cloud/firestoredata"
	"github.com/joaopenteado/firesync/internal/eventinfo"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

// propagator propagates changes from the local database to the Pub/Sub topic
// for other replicators to consume. It skipps propagating changes resulted
// from changes replicated from other sources.
type propagator struct {
	topic         *pubsub.Topic
	defaultRegion string
}

func New(ctx context.Context, topic *pubsub.Topic, defaultRegion string) http.Handler {
	return &propagator{topic: topic, defaultRegion: defaultRegion}
}

func (svc *propagator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := zerolog.Ctx(ctx).With().Logger()

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Err(err).Msg("body read failed")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// https://cloud.google.com/eventarc/docs/cloudevents#firestore
	event := firestoredata.DocumentEventData{}
	if err := proto.Unmarshal(bodyBytes, &event); err != nil {
		logger.Err(err).Msg("body unmarshal failed")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	docName := event.GetValue().GetName()
	if docName == "" {
		docName = event.GetOldValue().GetName()
	}

	eventType, skip := shouldSkip(&event)

	logger = logger.With().
		Str("document_name", docName).
		Str("event_type", eventType.String()).
		Logger()

	if skip {
		logger.Debug().Msg("replicated change skipped")
		w.WriteHeader(http.StatusOK)
		return
	}

	sourceLocation := svc.defaultRegion
	if location := r.Header.Get("ce-location"); location != "" {
		sourceLocation = location
	}

	// publish change to topic
	res := svc.topic.Publish(ctx, &pubsub.Message{
		Attributes: map[string]string{
			"source_location": sourceLocation,
		},
		Data: bodyBytes,
	})

	logger = logger.With().
		Str("topic_name", svc.topic.String()).
		Logger()

	msgID, err := res.Get(ctx)
	if err != nil {
		logger.Err(err).Msg("change propagation failed")

		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	logger.Info().Str("message_id", msgID).Msg("change propagated")

	w.WriteHeader(http.StatusCreated)
}

func shouldSkip(event *firestoredata.DocumentEventData) (eventinfo.EventType, bool) {
	switch eventinfo.GetEventType(event) {
	case eventinfo.EventTypeCreated:
		if _, ok := event.GetValue().GetFields()["_firesync"]; ok {
			// Document was created by a replicator
			return eventinfo.EventTypeCreated, true
		}
		return eventinfo.EventTypeCreated, false
	case eventinfo.EventTypeUpdated:
		for _, field := range event.GetUpdateMask().GetFieldPaths() {
			if strings.HasPrefix(field, "_firesync") {
				// Document was updated by a replicator
				return eventinfo.EventTypeUpdated, true
			}
		}
		return eventinfo.EventTypeUpdated, false
	case eventinfo.EventTypeDeleted:
		if firesync, ok := event.GetValue().GetFields()["_firesync"]; ok {
			if deleted, ok := firesync.GetMapValue().GetFields()["deleted"]; ok && deleted.GetBooleanValue() {
				// Doesn't work!
				// Document was deleted by a replicator
				return eventinfo.EventTypeDeleted, true
			}
		}
		return eventinfo.EventTypeDeleted, false
	}

	panic("unreachable event type") // will be recovered in middleware
}
