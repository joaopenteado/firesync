package handler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/googleapis/google-cloudevents-go/cloud/firestoredata"
	"github.com/joaopenteado/firesync/internal/model"
	"github.com/joaopenteado/firesync/internal/service"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	unsupportedMediaType = errors.New(http.StatusText(http.StatusUnsupportedMediaType))
)

type Propagator interface {
	Propagate(ctx context.Context, event *model.Event) (service.PropagationResult, error)
}

type propagateOption interface {
	apply(*propagateOptions)
}

type propagateOptions struct {
	forceHTTP200Acknowledgement bool
}

type funcPropagateOption func(*propagateOptions)

func (f funcPropagateOption) apply(o *propagateOptions) { f(o) }

// WithForceHTTP200Acknowledgement forces the handler to return a 200 OK instead
// of semantically correct status codes for suceful message acknowledgements
// from the Pub/Sub API. This is necessary for the simulator to work, since it
// does not recognize status codes other than 200 OK as successful
// acknowledgements.
// See https://issuetracker.google.com/issues/434641504
func WithHTTP200Acknowledgement(enforce bool) propagateOption {
	return funcPropagateOption(func(o *propagateOptions) {
		o.forceHTTP200Acknowledgement = enforce
	})
}

func Propagate(svc Propagator, opts ...propagateOption) http.Handler {
	options := &propagateOptions{
		forceHTTP200Acknowledgement: false,
	}
	for _, opt := range opts {
		opt.apply(options)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		ctx := r.Context()
		logger := zerolog.Ctx(ctx).With().Logger()

		contentType := r.Header.Get("content-type")
		if contentType == "" {
			contentType = r.Header.Get("ce-datacontenttype")
		}
		rawEvent, err := parseFirestoreDocumentEventData(contentType, r.Body)
		if err != nil {
			logger.Err(err).
				Str("content_type", contentType).
				Msg("failed to parse firestore document event data")
			if errors.Is(err, unsupportedMediaType) {
				w.WriteHeader(http.StatusUnsupportedMediaType)
				return
			}

			w.WriteHeader(http.StatusBadRequest)
			return
		}

		eventTime := time.Now()
		if eventTimeStr := r.Header.Get("ce-time"); eventTimeStr != "" {
			parsedTime, err := time.Parse(time.RFC3339Nano, eventTimeStr)
			if err == nil && parsedTime.Before(eventTime) {
				eventTime = parsedTime
			}
		}

		modelEvent, err := model.ParseEvent(rawEvent, eventTime)
		if err != nil {
			logger.Err(err).Msg("failed to parse event")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		result, err := svc.Propagate(ctx, modelEvent)
		if err != nil || result == service.PropagationResultError {
			logger.Error().Err(err).Msg("propagation failed")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		switch result {
		case service.PropagationResultSuccess:
			if options.forceHTTP200Acknowledgement {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusAccepted)

		case service.PropagationResultSkipped:
			if options.forceHTTP200Acknowledgement {
				w.WriteHeader(http.StatusOK)
				return
			}

			w.WriteHeader(http.StatusNoContent)
		default:
			logger.Error().Stringer("result", result).Msg("unhandled propagation result")
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
}

func parseFirestoreDocumentEventData(contentType string, r io.Reader) (event *firestoredata.DocumentEventData, err error) {
	var bodyBytes []byte
	bodyBytes, err = io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	event = &firestoredata.DocumentEventData{}
	switch contentType {
	case "application/protobuf":
		err = proto.Unmarshal(bodyBytes, event)
		return

	case "application/json":
		err = protojson.Unmarshal(bodyBytes, event)
		return

	default:
		return nil, unsupportedMediaType
	}
}
