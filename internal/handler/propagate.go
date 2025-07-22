package handler

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/googleapis/google-cloudevents-go/cloud/firestoredata"
	"github.com/joaopenteado/firesync/internal/service"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

type Propagator interface {
	Propagate(ctx context.Context, eventTime time.Time, event *firestoredata.DocumentEventData) (service.PropagationResult, error)
}

func Propagate(svc Propagator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := zerolog.Ctx(ctx).With().Logger()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Err(err).Msg("body read failed")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		event := firestoredata.DocumentEventData{}
		if err := proto.Unmarshal(bodyBytes, &event); err != nil {
			logger.Err(err).Msg("proto unmarshal failed")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		eventTime := time.Now()
		if eventTimeStr := r.Header.Get("ce-time"); eventTimeStr != "" {
			parsedTime, err := time.Parse(time.RFC3339, eventTimeStr)
			if err == nil && parsedTime.Before(eventTime) {
				eventTime = parsedTime
			}
		}
		if docTime := event.GetValue().GetUpdateTime().AsTime(); !docTime.IsZero() && docTime.Before(eventTime) {
			eventTime = docTime
		}

		result, err := svc.Propagate(ctx, eventTime, &event)
		if err != nil || result == service.PropagationResultError {
			logger.Error().Err(err).Msg("propagation failed")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		switch result {
		case service.PropagationResultSuccess:
			w.WriteHeader(http.StatusAccepted)
		case service.PropagationResultSkipped:
			w.WriteHeader(http.StatusNoContent)
		default:
			logger.Error().Stringer("result", result).Msg("unhandled propagation result")
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
}
