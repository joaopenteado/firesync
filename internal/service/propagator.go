package service

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/joaopenteado/firesync/internal/model"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type propagationMetrics struct {
	PropagationEventCount metric.Int64Counter
	PropagationLatency    metric.Int64Histogram
}

func newPropagationMetrics(meter metric.Meter) propagationMetrics {
	PropagationEventCount, err := meter.Int64Counter("firesync.propagation.event_count",
		metric.WithDescription("The total number of changes propagated from the source database to the Pub/Sub topic."),
		metric.WithUnit("1"),
	)
	if err != nil {
		log.Warn().Err(err).
			Str("metric", "firesync.propagation.event_count").
			Msg("failed to create metric")
		PropagationEventCount = noop.Int64Counter{}
	}

	PropagationLatency, err := meter.Int64Histogram("firesync.propagation.latency",
		metric.WithDescription("The latency of propagating changes to the Pub/Sub topic, from the moment the change happened in the source database to the moment the change was propagated to the Pub/Sub topic."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		log.Warn().Err(err).
			Str("metric", "firesync.propagation.latency").
			Msg("failed to create metric")
		PropagationLatency = noop.Int64Histogram{}
	}

	return propagationMetrics{
		PropagationEventCount: PropagationEventCount,
		PropagationLatency:    PropagationLatency,
	}
}

type propagator struct {
	topic   *pubsub.Topic
	db      *firestore.Client
	metrics propagationMetrics

	tombstoneTTL time.Duration
}

type PropagationResult uint

const (
	PropagationResultUnknown PropagationResult = iota
	PropagationResultSuccess
	PropagationResultSkipped
	PropagationResultError
)

func (r PropagationResult) String() string {
	switch r {
	case PropagationResultUnknown:
		return "unknown"
	case PropagationResultSuccess:
		return "success"
	case PropagationResultSkipped:
		return "skipped"
	case PropagationResultError:
		return "error"
	default:
		return fmt.Sprintf("unknown (%d)", r)
	}
}

func NewPropagator(topic *pubsub.Topic, db *firestore.Client, tombstoneTTL time.Duration, meter metric.Meter) *propagator {
	return &propagator{
		topic:        topic,
		db:           db,
		metrics:      newPropagationMetrics(meter),
		tombstoneTTL: tombstoneTTL,
	}
}

func (svc *propagator) Propagate(ctx context.Context, event *model.Event) (result PropagationResult, err error) {
	logger := zerolog.Ctx(ctx).With().
		Stringer("event_type", event.Type).
		Str("project_id", event.Name.ProjectID).
		Str("database_id", event.Name.DatabaseID).
		Str("document_path", event.Name.Path).
		Logger()
	ctx = logger.WithContext(ctx)

	defer func() {
		svc.metrics.PropagationEventCount.Add(ctx, 1, metric.WithAttributes(
			attribute.String("result", result.String()),
		))

		if result == PropagationResultSuccess {
			svc.metrics.PropagationLatency.Record(ctx, time.Since(event.Timestamp).Milliseconds())
		}
	}()

	var shouldPropagate bool
	switch event.Type {
	case model.EventTypeReplicated, model.EventTypeTombstone:
		shouldPropagate = false

	case model.EventTypeCreated:
		shouldPropagate, err = svc.processCreateEvent(ctx, event)

	case model.EventTypeUpdated:
		shouldPropagate, err = svc.processUpdateEvent(ctx, event)

	case model.EventTypeDeleted:
		shouldPropagate, err = svc.processDeleteEvent(ctx, event)

	default:
		return PropagationResultUnknown, fmt.Errorf("unknown event type: %s", event.Type)
	}

	if err != nil {
		return PropagationResultError, fmt.Errorf("failed to process event: %w", err)
	}

	if !shouldPropagate {
		logger.Debug().Msg("event propagation skipped")
		return PropagationResultSkipped, nil
	}

	marshaledRawEvent, err := proto.Marshal(event.Data)
	if err != nil {
		return PropagationResultError, fmt.Errorf("failed to marshal event: %w", err)
	}

	attrs := map[string]string{
		"content-type":  "application/protobuf",
		"event-time":    event.Timestamp.Format(time.RFC3339Nano),
		"event-type":    event.Type.String(),
		"project-id":    event.Name.ProjectID,
		"database-id":   event.Name.DatabaseID,
		"document-path": event.Name.Path,
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(attrs))

	res := svc.topic.Publish(ctx, &pubsub.Message{
		Data:       marshaledRawEvent,
		Attributes: attrs,
	})

	msgID, err := res.Get(ctx)
	if err != nil {
		return PropagationResultError, fmt.Errorf("failed to get message ID: %w", err)
	}

	logger.Debug().Str("message_id", msgID).Msg("event propagated")

	return PropagationResultSuccess, nil
}

func (svc *propagator) processCreateEvent(ctx context.Context, event *model.Event) (shouldPropagate bool, err error) {
	logger := zerolog.Ctx(ctx)
	err = svc.db.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		// check if the document tombstone exists with a read
		snap, err := tx.Get(event.Name.TombstoneRef(svc.db))
		if err != nil && status.Code(err) != codes.NotFound {
			return err
		}

		// if the tombstone exists and the timestamp is more recent than the
		// document's create time, this is a replicated delete event
		if snap.Exists() {
			tombstone := &model.Tombstone{}
			if err := snap.DataTo(tombstone); err != nil {
				return fmt.Errorf("failed to unmarshal tombstone: %w", err)
			}

			if tombstone.Timestamp.AsTime().After(event.Timestamp) {
				// delete the document, a more recent tombstone exists
				err = tx.Delete(event.Name.Ref(svc.db), firestore.LastUpdateTime(event.Timestamp))
				if status.Code(err) == codes.FailedPrecondition {
					logger.Debug().Msg("stale event, skipping propagation")
					return nil
				}
				return err
			}

			// tombstone exists but is older than the document's create time
			// it can be deleted, but we will leave it up to the garbage
			// collector
		}

		metadata := &model.Metadata{
			Timestamp: timestamppb.New(event.Timestamp),
			Source:    fmt.Sprintf("projects/%s/databases/%s", event.Name.ProjectID, event.Name.DatabaseID),
			Trace:     trace.SpanContextFromContext(ctx).TraceID().String(),
		}

		// if the tombstone does not exist or is older than the document's
		// create time, we can add firesync metadata to the document
		err = tx.Update(event.Name.Ref(svc.db), []firestore.Update{
			{
				Path:  "_firesync",
				Value: metadata,
			},
		}, firestore.LastUpdateTime(event.Timestamp))
		if status.Code(err) == codes.FailedPrecondition {
			logger.Debug().Msg("stale event, skipping propagation")
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to update document: %w", err)
		}

		shouldPropagate = true
		return nil
	})

	if err != nil {
		return false, fmt.Errorf("failed to update document: %w", err)
	}

	return shouldPropagate, nil
}

func (svc *propagator) processDeleteEvent(ctx context.Context, event *model.Event) (shouldPropagate bool, err error) {
	logger := zerolog.Ctx(ctx)

	tombstone := &model.Tombstone{
		Document:   event.Name.Ref(svc.db),
		Timestamp:  timestamppb.New(event.Timestamp),
		Source:     fmt.Sprintf("projects/%s/databases/%s", event.Name.ProjectID, event.Name.DatabaseID),
		Trace:      trace.SpanContextFromContext(ctx).TraceID().String(),
		Expiration: timestamppb.New(event.Timestamp.Add(svc.tombstoneTTL)),
	}

	err = svc.db.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		// check if a new document has been created
		snap, err := tx.Get(event.Name.Ref(svc.db))
		if err != nil && status.Code(err) != codes.NotFound {
			return err
		}

		if snap.Exists() {
			var ts time.Time
			md := struct {
				Metadata *model.Metadata `firestore:"_firesync"`
			}{}
			if err := snap.DataTo(&md); err != nil {
				// create change might have not been propagated yet
				// use the create time of the document
				ts = event.Timestamp
			} else {
				ts = md.Metadata.Timestamp.AsTime()
			}

			if ts.After(event.Timestamp) {
				logger.Debug().Msg("newer document exists, skipping propagation")
				return nil
			}

			// delete the document to keep consistency
			err = tx.Delete(event.Name.Ref(svc.db), firestore.LastUpdateTime(ts))
			if status.Code(err) == codes.FailedPrecondition {
				logger.Debug().Msg("stale event, skipping propagation")
				return nil
			}
			if err != nil {
				return err
			}
		}

		// check if a more recent tombstone exists
		snap, err = tx.Get(event.Name.TombstoneRef(svc.db))
		if err != nil && status.Code(err) != codes.NotFound {
			return err
		}

		if snap.Exists() {
			tombstone := &model.Tombstone{}
			if err := snap.DataTo(tombstone); err != nil {
				return fmt.Errorf("failed to unmarshal tombstone: %w", err)
			}

			if tombstone.Timestamp.AsTime().After(event.Timestamp) {
				logger.Debug().Msg("newer tombstone already exists, skipping propagation")
				return nil
			}

			err = tx.Update(event.Name.TombstoneRef(svc.db), []firestore.Update{
				{
					Path:  "ts",
					Value: tombstone.Timestamp,
				},
				{
					Path:  "src",
					Value: tombstone.Source,
				},
				{
					Path:  "trace",
					Value: tombstone.Trace,
				},
				{
					Path:  "exp",
					Value: tombstone.Expiration,
				},
			}, firestore.LastUpdateTime(event.Timestamp))
			if status.Code(err) == codes.FailedPrecondition {
				logger.Debug().Msg("stale event, skipping propagation")
				return nil
			}
			if err != nil {
				return fmt.Errorf("failed to update tombstone: %w", err)
			}

			shouldPropagate = true
			return nil
		}

		err = tx.Create(event.Name.TombstoneRef(svc.db), tombstone)
		if err != nil {
			return fmt.Errorf("failed to create tombstone: %w", err)
		}

		shouldPropagate = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("failed to update document: %w", err)
	}

	return shouldPropagate, nil
}

func (svc *propagator) processUpdateEvent(ctx context.Context, event *model.Event) (shouldPropagate bool, err error) {
	logger := zerolog.Ctx(ctx)

	err = svc.db.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		// check if a tombstone exists
		snap, err := tx.Get(event.Name.TombstoneRef(svc.db))
		if err != nil && status.Code(err) != codes.NotFound {
			return err
		}

		if snap.Exists() {
			tombstone := &model.Tombstone{}
			if err := snap.DataTo(tombstone); err != nil {
				return fmt.Errorf("failed to unmarshal tombstone: %w", err)
			}

			if tombstone.Timestamp.AsTime().After(event.Timestamp) {
				logger.Debug().Msg("newer tombstone exists, skipping propagation and deleting the document")
				err := tx.Delete(event.Name.Ref(svc.db), firestore.LastUpdateTime(event.Timestamp))
				if status.Code(err) == codes.FailedPrecondition {
					logger.Debug().Msg("stale event, skipping propagation")
					return nil
				}
				if err != nil {
					return fmt.Errorf("failed to delete document: %w", err)
				}
				return nil
			}
		}

		metadata := &model.Metadata{
			Timestamp: timestamppb.New(event.Timestamp),
			Source:    fmt.Sprintf("projects/%s/databases/%s", event.Name.ProjectID, event.Name.DatabaseID),
			Trace:     trace.SpanContextFromContext(ctx).TraceID().String(),
		}

		err = tx.Update(event.Name.Ref(svc.db), []firestore.Update{
			{
				Path:  "_firesync",
				Value: metadata,
			},
		}, firestore.LastUpdateTime(event.Timestamp))
		if status.Code(err) == codes.FailedPrecondition {
			logger.Debug().Msg("stale event, skipping propagation")
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to update document: %w", err)
		}

		shouldPropagate = true
		return nil
	})

	if err != nil {
		return false, fmt.Errorf("failed to update document: %w", err)
	}

	return shouldPropagate, nil
}
