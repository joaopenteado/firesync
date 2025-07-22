package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/googleapis/google-cloudevents-go/cloud/firestoredata"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
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
	metrics propagationMetrics
}

type PropagationResult uint8

const (
	PropagationResultSuccess PropagationResult = iota
	PropagationResultSkipped
	PropagationResultError
)

func (r PropagationResult) String() string {
	switch r {
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

func NewPropagator(topic *pubsub.Topic, meter metric.Meter) *propagator {
	return &propagator{
		topic:   topic,
		metrics: newPropagationMetrics(meter),
	}
}

func (svc *propagator) Propagate(ctx context.Context, eventTime time.Time, event *firestoredata.DocumentEventData) (PropagationResult, error) {
	docName := parseDocumentName(event)
	if docName.ProjectID == "" || docName.DatabaseID == "" || docName.DocumentPath == "" {
		svc.metrics.PropagationEventCount.Add(ctx, 1, metric.WithAttributes(
			attribute.String("result", PropagationResultError.String()),
		))
		// TODO: this should result in a 4XX error
		return PropagationResultError, fmt.Errorf("invalid document name format")
	}

	eventType := newEventType(docName, event)
	logger := zerolog.Ctx(ctx).With().
		Stringer("event_type", eventType).
		Str("project_id", docName.ProjectID).
		Str("database_id", docName.DatabaseID).
		Str("document_path", docName.DocumentPath).
		Logger()

	if eventType == eventTypeReplicated || eventType == eventTypeTombstone {
		logger.Debug().Msg("event propagation skipped")
		svc.metrics.PropagationEventCount.Add(ctx, 1, metric.WithAttributes(
			attribute.String("result", PropagationResultSkipped.String()),
		))
		return PropagationResultSkipped, nil
	}

	logger.Debug().Msg("processing event for propagation")

	svc.metrics.PropagationEventCount.Add(ctx, 1, metric.WithAttributes(
		attribute.String("result", PropagationResultSuccess.String()),
	))
	return PropagationResultSuccess, nil

}

type eventType uint8

const (
	eventTypeCreate eventType = iota
	eventTypeUpdate
	eventTypeDelete
	eventTypeReplicated
	eventTypeTombstone
)

func (e eventType) String() string {
	switch e {
	case eventTypeCreate:
		return "create"
	case eventTypeUpdate:
		return "update"
	case eventTypeDelete:
		return "delete"
	case eventTypeReplicated:
		return "replicated"
	case eventTypeTombstone:
		return "tombstone"
	default:
		return fmt.Sprintf("unknown (%d)", e)
	}
}

func newEventType(docName documentName, event *firestoredata.DocumentEventData) eventType {
	// If no old value, this is a create event
	if event.GetOldValue() == nil {
		doc := event.GetValue()
		if doc == nil {
			return eventTypeCreate
		}

		// Check if this is a tombstone for a replicated delete event
		// Document path starts with _firesync/ indicates internal firesync bookkeeping
		if strings.HasPrefix(docName.DocumentPath, "_firesync/") {
			return eventTypeTombstone
		}

		// Check if this was a replicated create event
		if _, ok := doc.GetFields()["_firesync"]; ok {
			// Only replicated create events have _firesync field right away
			return eventTypeReplicated
		}

		return eventTypeCreate
	}

	// If no new value, this is a delete event
	if event.GetValue() == nil {
		return eventTypeDelete
	}

	// Both old and new values exist, this is an update

	// If any of the updated fields are in the _firesync map field, this is a
	// change made by firesync
	updatedFieldPaths := event.GetUpdateMask().GetFieldPaths()
	if hasAnyFiresyncFields(updatedFieldPaths) {
		return eventTypeReplicated
	}

	return eventTypeUpdate
}

type documentName struct {
	ProjectID    string
	DatabaseID   string
	DocumentPath string
	IsOld        bool
}

func parseDocumentName(event *firestoredata.DocumentEventData) documentName {
	const (
		projectsPrefix = "projects/"
		databasesInfix = "/databases/"
		documentsInfix = "/documents/"
		minValidLength = len(projectsPrefix) + 1 + len(databasesInfix) + 1 + len(documentsInfix) + 1
	)

	d := documentName{}

	var name string
	if event.GetValue() != nil {
		name = event.GetValue().GetName()
	}

	if name == "" && event.GetOldValue() != nil {
		name = event.GetOldValue().GetName()
		d.IsOld = true
	}

	// Early validation - return empty documentName if any validation fails
	if len(name) < minValidLength {
		return documentName{}
	}

	if !strings.HasPrefix(name, projectsPrefix) {
		return documentName{}
	}

	dbStart := strings.Index(name[len(projectsPrefix):], databasesInfix)
	if dbStart == -1 {
		return documentName{}
	}
	dbStart += len(projectsPrefix) // Adjust for the prefix we skipped

	projectID := name[len(projectsPrefix):dbStart]
	if projectID == "" || strings.Contains(projectID, "/") {
		return documentName{}
	}

	docStart := strings.Index(name[dbStart+len(databasesInfix):], documentsInfix)
	if docStart == -1 {
		return documentName{}
	}
	docStart += dbStart + len(databasesInfix) // Adjust for the sections we skipped

	databaseID := name[dbStart+len(databasesInfix) : docStart]
	if databaseID == "" || strings.Contains(databaseID, "/") {
		return documentName{}
	}

	documentPath := name[docStart+len(documentsInfix):]
	if documentPath == "" {
		return documentName{}
	}

	// All validations passed, populate the struct
	d.ProjectID = projectID
	d.DatabaseID = databaseID
	d.DocumentPath = documentPath

	return d
}

func (d documentName) String() string {
	return fmt.Sprintf("projects/%s/databases/%s/documents/%s", d.ProjectID, d.DatabaseID, d.DocumentPath)
}

func hasAnyFiresyncFields(fieldNames []string) bool {
	for _, fieldName := range fieldNames {
		if fieldName == "_firesync" || strings.HasPrefix(fieldName, "_firesync.") {
			return true
		}
	}
	return false
}
