package middleware

import (
	"net/http"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"
)

func CloudEvent(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		ceDict := zerolog.Dict()
		traceAttrs := make([]attribute.KeyValue, 0, 13)

		if id := r.Header.Get("ce-id"); id != "" {
			ceDict.Str("id", id)
			traceAttrs = append(traceAttrs, semconv.CloudEventsEventID(id))
		}

		if source := r.Header.Get("ce-source"); source != "" {
			ceDict.Str("source", source)
			traceAttrs = append(traceAttrs, semconv.CloudEventsEventSource(source))
		}

		if specVersion := r.Header.Get("ce-specversion"); specVersion != "" {
			ceDict.Str("specversion", specVersion)
			traceAttrs = append(traceAttrs, semconv.CloudEventsEventSpecVersion(specVersion))
		}

		if subject := r.Header.Get("ce-subject"); subject != "" {
			ceDict.Str("subject", subject)
			traceAttrs = append(traceAttrs, semconv.CloudEventsEventSubject(subject))
		}

		if eventType := r.Header.Get("ce-type"); eventType != "" {
			ceDict.Str("type", eventType)
			traceAttrs = append(traceAttrs, semconv.CloudEventsEventType(eventType))
		}

		// Non-standard OTEL attribute convention, but useful nonetheless

		if time := r.Header.Get("ce-time"); time != "" {
			ceDict.Str("time", time)
			traceAttrs = append(traceAttrs, attribute.String("cloudevents.event_time", time))
		}

		if database := r.Header.Get("ce-database"); database != "" {
			ceDict.Str("database", database)
			traceAttrs = append(traceAttrs, attribute.String("cloudevents.event_database", database))
		}

		if dataContentType := r.Header.Get("ce-datacontenttype"); dataContentType != "" {
			ceDict.Str("datacontenttype", dataContentType)
			traceAttrs = append(traceAttrs, attribute.String("cloudevents.event_datacontenttype", dataContentType))
		}

		if dataSchema := r.Header.Get("ce-dataschema"); dataSchema != "" {
			ceDict.Str("dataschema", dataSchema)
			traceAttrs = append(traceAttrs, attribute.String("cloudevents.event_dataschema", dataSchema))
		}

		if document := r.Header.Get("ce-document"); document != "" {
			ceDict.Str("document", document)
			traceAttrs = append(traceAttrs, attribute.String("cloudevents.event_document", document))
		}

		if location := r.Header.Get("ce-location"); location != "" {
			ceDict.Str("location", location)
			traceAttrs = append(traceAttrs, attribute.String("cloudevents.event_location", location))
		}

		if namespace := r.Header.Get("ce-namespace"); namespace != "" {
			ceDict.Str("namespace", namespace)
			traceAttrs = append(traceAttrs, attribute.String("cloudevents.event_namespace", namespace))
		}

		if project := r.Header.Get("ce-project"); project != "" {
			ceDict.Str("project", project)
			traceAttrs = append(traceAttrs, attribute.String("cloudevents.event_project", project))
		}

		if len(traceAttrs) == 0 {
			// No CloudEvent headers, skip
			next.ServeHTTP(w, r)
			return
		}

		logger := zerolog.Ctx(ctx).With().
			Dict("cloudevent", ceDict).
			Logger()

		if span := trace.SpanFromContext(ctx); span.IsRecording() {
			span.SetAttributes(traceAttrs...)
		}

		next.ServeHTTP(w, r.WithContext(logger.WithContext(ctx)))
	})
}
