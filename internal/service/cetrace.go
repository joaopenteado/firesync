package service

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.32.0"
	"go.opentelemetry.io/otel/trace"
)

func traceCloudEventHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span := trace.SpanFromContext(r.Context())

		attrs := make([]attribute.KeyValue, 0, 4)

		if id := r.Header.Get("ce-id"); id != "" {
			attrs = append(attrs, semconv.CloudEventsEventID(id))
		}

		if source := r.Header.Get("ce-source"); source != "" {
			attrs = append(attrs, semconv.CloudEventsEventSource(source))
		}

		if specVersion := r.Header.Get("ce-specversion"); specVersion != "" {
			attrs = append(attrs, semconv.CloudEventsEventSpecVersion(specVersion))
		}

		if subject := r.Header.Get("ce-subject"); subject != "" {
			attrs = append(attrs, semconv.CloudEventsEventSubject(subject))
		}

		if eventType := r.Header.Get("ce-type"); eventType != "" {
			attrs = append(attrs, semconv.CloudEventsEventType(eventType))
		}

		// Not standard OTEL attribute convention, but useful nonetheless

		if eventTime := r.Header.Get("ce-time"); eventTime != "" {
			attrs = append(attrs, attribute.String("cloudevents.event_time", eventTime))
		}

		if database := r.Header.Get("ce-database"); database != "" {
			attrs = append(attrs, attribute.String("cloudevents.event_database", database))
		}

		if contentType := r.Header.Get("ce-datacontenttype"); contentType != "" {
			attrs = append(attrs, attribute.String("cloudevents.event_datacontenttype", contentType))
		}

		if schema := r.Header.Get("ce-dataschema"); schema != "" {
			attrs = append(attrs, attribute.String("cloudevents.event_dataschema", schema))
		}

		if document := r.Header.Get("ce-document"); document != "" {
			attrs = append(attrs, attribute.String("cloudevents.event_document", document))
		}

		if location := r.Header.Get("ce-location"); location != "" {
			attrs = append(attrs, attribute.String("cloudevents.event_location", location))
		}

		if namespace := r.Header.Get("ce-namespace"); namespace != "" {
			attrs = append(attrs, attribute.String("cloudevents.event_namespace", namespace))
		}

		if project := r.Header.Get("ce-project"); project != "" {
			attrs = append(attrs, attribute.String("cloudevents.event_project", project))
		}

		if traceparent := r.Header.Get("ce-traceparent"); traceparent != "" {
			attrs = append(attrs, attribute.String("cloudevents.event_traceparent", traceparent))
		}

		if tracestate := r.Header.Get("ce-tracestate"); tracestate != "" {
			attrs = append(attrs, attribute.String("cloudevents.event_tracestate", tracestate))
		}

		if len(attrs) > 0 {
			span.SetAttributes(attrs...)
		}

		next.ServeHTTP(w, r)
	})
}
