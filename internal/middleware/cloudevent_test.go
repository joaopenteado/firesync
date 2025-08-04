package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

func TestCloudEvent(t *testing.T) {
	headers := map[string]string{
		"ce-id":              "1",
		"ce-source":          "source",
		"ce-specversion":     "1.0",
		"ce-subject":         "sub",
		"ce-type":            "type",
		"ce-time":            "2024-01-01T00:00:00Z",
		"ce-database":        "db",
		"ce-datacontenttype": "application/json",
		"ce-dataschema":      "schema",
		"ce-document":        "doc",
		"ce-location":        "loc",
		"ce-namespace":       "ns",
		"ce-project":         "proj",
	}

	logFields := map[string]string{
		"id":              "1",
		"source":          "source",
		"specversion":     "1.0",
		"subject":         "sub",
		"type":            "type",
		"time":            "2024-01-01T00:00:00Z",
		"database":        "db",
		"datacontenttype": "application/json",
		"dataschema":      "schema",
		"document":        "doc",
		"location":        "loc",
		"namespace":       "ns",
		"project":         "proj",
	}

	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	tracer := tp.Tracer("test")

	var buf bytes.Buffer
	baseLogger := zerolog.New(&buf)

	t.Run("with headers", func(t *testing.T) {
		ctx := baseLogger.WithContext(context.Background())
		ctx, span := tracer.Start(ctx, "span")
		req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		rr := httptest.NewRecorder()
		CloudEvent(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			zerolog.Ctx(r.Context()).Info().Msg("done")
		})).ServeHTTP(rr, req)
		span.End()

		var entry map[string]any
		if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
			t.Fatalf("unmarshal log: %v", err)
		}
		ce, ok := entry["cloudevent"].(map[string]any)
		if !ok {
			t.Fatalf("no cloudevent field in log: %v", entry)
		}
		for k, v := range logFields {
			if ce[k] != v {
				t.Errorf("log cloudevent %s = %v, want %v", k, ce[k], v)
			}
		}

		spans := sr.Ended()
		if len(spans) != 1 {
			t.Fatalf("got %d spans, want 1", len(spans))
		}
		attrMap := map[attribute.Key]string{}
		for _, kv := range spans[0].Attributes() {
			attrMap[kv.Key] = kv.Value.AsString()
		}
		expected := map[attribute.Key]string{
			semconv.CloudEventsEventIDKey:                      headers["ce-id"],
			semconv.CloudEventsEventSourceKey:                  headers["ce-source"],
			semconv.CloudEventsEventSpecVersionKey:             headers["ce-specversion"],
			semconv.CloudEventsEventSubjectKey:                 headers["ce-subject"],
			semconv.CloudEventsEventTypeKey:                    headers["ce-type"],
			attribute.Key("cloudevents.event_time"):            headers["ce-time"],
			attribute.Key("cloudevents.event_database"):        headers["ce-database"],
			attribute.Key("cloudevents.event_datacontenttype"): headers["ce-datacontenttype"],
			attribute.Key("cloudevents.event_dataschema"):      headers["ce-dataschema"],
			attribute.Key("cloudevents.event_document"):        headers["ce-document"],
			attribute.Key("cloudevents.event_location"):        headers["ce-location"],
			attribute.Key("cloudevents.event_namespace"):       headers["ce-namespace"],
			attribute.Key("cloudevents.event_project"):         headers["ce-project"],
		}
		if len(attrMap) != len(expected) {
			t.Fatalf("got %d attributes, want %d", len(attrMap), len(expected))
		}
		for k, v := range expected {
			if attrMap[k] != v {
				t.Errorf("span attr %s = %q, want %q", k, attrMap[k], v)
			}
		}
	})

	t.Run("no headers", func(t *testing.T) {
		buf.Reset()
		sr.Reset()

		ctx := baseLogger.WithContext(context.Background())
		ctx, span := tracer.Start(ctx, "span")
		req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
		rr := httptest.NewRecorder()
		CloudEvent(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			zerolog.Ctx(r.Context()).Info().Msg("done")
		})).ServeHTTP(rr, req)
		span.End()

		if strings.Contains(buf.String(), "cloudevent") {
			t.Fatalf("unexpected cloudevent in log: %s", buf.String())
		}
		spans := sr.Ended()
		if len(spans) != 1 {
			t.Fatalf("got %d spans, want 1", len(spans))
		}
		if len(spans[0].Attributes()) != 0 {
			t.Fatalf("expected no attributes, got %v", spans[0].Attributes())
		}
	})
}
