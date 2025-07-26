package telemetry

import (
	"context"
	"net/url"
	"os"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/oauth"
)

// NewCloudTraceExporter returns a Google Cloud Trace OTLP exporter.
func NewCloudTraceExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	creds, err := oauth.NewApplicationDefault(ctx, "https://www.googleapis.com/auth/trace.append")
	if err != nil {
		return nil, err
	}

	return otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpointURL("https://telemetry.googleapis.com:443/v1/traces"),
		otlptracegrpc.WithDialOption(grpc.WithPerRPCCredentials(creds)),
	)
}

// NewOTLPTraceExporter returns an OTLP gRPC trace exporter for services like Uptrace.
func NewOTLPTraceExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	endpoint := getFirstEnv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT")

	if url, err := url.Parse(endpoint); err == nil && url.Hostname() == "telemetry.googleapis.com" {
		return NewCloudTraceExporter(ctx)
	}

	return otlptracegrpc.New(ctx)
}

// NewStdoutTraceExporter returns a stdout trace exporter for local development.
func NewStdoutTraceExporter(prettyPrint bool) (sdktrace.SpanExporter, error) {
	if prettyPrint {
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	}
	return stdouttrace.New()
}

func getFirstEnv(envs ...string) string {
	for _, env := range envs {
		if value := os.Getenv(env); value != "" {
			return value
		}
	}
	return ""
}
