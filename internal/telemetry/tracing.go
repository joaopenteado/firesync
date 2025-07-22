package telemetry

import (
	"context"

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

// NewStdoutTraceExporter returns a stdout trace exporter for local development.
func NewStdoutTraceExporter() (sdktrace.SpanExporter, error) {
	return stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
	)
}
