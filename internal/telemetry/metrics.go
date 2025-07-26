package telemetry

import (
	"context"

	mexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/sdk/metric"
)

func NewCloudMonitoringMetricExporter(projectID string) (metric.Exporter, error) {
	return mexporter.New(mexporter.WithProjectID(projectID))
}

func NewOTLPMetricExporter(ctx context.Context) (metric.Exporter, error) {
	return otlpmetricgrpc.New(ctx)
}

// NewStdoutMetricExporter returns a stdout metric exporter for local
// development.
func NewStdoutMetricExporter(prettyPrint bool) (metric.Exporter, error) {
	if prettyPrint {
		return stdoutmetric.New(stdoutmetric.WithPrettyPrint())
	}
	return stdoutmetric.New()
}
