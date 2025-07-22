package telemetry

import (
	mexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/sdk/metric"
)

func NewCloudMonitoringMetricReader(projectID string) (metric.Reader, error) {
	exporter, err := mexporter.New(mexporter.WithProjectID(projectID))
	if err != nil {
		return nil, err
	}
	return metric.NewPeriodicReader(exporter), nil
}

// NewStdoutMetricReader returns a stdout metric reader for local development.
func NewStdoutMetricReader() (metric.Reader, error) {
	exporter, err := stdoutmetric.New(
		stdoutmetric.WithPrettyPrint(),
	)
	if err != nil {
		return nil, err
	}
	return metric.NewPeriodicReader(exporter), nil
}
