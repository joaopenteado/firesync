package telemetry

import (
	"context"
	"runtime/debug"

	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

type ResourceConfig struct {
	ServiceName     string
	ServiceRevision string
	InstanceID      string
	ProjectID       string
	Environment     string
}

func NewResource(ctx context.Context, cfg ResourceConfig) (*resource.Resource, error) {
	return resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithOS(),
		resource.WithContainer(),
		resource.WithProcess(),
		resource.WithProcessRuntimeName(),
		resource.WithProcessRuntimeVersion(),
		resource.WithProcessRuntimeDescription(),
		resource.WithDetectors(
			gcp.NewDetector(),
		),
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceRevision),
			semconv.ServiceInstanceID(cfg.InstanceID),
			semconv.DeploymentEnvironmentName(cfg.Environment),
			semconv.VCSRefHeadRevision(getVcsRevision()),
			semconv.VCSRepositoryURLFull("https://github.com/joaopenteado/firesync"),
			attribute.String("gcp.project_id", cfg.ProjectID),
		),
		resource.WithSchemaURL(semconv.SchemaURL),
	)
}

func getVcsRevision() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				return s.Value
			}
		}
	}
	return "unknown"
}
