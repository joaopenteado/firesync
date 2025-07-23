package config

import (
	"context"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/rs/zerolog"
	"github.com/sethvargo/go-envconfig"
)

type Config struct {
	// Project ID of the project the Cloud Run service belongs to.
	ProjectID string `env:"GOOGLE_CLOUD_PROJECT, required"`

	// Region of this Cloud Run service.
	Region string `env:"GOOGLE_CLOUD_REGION, required"`

	// Unique identifier of the instance.
	InstanceID string `env:"CLOUD_RUN_INSTANCE_ID, required"`

	// The port your HTTP server should listen on. By default, the service will
	// listen on port 8080.
	Port uint16 `env:"PORT, default=8080"`

	// The name of the Cloud Run service being run.
	ServiceName string `env:"K_SERVICE, default=firesync"`

	// The name of the Cloud Run revision being run.
	ServiceRevision string `env:"K_REVISION, required"`

	// The name of the Cloud Run configuration that created the revision.
	ServiceConfiguration string `env:"K_CONFIGURATION, required"`

	// Environment of the Cloud Run service.
	Environment string `env:"ENVIRONMENT, default=production"`

	// GracefulShutdownTimeout represents how long the service has to gracefully
	// terminate after receiving a SIGTERM or SIGINT signal.
	// Cloud Run will forcefully terminate the application after 10 seconds.
	// https://cloud.google.com/run/docs/reference/container-contract#instance-shutdown
	ServerGracefulShutdownTimeout time.Duration `env:"GRACEFUL_SHUTDOWN_TIMEOUT, default=8s"`

	// ServiceTimeout is the timeout for the service.
	RequestTimeout time.Duration `env:"REQUEST_TIMEOUT, default=10s"`

	// TracingEnabled enables tracing of the service.
	TracingEnabled bool `env:"ENABLE_TRACING, default=true"`

	// MetricsEnabled enables metrics of the service.
	MetricsEnabled bool `env:"ENABLE_METRICS, default=true"`

	// ProfilingEnabled enables profiling of the service.
	ProfilingEnabled bool `env:"ENABLE_PROFILING, default=false"`

	// DatabaseID is the ID of the Cloud Firestore database to replicate changes
	// to. If not provided, the default database will be used.
	// Can be in the format of "projects/{project_id}/databases/{database_id}"
	// or "{database_id}".
	Database string `env:"DATABASE, default=(default)"`

	// Topic is the name of the Cloud Pub/Sub topic to propagate changes to.
	// If not provided, the "firesync" topic will be used.
	// Can be in the format of "projects/{project_id}/topics/{topic_id}" or
	// "{topic_id}".
	Topic string `env:"TOPIC, default=firesync"`

	// LogLevel controls the verbosity of the logs.
	LogLevel zerolog.Level `env:"LOG_LEVEL, default=info"`

	// TraceSampleRatio is the ratio of traces to sample.
	TraceSampleRatio float64 `env:"OTEL_TRACES_SAMPLER_ARG, default=0.1"`
}

func environmentDefaults(env string) envconfig.Lookuper {
	switch env {
	case "local":
		return envconfig.MapLookuper(map[string]string{
			"GOOGLE_CLOUD_PROJECT":  "firesync",
			"GOOGLE_CLOUD_REGION":   "us-central1",
			"CLOUD_RUN_INSTANCE_ID": "local",
			"K_REVISION":            "local",
			"K_CONFIGURATION":       "local",
			"LOG_LEVEL":             "debug",
			"ENABLE_TRACING":        "false",
			"ENABLE_METRICS":        "false",
			"ENABLE_PROFILING":      "false",
		})
	case "development":
		return envconfig.MapLookuper(map[string]string{
			"ENABLE_PROFILING":        "true",
			"LOG_LEVEL":               "debug",
			"OTEL_TRACES_SAMPLER_ARG": "1.0",
		})
	case "staging":
		return envconfig.MapLookuper(map[string]string{
			"ENABLE_PROFILING":        "true",
			"LOG_LEVEL":               "debug",
			"OTEL_TRACES_SAMPLER_ARG": "0.5",
		})
	default:
		// production defaults are set in the struct tags
		return envconfig.MapLookuper(nil)
	}
}

func metadataLookuper(ctx context.Context) envconfig.Lookuper {
	return envconfig.LookuperFunc(func(key string) (string, bool) {
		if !metadata.OnGCEWithContext(ctx) {
			return "", false
		}

		switch key {
		case "GOOGLE_CLOUD_PROJECT":
			projectID, err := metadata.ProjectIDWithContext(ctx)
			if err != nil {
				return "", false
			}
			return projectID, true
		case "GOOGLE_CLOUD_REGION":
			region, err := metadata.GetWithContext(ctx, "instance/region")
			if err != nil {
				return "", false
			}
			return region[strings.LastIndexByte(region, '/')+1:], true
		case "CLOUD_RUN_INSTANCE_ID":
			instanceID, err := metadata.InstanceIDWithContext(ctx)
			if err != nil {
				return "", false
			}
			return instanceID, true
		}
		return "", false
	})
}

func Load(ctx context.Context) (*Config, error) {
	cfg := &Config{}
	opts := &envconfig.Config{
		Target: cfg,
		Lookuper: envconfig.MultiLookuper(
			envconfig.OsLookuper(),
			environmentDefaults(os.Getenv("ENVIRONMENT")),
			metadataLookuper(ctx),
		),
	}

	if err := envconfig.ProcessWith(ctx, opts); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) DatabaseID() string {
	dbID := c.Database

	if strings.HasPrefix(dbID, "projects/") {
		return dbID[strings.LastIndexByte(dbID, '/')+1:]
	}

	return dbID
}

func (c *Config) DatabaseProjectID() string {
	dbID := c.Database

	if strings.HasPrefix(dbID, "projects/") {
		pj := dbID[len("projects/"):]
		if idx := strings.IndexByte(pj, '/'); idx != -1 {
			return pj[:idx]
		}
		return ""
	}

	return c.ProjectID
}

func (c *Config) TopicID() string {
	topicID := c.Topic

	if strings.HasPrefix(topicID, "projects/") {
		return topicID[strings.LastIndexByte(topicID, '/')+1:]
	}

	return topicID
}

func (c *Config) TopicProjectID() string {
	topicID := c.Topic

	if strings.HasPrefix(topicID, "projects/") {
		pj := topicID[len("projects/"):]
		if idx := strings.IndexByte(pj, '/'); idx != -1 {
			return pj[:idx]
		}
		return ""
	}

	return c.ProjectID
}
