package config

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	_ "cloud.google.com/go/compute/metadata"
	"github.com/rs/zerolog"
)

import _ "unsafe"

//go:linkname metadataOnGCEOnce cloud.google.com/go/compute/metadata.onGCEOnce
var metadataOnGCEOnce sync.Once

//go:linkname metadataOnGCE cloud.google.com/go/compute/metadata.onGCE
var metadataOnGCE bool

func resetMetadataCache() {
	metadataOnGCEOnce = sync.Once{}
	metadataOnGCE = false
}

func TestEnvironmentDefaults(t *testing.T) {
	tests := []struct {
		env  string
		want map[string]string
	}{
		{
			env: EnvironmentLocal,
			want: map[string]string{
				"GOOGLE_CLOUD_PROJECT":           "firesync",
				"GOOGLE_CLOUD_REGION":            "us-central1",
				"CLOUD_RUN_INSTANCE_ID":          "local",
				"K_SERVICE":                      "firesync",
				"K_REVISION":                     "local",
				"K_CONFIGURATION":                "local",
				"FORCE_HTTP_200_ACKNOWLEDGEMENT": "true",
				"LOG_LEVEL":                      "debug",
				"LOG_PRETTY":                     "true",
				"OTEL_TRACES_EXPORTER":           "otlp",
				"OTEL_METRICS_EXPORTER":          "otlp",
				"OTEL_TRACES_SAMPLER_ARG":        "1.0",
			},
		},
		{
			env: EnvironmentDevelopment,
			want: map[string]string{
				"GOOGLE_CLOUD_PROFILER_ENABLED": "true",
				"LOG_LEVEL":                     "debug",
				"OTEL_TRACES_EXPORTER":          "otlp",
				"OTEL_METRICS_EXPORTER":         "googlecloudmetrics",
				"OTEL_TRACES_SAMPLER_ARG":       "1.0",
			},
		},
		{env: EnvironmentProduction, want: map[string]string{}},
	}

	for _, tt := range tests {
		t.Run(tt.env, func(t *testing.T) {
			lu := environmentDefaults(tt.env)
			for k, v := range tt.want {
				got, ok := lu.Lookup(k)
				if !ok || got != v {
					t.Fatalf("%s: got %q, %v want %q", k, got, ok, v)
				}
			}
			// ensure nonexistent key not found
			if _, ok := lu.Lookup("SOME_RANDOM_KEY"); ok {
				t.Fatalf("unexpected key found")
			}
		})
	}
}

func TestMetadataLookuper(t *testing.T) {
	t.Run("not on gce", func(t *testing.T) {
		resetMetadataCache()
		lookup := metadataLookuper(context.Background())
		if v, ok := lookup.Lookup("GOOGLE_CLOUD_PROJECT"); ok || v != "" {
			t.Fatalf("expected empty lookup; got %q, %v", v, ok)
		}
	})

	t.Run("on gce", func(t *testing.T) {
		resetMetadataCache()
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Metadata-Flavor", "Google")
			switch r.URL.Path {
			case "/computeMetadata/v1/project/project-id":
				fmt.Fprint(w, "test-project")
			case "/computeMetadata/v1/instance/region":
				fmt.Fprint(w, "projects/test/regions/test-region")
			case "/computeMetadata/v1/instance/id":
				fmt.Fprint(w, "test-instance")
			default:
				http.NotFound(w, r)
			}
		}))
		defer ts.Close()

		host := strings.TrimPrefix(ts.URL, "http://")
		t.Setenv("GCE_METADATA_HOST", host)

		lookup := metadataLookuper(context.Background())
		if v, ok := lookup.Lookup("GOOGLE_CLOUD_PROJECT"); !ok || v != "test-project" {
			t.Fatalf("project: got %q, %v", v, ok)
		}
		if v, ok := lookup.Lookup("GOOGLE_CLOUD_REGION"); !ok || v != "test-region" {
			t.Fatalf("region: got %q, %v", v, ok)
		}
		if v, ok := lookup.Lookup("CLOUD_RUN_INSTANCE_ID"); !ok || v != "test-instance" {
			t.Fatalf("instance: got %q, %v", v, ok)
		}
	})
}

func TestLoad(t *testing.T) {
	resetMetadataCache()
	t.Setenv("ENVIRONMENT", EnvironmentLocal)
	t.Setenv("K_SERVICE", "custom-service")

	cfg, err := Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Environment != EnvironmentLocal {
		t.Fatalf("Environment = %q", cfg.Environment)
	}
	if cfg.ServiceName != "custom-service" {
		t.Fatalf("ServiceName = %q", cfg.ServiceName)
	}
	if cfg.ProjectID != "firesync" || cfg.Region != "us-central1" || cfg.InstanceID != "local" {
		t.Fatalf("unexpected metadata values: %+v", cfg)
	}
	if cfg.Port != 8080 {
		t.Fatalf("Port = %d", cfg.Port)
	}
	if !cfg.ForceHTTP200Acknowledgement {
		t.Fatalf("ForceHTTP200Acknowledgement = false")
	}
	if cfg.LogLevel != zerolog.DebugLevel {
		t.Fatalf("LogLevel = %v", cfg.LogLevel)
	}
	if !cfg.LogPretty {
		t.Fatalf("LogPretty = false")
	}
	if cfg.TracingExporter != "otlp" || cfg.MetricsExporter != "otlp" {
		t.Fatalf("unexpected exporters: trace=%q metrics=%q", cfg.TracingExporter, cfg.MetricsExporter)
	}
	if cfg.TraceSampleRatio != 1.0 {
		t.Fatalf("TraceSampleRatio = %v", cfg.TraceSampleRatio)
	}
	if cfg.ShutdownTimeout != 8*time.Second {
		t.Fatalf("ShutdownTimeout = %v", cfg.ShutdownTimeout)
	}
	if cfg.RequestTimeout != 10*time.Second {
		t.Fatalf("RequestTimeout = %v", cfg.RequestTimeout)
	}
	if cfg.Database != "(default)" || cfg.DatabaseID() != "(default)" {
		t.Fatalf("unexpected database: %q %q", cfg.Database, cfg.DatabaseID())
	}
	if cfg.Topic != "firesync" {
		t.Fatalf("Topic = %q", cfg.Topic)
	}
}

func TestDatabaseID(t *testing.T) {
	cfg := &Config{Database: "projects/test-project/databases/test-db"}
	if got := cfg.DatabaseID(); got != "test-db" {
		t.Fatalf("got %q, want %q", got, "test-db")
	}
	cfg.Database = "simple"
	if got := cfg.DatabaseID(); got != "simple" {
		t.Fatalf("got %q, want %q", got, "simple")
	}
}
