package telemetry

import (
	"context"
	"errors"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/metric"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

type Manager struct {
	tp *sdktrace.TracerProvider
	mp *sdkmetric.MeterProvider
}

type Options struct {
	ProjectID        string
	ServiceName      string
	ServiceRevision  string
	InstanceID       string
	TracingEnabled   bool
	MetricsEnabled   bool
	Environment      string
	TraceSampleRatio float64
}

func NewManager(ctx context.Context, opts Options) *Manager {
	m := &Manager{}

	if !opts.TracingEnabled && !opts.MetricsEnabled {
		return m // no-op manager, nothing to clean up
	}

	res, err := NewResource(ctx, ResourceConfig{
		ProjectID:       opts.ProjectID,
		ServiceName:     opts.ServiceName,
		ServiceRevision: opts.ServiceRevision,
		InstanceID:      opts.InstanceID,
		Environment:     opts.Environment,
	})
	if err != nil {
		log.Warn().Err(err).Msg("failed to create telemetry resource: tracing and metrics will be disabled")
		return m
	}

	m.initTracerProvider(ctx, res, opts)
	m.initMetricProvider(res, opts)

	return m
}

func (m *Manager) initTracerProvider(ctx context.Context, res *resource.Resource, opts Options) {
	if !opts.TracingEnabled {
		return
	}

	var exporter sdktrace.SpanExporter
	var err error

	if opts.Environment == "local" {
		exporter, err = NewStdoutTraceExporter()
	} else {
		exporter, err = NewCloudTraceExporter(ctx)
	}

	if err != nil {
		log.Warn().Err(err).Msg("failed to create trace exporter: tracing will be disabled")
		return
	}

	m.tp = sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(opts.TraceSampleRatio)),
	)
}

func (m *Manager) initMetricProvider(res *resource.Resource, opts Options) {
	if !opts.MetricsEnabled {
		return
	}

	var mr sdkmetric.Reader
	var err error

	if opts.Environment == "local" {
		mr, err = NewStdoutMetricReader()
	} else {
		mr, err = NewCloudMonitoringMetricReader(opts.ProjectID)
	}

	if err != nil {
		log.Warn().Err(err).Msg("failed to create metric reader: metrics will be disabled")
		return
	}

	m.mp = sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(mr),
		sdkmetric.WithResource(res),
	)
}

func (m *Manager) TracerProvider() trace.TracerProvider {
	if m.tp == nil {
		return nooptrace.NewTracerProvider()
	}
	return m.tp
}

func (m *Manager) MeterProvider() metric.MeterProvider {
	if m.mp == nil {
		return noopmetric.NewMeterProvider()
	}
	return m.mp
}

func (m *Manager) Shutdown(ctx context.Context) error {
	var err error
	if m.tp != nil {
		err = m.tp.Shutdown(ctx)
	}
	if m.mp != nil {
		err = errors.Join(err, m.mp.Shutdown(ctx))
	}
	return err
}
