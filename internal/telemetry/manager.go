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
	ProjectID                  string
	ServiceName                string
	ServiceRevision            string
	InstanceID                 string
	Environment                string
	TracingExporter            string
	MetricsExporter            string
	OTLPProtocol               string
	ConsoleExporterPrettyPrint bool
	TraceSampleRatio           float64
}

func NewManager(ctx context.Context, opts Options) *Manager {
	m := &Manager{}

	if opts.TracingExporter == "none" && opts.MetricsExporter == "none" {
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
	m.initMetricProvider(ctx, res, opts)

	return m
}

func (m *Manager) initTracerProvider(ctx context.Context, res *resource.Resource, opts Options) {
	if opts.TracingExporter == "none" {
		return
	}

	var exporter sdktrace.SpanExporter
	var err error

	switch opts.TracingExporter {
	case "console":
		exporter, err = NewStdoutTraceExporter(opts.ConsoleExporterPrettyPrint)
	case "otlp":
		if opts.OTLPProtocol != "grpc" {
			log.Warn().Str("otlp_protocol", opts.OTLPProtocol).Msg("unsupported otlp protocol: tracing will be disabled")
			return
		}
		exporter, err = NewOTLPTraceExporter(ctx)
	default:
		log.Warn().Str("tracing_exporter", opts.TracingExporter).Msg("unsupported tracing exporter: tracing will be disabled")
		return
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

func (m *Manager) initMetricProvider(ctx context.Context, res *resource.Resource, opts Options) {
	if opts.MetricsExporter == "none" {
		return
	}

	var me sdkmetric.Exporter
	var err error

	switch opts.MetricsExporter {
	case "console":
		me, err = NewStdoutMetricExporter(opts.ConsoleExporterPrettyPrint)
	case "otlp":
		if opts.OTLPProtocol != "grpc" {
			log.Warn().Str("otlp_protocol", opts.OTLPProtocol).Msg("unsupported otlp protocol: metrics will be disabled")
			return
		}
		me, err = NewOTLPMetricExporter(ctx)
	case "googlecloudmetrics":
		me, err = NewCloudMonitoringMetricExporter(opts.ProjectID)
	default:
		log.Warn().Str("metrics_exporter", opts.MetricsExporter).Msg("unsupported metrics exporter: metrics will be disabled")
		return
	}

	if err != nil {
		log.Warn().Err(err).Msg("failed to create metric exporter: metrics will be disabled")
		return
	}

	mr := sdkmetric.NewPeriodicReader(me)

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
