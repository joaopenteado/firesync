package service

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/profiler"
	"cloud.google.com/go/pubsub"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/joaopenteado/firesync/internal/service/handlers/propagator"
	"github.com/joaopenteado/firesync/internal/service/handlers/replicator"
	"github.com/joaopenteado/runcfg"
	"github.com/joaopenteado/runcfg/otelcfg"
	"github.com/joaopenteado/runcfg/zerologcfg"
	"github.com/riandyrn/otelchi"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"
	"github.com/rs/zerolog/log"
	"github.com/sethvargo/go-envconfig"
	otelattr "go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
)

type Service interface {
	// Start starts the service and returns immediately if the service fails to
	// start. Blocks the caller until the service is gracefully stopped with
	// Stop. A non-sucessful start does not need to be stopped.
	Start(ctx context.Context) error

	// Stop stops the service, blocking until the context is cancelled or the
	// service is stopped. It returns an error if the service fails to stop
	// gracefully or the context is cancelled.
	Stop(ctx context.Context) error
}

type config struct {
	runcfg.Metadata `env:"RUNCFG_METADATA, decodeunset"`
	runcfg.Service  `env:"RUNCFG_SERVICE, decodeunset"`
	Timeout         time.Duration `env:"TIMEOUT, default=10s"`
	LogLevel        zerolog.Level `env:"LOG_LEVEL, default=info"`

	// DatabaseID is the ID of the Cloud Firestore database to replicate changes
	// to. If not provided, the default database will be used.
	DatabaseID string `env:"DATABASE_ID, default=(default)"`

	// DatabaseProjectID is the ID of the project hosting the Cloud Firestore
	// database. If not provided, the project ID of the service will be used.
	DatabaseProjectID string `env:"DATABASE_PROJECT_ID"`

	// Topic is the name of the Cloud Pub/Sub topic to propagate changes to.
	// If not provided, the "firesync" topic will be used.
	Topic string `env:"TOPIC, default=firesync"`

	// TopicProjectID is the ID of the project hosting the Cloud Pub/Sub topic.
	// If not provided, the project ID of the service will be used.
	TopicProjectID string `env:"TOPIC_PROJECT_ID"`
}

type service struct {
	config
	srv           *http.Server
	hasOtel       bool
	shutdownFuncs []func(context.Context) error
}

// New creates a new service instance. The context might be retained by other
// components and should not be cancelled unless performing a forceful shutdown.
// An unsucessful initialization does not need to be stopped.
func New(ctx context.Context) (Service, error) {
	return newService(ctx)
}

func newService(ctx context.Context) (svc *service, err error) {
	// Stop will not be called in the event of an initialization error, since
	// the service is not started. Ensure any resources created are cleaned up.
	svc = &service{}
	defer func(svc *service) {
		if err != nil {
			err = errors.Join(err, svc.Stop(ctx))
		}
	}(svc)

	// Load configuration
	if err := envconfig.Process(ctx, &svc.config); err != nil {
		return nil, err
	}

	if svc.TopicProjectID == "" {
		svc.TopicProjectID = svc.ProjectID
	}
	if svc.DatabaseProjectID == "" {
		svc.DatabaseProjectID = svc.ProjectID
	}

	// Setup global logger for Cloud Logging
	zerolog.SetGlobalLevel(svc.LogLevel)
	log.Logger = log.Hook(zerologcfg.Hook(svc.ProjectID))

	// Setup Cloud Profier
	profCfg := profiler.Config{
		Service:        svc.Name,
		ServiceVersion: svc.Revision,
		ProjectID:      svc.ProjectID,
		Instance:       svc.InstanceID,
		Zone:           svc.Region,
	}
	if err := profiler.Start(profCfg); err != nil {
		log.Err(err).Msg("failed to start profiler")
	}

	// Setup OpenTelemetry
	svc.hasOtel = true
	if err := svc.setupOpenTelemetry(ctx); err != nil {
		log.Warn().Err(err).Msg("failed to setup OpenTelemetry")
		svc.hasOtel = false
	}

	// Setup API clients
	pubsubClient, err := pubsub.NewClientWithConfig(ctx, svc.TopicProjectID, &pubsub.ClientConfig{
		EnableOpenTelemetryTracing: svc.hasOtel,
	})
	if err != nil {
		return nil, err
	}
	svc.shutdownFuncs = append(svc.shutdownFuncs, func(context.Context) error { return pubsubClient.Close() })

	firestoreClient, err := firestore.NewClientWithDatabase(ctx, svc.DatabaseProjectID, svc.DatabaseID)
	if err != nil {
		return nil, err
	}
	svc.shutdownFuncs = append(svc.shutdownFuncs, func(context.Context) error { return firestoreClient.Close() })

	// Setup router
	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.Timeout(svc.Timeout),
		middleware.Heartbeat("/healthz"), // Liveness probe
	)

	if svc.hasOtel {
		r.Use(otelchi.Middleware(svc.Name, otelchi.WithChiRoutes(r)))
	}

	r.Use(hlog.NewHandler(log.Logger))

	// Register handlers
	r.Route("/v1", func(r chi.Router) {
		r.Method(http.MethodPost, "/replicate", replicator.New(ctx, firestoreClient))

		r.With(traceCloudEventHeaders).
			Method(http.MethodPost, "/propagate", propagator.New(ctx, pubsubClient.Topic(svc.Topic)))
	})

	// Setup HTTP server
	svc.srv = &http.Server{
		Addr:    ":" + strconv.FormatUint(uint64(svc.Port), 10),
		Handler: r,
		// TODO: extra options
	}

	return svc, nil
}

func (s *service) setupOpenTelemetry(ctx context.Context) error {
	attrs := resource.Default().Attributes()

	// Required to assign a project to the traces in Cloud Trace
	// https://cloud.google.com/trace/docs/migrate-to-otlp-endpoints
	attrs = append(attrs, otelattr.String("gcp.project_id", s.ProjectID))

	// For a nicer UI in Cloud Trace, add the service name
	attrs = append(attrs, otelattr.String("service.name", s.Name))

	// OpenTelemetry configuration
	res := otelcfg.NewServiceResource(&s.Metadata, &s.Service, otelcfg.WithAttributes(attrs...))

	tp, err := otelcfg.SetupTracerProvider(ctx,
		otelcfg.WithTracerProviderOptions(trace.WithResource(res)),
	)
	if err != nil {
		return err
	}

	s.shutdownFuncs = append(s.shutdownFuncs, tp.Shutdown)
	return nil
}

func (s *service) Start(ctx context.Context) error {
	s.shutdownFuncs = append(s.shutdownFuncs, s.srv.Shutdown)
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

func (s *service) Stop(ctx context.Context) (retErr error) {
	for _, fn := range slices.Backward(s.shutdownFuncs) {
		fnName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
		if err := fn(ctx); err != nil {
			log.Err(err).
				Str("function", fnName).
				Msg("failed to execute shutdown function")
			retErr = errors.Join(retErr, err)
			continue
		}

		log.Debug().
			Str("function", fnName).
			Msg("shutdown function executed successfully")
	}
	return retErr
}
