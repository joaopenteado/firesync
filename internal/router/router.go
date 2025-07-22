package router

import (
	"net/http"
	"time"

	chimiddleware "github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/joaopenteado/firesync/internal/middleware"
	"github.com/riandyrn/otelchi"
	"github.com/rs/zerolog/log"
)

type Config struct {
	PropagateHandler http.Handler
	ReplicateHandler http.Handler
	ServiceName      string
	TracingEnabled   bool
}

func New(cfg Config) http.Handler {
	r := chi.NewRouter()

	r.Use(
		chimiddleware.Recoverer,
		chimiddleware.Timeout(10*time.Second),
		chimiddleware.Heartbeat("/healthz"), // Liveness probe
		middleware.Logger(log.Logger),
	)

	if cfg.TracingEnabled {
		r.Use(otelchi.Middleware(cfg.ServiceName, otelchi.WithChiRoutes(r)))
	}

	r.Route("/v1", func(r chi.Router) {
		// Propagate receives CloudEvents from Eventarc/PubSub/Firestore
		r.With(middleware.CloudEvent).
			Method(http.MethodPost, "/propagate", cfg.PropagateHandler)

		r.Method(http.MethodPost, "/replicate", cfg.ReplicateHandler)
	})

	return r
}
