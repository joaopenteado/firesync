package middleware

import (
	"net/http"

	"github.com/rs/zerolog"
)

// Logger returns a middleware that adds the zerolog logger to the request
// context. It works very similarily to hlog.NewHandler with the difference that
// it adds the request context to the logger. This is especially important for
// tracing, where the request context is used to trace the request through the
// system. Ensure that this middleware is ran after any other middleware that
// injects tracing information to the request context.
func Logger(logger zerolog.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			logger := logger.With().Ctx(ctx).Logger()
			r = r.WithContext(logger.WithContext(ctx))
			next.ServeHTTP(w, r)
		})
	}
}
