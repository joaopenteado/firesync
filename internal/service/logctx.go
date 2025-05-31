package service

import (
	"net/http"

	"github.com/rs/zerolog"
)

func logCtx(logger zerolog.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			logger := logger.With().Ctx(ctx).Logger()
			r = r.WithContext(logger.WithContext(ctx))
			next.ServeHTTP(w, r)
		})
	}
}
