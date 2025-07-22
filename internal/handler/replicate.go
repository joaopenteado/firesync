package handler

import (
	"context"
	"net/http"

	"github.com/joaopenteado/firesync/internal/service"
)

type Replicator interface {
	Replicate(ctx context.Context) (service.ReplicationResult, error)
}

func Replicate(svc Replicator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}
