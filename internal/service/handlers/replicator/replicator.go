package replicator

import (
	"context"
	"net/http"

	"cloud.google.com/go/firestore"
)

type replicator struct {
	client *firestore.Client
}

func New(ctx context.Context, client *firestore.Client) http.Handler {
	return &replicator{client: client}
}

func (svc *replicator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
	w.Write([]byte("replicator"))
}
