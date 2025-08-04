package service

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
)

// FirestoreClient defines the subset of Firestore functionality required by the
// propagator. It is satisfied by the real Firestore client and can be mocked in
// tests.
type FirestoreClient interface {
	RunTransaction(ctx context.Context, f func(context.Context, Transaction) error) error
	Doc(path string) *firestore.DocumentRef
}

// Transaction represents a Firestore transaction. Implementations should
// provide the minimal operations used by the propagator.
type Transaction interface {
	Get(path string) (DocumentSnapshot, error)
	Update(path string, updates []Update, ts time.Time) error
	Delete(path string, ts time.Time) error
	Create(path string, data interface{}) error
}

// DocumentSnapshot is a wrapper around Firestore's DocumentSnapshot allowing it
// to be mocked in tests.
type DocumentSnapshot interface {
	Exists() bool
	DataTo(interface{}) error
}

// Update mirrors firestore.Update but allows us to decouple from the Firestore
// client in tests.
type Update struct {
	Path  string
	Value interface{}
}

// firestoreClientAdapter adapts the real firestore.Client to FirestoreClient.
type firestoreClientAdapter struct{ *firestore.Client }

// NewFirestoreClientAdapter wraps a firestore.Client so it can be consumed by
// the propagator.
func NewFirestoreClientAdapter(c *firestore.Client) FirestoreClient {
	if c == nil {
		return nil
	}
	return &firestoreClientAdapter{c}
}

func (c *firestoreClientAdapter) RunTransaction(ctx context.Context, f func(context.Context, Transaction) error) error {
	return c.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		return f(ctx, &transactionAdapter{Transaction: tx, client: c.Client})
	})
}

func (c *firestoreClientAdapter) Doc(path string) *firestore.DocumentRef { return c.Client.Doc(path) }

// transactionAdapter adapts firestore.Transaction to our Transaction interface.
type transactionAdapter struct {
	*firestore.Transaction
	client *firestore.Client
}

func (t *transactionAdapter) Get(path string) (DocumentSnapshot, error) {
	snap, err := t.Transaction.Get(t.client.Doc(path))
	if err != nil {
		return nil, err
	}
	return &documentSnapshotAdapter{snap}, nil
}

func (t *transactionAdapter) Update(path string, updates []Update, ts time.Time) error {
	fsUpdates := make([]firestore.Update, len(updates))
	for i, u := range updates {
		fsUpdates[i] = firestore.Update{Path: u.Path, Value: u.Value}
	}
	return t.Transaction.Update(t.client.Doc(path), fsUpdates, firestore.LastUpdateTime(ts))
}

func (t *transactionAdapter) Delete(path string, ts time.Time) error {
	return t.Transaction.Delete(t.client.Doc(path), firestore.LastUpdateTime(ts))
}

func (t *transactionAdapter) Create(path string, data interface{}) error {
	return t.Transaction.Create(t.client.Doc(path), data)
}

// documentSnapshotAdapter adapts firestore.DocumentSnapshot to our interface.
type documentSnapshotAdapter struct{ *firestore.DocumentSnapshot }

func (s *documentSnapshotAdapter) Exists() bool { return s.DocumentSnapshot.Exists() }

func (s *documentSnapshotAdapter) DataTo(v interface{}) error { return s.DocumentSnapshot.DataTo(v) }
