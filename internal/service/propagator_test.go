package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/googleapis/google-cloudevents-go/cloud/firestoredata"
	"github.com/joaopenteado/firesync/internal/model"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---- Mocks ----

type mockTopic struct {
	msg    *pubsub.Message
	result PublishResult
}

func (m *mockTopic) Publish(ctx context.Context, msg *pubsub.Message) PublishResult {
	m.msg = msg
	if m.result == nil {
		m.result = &mockResult{id: "id"}
	}
	return m.result
}

type mockResult struct {
	id  string
	err error
}

func (r *mockResult) Get(ctx context.Context) (string, error) { return r.id, r.err }

type mockFirestore struct {
	tx  Transaction
	err error
}

func (m *mockFirestore) RunTransaction(ctx context.Context, f func(context.Context, Transaction) error) error {
	if m.err != nil {
		return m.err
	}
	return f(ctx, m.tx)
}

func (m *mockFirestore) Doc(p string) *firestore.DocumentRef { return &firestore.DocumentRef{Path: p} }

type mockTx struct {
	get    func(string) (DocumentSnapshot, error)
	update func(string, []Update, time.Time) error
	delete func(string, time.Time) error
	create func(string, interface{}) error
}

func (m *mockTx) Get(p string) (DocumentSnapshot, error) {
	if m.get != nil {
		return m.get(p)
	}
	return nil, status.Error(codes.NotFound, "not found")
}
func (m *mockTx) Update(p string, u []Update, ts time.Time) error {
	if m.update != nil {
		return m.update(p, u, ts)
	}
	return nil
}
func (m *mockTx) Delete(p string, ts time.Time) error {
	if m.delete != nil {
		return m.delete(p, ts)
	}
	return nil
}
func (m *mockTx) Create(p string, data interface{}) error {
	if m.create != nil {
		return m.create(p, data)
	}
	return nil
}

type mockSnap struct {
	exists bool
	data   interface{}
	err    error
}

func (s *mockSnap) Exists() bool { return s.exists }

func (s *mockSnap) DataTo(dst interface{}) error {
	if s.err != nil {
		return s.err
	}
	dv := reflect.ValueOf(dst)
	dv.Elem().Set(reflect.ValueOf(s.data).Elem())
	return nil
}

// helper to create event
var defaultName = model.DocumentName{ProjectID: "p", DatabaseID: "d", Path: "users/1"}

func sampleEvent(typ model.EventType, ts time.Time) *model.Event {
	return &model.Event{
		Type:      typ,
		Name:      defaultName,
		Timestamp: ts,
		Data:      &firestoredata.DocumentEventData{Value: &firestoredata.Document{Name: defaultName.String()}},
	}
}

// ---- Tests ----

func TestPropagate_SkipTypes(t *testing.T) {
	svc := NewPropagator(&mockTopic{}, &mockFirestore{}, time.Second, noop.Meter{})
	for _, typ := range []model.EventType{model.EventTypeReplicated, model.EventTypeTombstone} {
		evt := sampleEvent(typ, time.Now())
		res, err := svc.Propagate(context.Background(), evt)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if res != PropagationResultSkipped {
			t.Fatalf("result = %v, want skipped", res)
		}
	}
}

func TestPropagate_CreateSuccess(t *testing.T) {
	topic := &mockTopic{result: &mockResult{id: "1"}}
	tx := &mockTx{
		get:    func(string) (DocumentSnapshot, error) { return &mockSnap{exists: false}, nil },
		update: func(p string, u []Update, ts time.Time) error { return nil },
	}
	svc := NewPropagator(topic, &mockFirestore{tx: tx}, time.Second, noop.Meter{})
	evt := sampleEvent(model.EventTypeCreated, time.Now())
	res, err := svc.Propagate(context.Background(), evt)
	if err != nil || res != PropagationResultSuccess {
		t.Fatalf("res=%v err=%v", res, err)
	}
	if topic.msg == nil {
		t.Fatalf("message not published")
	}
}

func TestPropagate_ProcessError(t *testing.T) {
	tx := &mockTx{get: func(string) (DocumentSnapshot, error) { return nil, errors.New("get err") }}
	svc := NewPropagator(&mockTopic{}, &mockFirestore{tx: tx}, time.Second, noop.Meter{})
	evt := sampleEvent(model.EventTypeCreated, time.Now())
	res, err := svc.Propagate(context.Background(), evt)
	if err == nil || res != PropagationResultError {
		t.Fatalf("expected error")
	}
}

// processCreateEvent tests
func TestProcessCreateEvent_TombstoneNewer(t *testing.T) {
	tomb := &model.Tombstone{Timestamp: timestamppb.New(time.Unix(2, 0))}
	tx := &mockTx{
		get:    func(p string) (DocumentSnapshot, error) { return &mockSnap{exists: true, data: tomb}, nil },
		delete: func(p string, ts time.Time) error { return nil },
	}
	svc := NewPropagator(&mockTopic{}, &mockFirestore{tx: tx}, time.Second, noop.Meter{})
	evt := sampleEvent(model.EventTypeCreated, time.Unix(1, 0))
	ok, err := svc.processCreateEvent(context.Background(), evt)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if ok {
		t.Fatalf("should not propagate")
	}
}

func TestProcessCreateEvent_Update(t *testing.T) {
	tx := &mockTx{
		get:    func(string) (DocumentSnapshot, error) { return &mockSnap{exists: false}, nil },
		update: func(p string, u []Update, ts time.Time) error { return nil },
	}
	svc := NewPropagator(&mockTopic{}, &mockFirestore{tx: tx}, time.Second, noop.Meter{})
	evt := sampleEvent(model.EventTypeCreated, time.Unix(1, 0))
	ok, err := svc.processCreateEvent(context.Background(), evt)
	if err != nil || !ok {
		t.Fatalf("want propagate true err nil got %v %v", ok, err)
	}
}

// processUpdateEvent
func TestProcessUpdateEvent_TombstoneNewer(t *testing.T) {
	tomb := &model.Tombstone{Timestamp: timestamppb.New(time.Unix(2, 0))}
	tx := &mockTx{
		get:    func(string) (DocumentSnapshot, error) { return &mockSnap{exists: true, data: tomb}, nil },
		delete: func(p string, ts time.Time) error { return nil },
	}
	svc := NewPropagator(&mockTopic{}, &mockFirestore{tx: tx}, time.Second, noop.Meter{})
	evt := sampleEvent(model.EventTypeUpdated, time.Unix(1, 0))
	ok, err := svc.processUpdateEvent(context.Background(), evt)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if ok {
		t.Fatalf("should not propagate")
	}
}

func TestProcessUpdateEvent_Update(t *testing.T) {
	tx := &mockTx{
		get:    func(string) (DocumentSnapshot, error) { return &mockSnap{exists: false}, nil },
		update: func(p string, u []Update, ts time.Time) error { return nil },
	}
	svc := NewPropagator(&mockTopic{}, &mockFirestore{tx: tx}, time.Second, noop.Meter{})
	evt := sampleEvent(model.EventTypeUpdated, time.Unix(1, 0))
	ok, err := svc.processUpdateEvent(context.Background(), evt)
	if err != nil || !ok {
		t.Fatalf("want propagate true err nil got %v %v", ok, err)
	}
}

// processDeleteEvent
func TestProcessDeleteEvent_Create(t *testing.T) {
	tx := &mockTx{
		get: func(p string) (DocumentSnapshot, error) {
			if p == defaultName.Path {
				return &mockSnap{exists: false}, nil
			}
			return &mockSnap{exists: false}, nil
		},
		create: func(p string, data interface{}) error { return nil },
	}
	svc := NewPropagator(&mockTopic{}, &mockFirestore{tx: tx}, time.Second, noop.Meter{})
	evt := sampleEvent(model.EventTypeDeleted, time.Unix(1, 0))
	ok, err := svc.processDeleteEvent(context.Background(), evt)
	if err != nil || !ok {
		t.Fatalf("want propagate true got %v %v", ok, err)
	}
}

func TestProcessDeleteEvent_TombstoneNewer(t *testing.T) {
	tomb := &model.Tombstone{Timestamp: timestamppb.New(time.Unix(2, 0))}
	tx := &mockTx{
		get: func(p string) (DocumentSnapshot, error) {
			if p == defaultName.TombstonePath() {
				return &mockSnap{exists: true, data: tomb}, nil
			}
			return &mockSnap{exists: false}, nil
		},
	}
	svc := NewPropagator(&mockTopic{}, &mockFirestore{tx: tx}, time.Second, noop.Meter{})
	evt := sampleEvent(model.EventTypeDeleted, time.Unix(1, 0))
	ok, err := svc.processDeleteEvent(context.Background(), evt)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if ok {
		t.Fatalf("should not propagate")
	}
}
