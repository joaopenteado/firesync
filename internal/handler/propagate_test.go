package handler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/googleapis/google-cloudevents-go/cloud/firestoredata"
	"github.com/joaopenteado/firesync/internal/model"
	"github.com/joaopenteado/firesync/internal/service"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// stubPropagator is a test implementation of the Propagator interface.
type stubPropagator struct {
	result service.PropagationResult
	err    error
	event  *model.Event
}

func (s *stubPropagator) Propagate(ctx context.Context, e *model.Event) (service.PropagationResult, error) {
	s.event = e
	return s.result, s.err
}

type failingReader struct{ err error }

func (f failingReader) Read(p []byte) (int, error) { return 0, f.err }

func sampleCreateEvent(t *testing.T) []byte {
	t.Helper()
	evt := &firestoredata.DocumentEventData{
		Value: &firestoredata.Document{
			Name:       "projects/p/databases/d/documents/users/1",
			UpdateTime: timestamppb.New(time.Unix(1, 0)),
		},
	}
	b, err := protojson.Marshal(evt)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}
	return b
}

func TestParseFirestoreDocumentEventData(t *testing.T) {
	evt := &firestoredata.DocumentEventData{Value: &firestoredata.Document{Name: "projects/p/databases/d/documents/coll/doc"}}
	protoBody, err := proto.Marshal(evt)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	jsonBody, err := protojson.Marshal(evt)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}
	readErr := errors.New("read error")

	tests := []struct {
		name        string
		contentType string
		r           io.Reader
		wantEvent   *firestoredata.DocumentEventData
		wantErr     error
	}{
		{"protobuf", "application/protobuf", bytes.NewReader(protoBody), evt, nil},
		{"json", "application/json", bytes.NewReader(jsonBody), evt, nil},
		{"unsupported", "text/plain", bytes.NewReader([]byte("foo")), nil, unsupportedMediaType},
		{"read error", "application/json", failingReader{readErr}, nil, readErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFirestoreDocumentEventData(tt.contentType, tt.r)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if !proto.Equal(got, tt.wantEvent) {
				t.Fatalf("got %v, want %v", got, tt.wantEvent)
			}
		})
	}
}

func TestPropagate_StatusCodes(t *testing.T) {
	body := sampleCreateEvent(t)
	tests := []struct {
		name   string
		result service.PropagationResult
		err    error
		opts   []propagateOption
		want   int
	}{
		{"success", service.PropagationResultSuccess, nil, nil, http.StatusAccepted},
		{"success forced 200", service.PropagationResultSuccess, nil, []propagateOption{WithHTTP200Acknowledgement(true)}, http.StatusOK},
		{"skipped", service.PropagationResultSkipped, nil, nil, http.StatusNoContent},
		{"skipped forced 200", service.PropagationResultSkipped, nil, []propagateOption{WithHTTP200Acknowledgement(true)}, http.StatusOK},
		{"error result", service.PropagationResultError, nil, nil, http.StatusInternalServerError},
		{"svc error", service.PropagationResultSuccess, errors.New("svc error"), nil, http.StatusInternalServerError},
		{"unknown result", service.PropagationResultUnknown, nil, nil, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &stubPropagator{result: tt.result, err: tt.err}
			handler := Propagate(svc, tt.opts...)
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			if rr.Code != tt.want {
				t.Fatalf("status = %d, want %d", rr.Code, tt.want)
			}
		})
	}
}

func TestPropagate_ParseErrors(t *testing.T) {
	validBody := sampleCreateEvent(t)
	invalidJSON := []byte("invalid json")
	emptyEvent, err := protojson.Marshal(&firestoredata.DocumentEventData{})
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}
	tests := []struct {
		name        string
		contentType string
		body        []byte
		want        int
	}{
		{"unsupported content type", "text/plain", validBody, http.StatusUnsupportedMediaType},
		{"invalid body", "application/json", invalidJSON, http.StatusBadRequest},
		{"parse event error", "application/json", emptyEvent, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &stubPropagator{result: service.PropagationResultSuccess}
			handler := Propagate(svc)
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.contentType)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			if rr.Code != tt.want {
				t.Fatalf("status = %d, want %d", rr.Code, tt.want)
			}
			if svc.event != nil {
				t.Fatalf("service should not be called on parse errors")
			}
		})
	}
}

func TestPropagate_EventTimeHeader(t *testing.T) {
	delEvent := &firestoredata.DocumentEventData{
		OldValue: &firestoredata.Document{
			Name: "projects/p/databases/d/documents/users/1",
		},
	}
	body, err := protojson.Marshal(delEvent)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}
	ts := time.Unix(10, 0).UTC()
	svc := &stubPropagator{result: service.PropagationResultSuccess}
	handler := Propagate(svc)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("ce-time", ts.Format(time.RFC3339Nano))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if svc.event == nil {
		t.Fatalf("service not called")
	}
	if !svc.event.Timestamp.Equal(ts) {
		t.Fatalf("timestamp = %v, want %v", svc.event.Timestamp, ts)
	}
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusAccepted)
	}
}
