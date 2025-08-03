package model

import (
	"testing"
	"time"

	"github.com/googleapis/google-cloudevents-go/cloud/firestoredata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestHasAnyFiresyncFields(t *testing.T) {
	tests := []struct {
		fields []string
		want   bool
	}{
		{nil, false},
		{[]string{"foo", "bar"}, false},
		{[]string{"_firesync"}, true},
		{[]string{"_firesync.version"}, true},
		{[]string{"prefix_firesync"}, false},
	}
	for _, tt := range tests {
		if got := hasAnyFiresyncFields(tt.fields); got != tt.want {
			t.Errorf("hasAnyFiresyncFields(%v) = %v, want %v", tt.fields, got, tt.want)
		}
	}
}

func doc(name string, fields map[string]*firestoredata.Value, ts time.Time) *firestoredata.Document {
	return &firestoredata.Document{
		Name:       name,
		Fields:     fields,
		UpdateTime: timestamppb.New(ts),
	}
}

func TestParseEvent(t *testing.T) {
	basePath := "projects/p/databases/d/documents/users/1"
	tombPath := "projects/p/databases/d/documents/_firesync/abc"
	ts1 := time.Unix(1, 0)
	ts2 := time.Unix(2, 0)
	ts3 := time.Unix(3, 0)
	ts4 := time.Unix(4, 0)

	tests := []struct {
		name      string
		event     *firestoredata.DocumentEventData
		eventTime time.Time
		wantType  EventType
		wantName  DocumentName
		wantTime  time.Time
		wantErr   string
	}{
		{
			name:      "created",
			event:     &firestoredata.DocumentEventData{Value: doc(basePath, nil, ts1)},
			eventTime: ts2,
			wantType:  EventTypeCreated,
			wantName:  DocumentName{ProjectID: "p", DatabaseID: "d", Path: "users/1"},
			wantTime:  ts1,
		},
		{
			name: "updated",
			event: &firestoredata.DocumentEventData{
				Value:      doc(basePath, nil, ts2),
				OldValue:   doc(basePath, nil, ts1),
				UpdateMask: &firestoredata.DocumentMask{FieldPaths: []string{"f"}},
			},
			eventTime: ts3,
			wantType:  EventTypeUpdated,
			wantName:  DocumentName{ProjectID: "p", DatabaseID: "d", Path: "users/1"},
			wantTime:  ts2,
		},
		{
			name: "deleted",
			event: &firestoredata.DocumentEventData{
				OldValue: doc(basePath, nil, ts1),
			},
			eventTime: ts3,
			wantType:  EventTypeDeleted,
			wantName:  DocumentName{ProjectID: "p", DatabaseID: "d", Path: "users/1"},
			wantTime:  ts3,
		},
		{
			name: "replicated create",
			event: &firestoredata.DocumentEventData{
				Value: doc(basePath, map[string]*firestoredata.Value{"_firesync": &firestoredata.Value{}}, ts1),
			},
			eventTime: ts2,
			wantType:  EventTypeReplicated,
			wantName:  DocumentName{ProjectID: "p", DatabaseID: "d", Path: "users/1"},
			wantTime:  ts1,
		},
		{
			name: "replicated update",
			event: &firestoredata.DocumentEventData{
				Value:      doc(basePath, nil, ts2),
				OldValue:   doc(basePath, nil, ts1),
				UpdateMask: &firestoredata.DocumentMask{FieldPaths: []string{"_firesync.ver"}},
			},
			eventTime: ts3,
			wantType:  EventTypeReplicated,
			wantName:  DocumentName{ProjectID: "p", DatabaseID: "d", Path: "users/1"},
			wantTime:  ts2,
		},
		{
			name: "tombstone value",
			event: &firestoredata.DocumentEventData{
				Value: doc(tombPath, nil, ts1),
			},
			eventTime: ts2,
			wantType:  EventTypeTombstone,
			wantName:  DocumentName{ProjectID: "p", DatabaseID: "d", Path: "_firesync/abc"},
			wantTime:  ts1,
		},
		{
			name: "tombstone delete",
			event: &firestoredata.DocumentEventData{
				OldValue: doc(tombPath, nil, ts1),
			},
			eventTime: ts4,
			wantType:  EventTypeTombstone,
			wantName:  DocumentName{ProjectID: "p", DatabaseID: "d", Path: "_firesync/abc"},
			wantTime:  ts4,
		},
		{
			name:      "no value nor old",
			event:     &firestoredata.DocumentEventData{},
			eventTime: ts1,
			wantErr:   "no value nor old value",
		},
		{
			name: "invalid value name",
			event: &firestoredata.DocumentEventData{
				Value: doc("badname", nil, ts1),
			},
			eventTime: ts1,
			wantErr:   "invalid document name format",
		},
		{
			name: "invalid old name",
			event: &firestoredata.DocumentEventData{
				OldValue: doc("badname", nil, ts1),
			},
			eventTime: ts1,
			wantErr:   "invalid old document name format",
		},
		{
			name: "update no mask",
			event: &firestoredata.DocumentEventData{
				Value:    doc(basePath, nil, ts2),
				OldValue: doc(basePath, nil, ts1),
			},
			eventTime: ts1,
			wantErr:   "no update mask in update event",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseEvent(tt.event, tt.eventTime)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("ParseEvent error = %v, want %v", err, tt.wantErr)
				}
				if got != nil {
					t.Fatalf("ParseEvent got = %v, want nil", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseEvent error = %v", err)
			}
			if got.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", got.Type, tt.wantType)
			}
			if got.Name != tt.wantName {
				t.Errorf("Name = %v, want %v", got.Name, tt.wantName)
			}
			if !got.Timestamp.Equal(tt.wantTime) {
				t.Errorf("Timestamp = %v, want %v", got.Timestamp, tt.wantTime)
			}
		})
	}
}
