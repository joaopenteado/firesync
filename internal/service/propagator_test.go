package service

import (
	"testing"

	"github.com/googleapis/google-cloudevents-go/cloud/firestoredata"
)

func TestParseDocumentName(t *testing.T) {
	tests := []struct {
		name     string
		event    *firestoredata.DocumentEventData
		expected documentName
	}{
		// Valid cases with Value (new document)
		{
			name: "simple document path from value",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "projects/my-project/databases/my-db/documents/users/123",
				},
			},
			expected: documentName{
				ProjectID:    "my-project",
				DatabaseID:   "my-db",
				DocumentPath: "users/123",
				IsOld:        false,
			},
		},
		{
			name: "nested document path from value",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "projects/test-project/databases/firestore/documents/collections/docs/subcollections/subdocs/item",
				},
			},
			expected: documentName{
				ProjectID:    "test-project",
				DatabaseID:   "firestore",
				DocumentPath: "collections/docs/subcollections/subdocs/item",
				IsOld:        false,
			},
		},
		{
			name: "firesync internal path from value",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "projects/prod-env/databases/main/documents/_firesync/metadata/config",
				},
			},
			expected: documentName{
				ProjectID:    "prod-env",
				DatabaseID:   "main",
				DocumentPath: "_firesync/metadata/config",
				IsOld:        false,
			},
		},
		{
			name: "minimal valid document from value",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "projects/p/databases/d/documents/c",
				},
			},
			expected: documentName{
				ProjectID:    "p",
				DatabaseID:   "d",
				DocumentPath: "c",
				IsOld:        false,
			},
		},

		// Valid cases with OldValue (deleted document)
		{
			name: "simple document path from old value",
			event: &firestoredata.DocumentEventData{
				OldValue: &firestoredata.Document{
					Name: "projects/my-project/databases/my-db/documents/users/123",
				},
			},
			expected: documentName{
				ProjectID:    "my-project",
				DatabaseID:   "my-db",
				DocumentPath: "users/123",
				IsOld:        true,
			},
		},
		{
			name: "nested document path from old value",
			event: &firestoredata.DocumentEventData{
				OldValue: &firestoredata.Document{
					Name: "projects/test-project/databases/firestore/documents/collections/docs/subcollections/subdocs/item",
				},
			},
			expected: documentName{
				ProjectID:    "test-project",
				DatabaseID:   "firestore",
				DocumentPath: "collections/docs/subcollections/subdocs/item",
				IsOld:        true,
			},
		},

		// Value takes precedence over OldValue
		{
			name: "value takes precedence over old value",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "projects/new-project/databases/new-db/documents/new/doc",
				},
				OldValue: &firestoredata.Document{
					Name: "projects/old-project/databases/old-db/documents/old/doc",
				},
			},
			expected: documentName{
				ProjectID:    "new-project",
				DatabaseID:   "new-db",
				DocumentPath: "new/doc",
				IsOld:        false,
			},
		},

		// Invalid cases - should return empty documentName
		{
			name: "nil event",
			event: &firestoredata.DocumentEventData{
				Value:    nil,
				OldValue: nil,
			},
			expected: documentName{},
		},
		{
			name: "empty document name in value",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "",
				},
			},
			expected: documentName{},
		},
		{
			name: "empty document name in old value",
			event: &firestoredata.DocumentEventData{
				OldValue: &firestoredata.Document{
					Name: "",
				},
			},
			expected: documentName{},
		},
		{
			name: "too short document name",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "projects/p/databases/d/documents",
				},
			},
			expected: documentName{},
		},
		{
			name: "missing projects prefix",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "my-project/databases/my-db/documents/users/123",
				},
			},
			expected: documentName{},
		},
		{
			name: "wrong prefix",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "project/my-project/databases/my-db/documents/users/123",
				},
			},
			expected: documentName{},
		},
		{
			name: "missing databases section",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "projects/my-project/database/my-db/documents/users/123",
				},
			},
			expected: documentName{},
		},
		{
			name: "missing documents section",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "projects/my-project/databases/my-db/document/users/123",
				},
			},
			expected: documentName{},
		},
		{
			name: "empty project ID",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "projects//databases/my-db/documents/users/123",
				},
			},
			expected: documentName{},
		},
		{
			name: "empty database ID",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "projects/my-project/databases//documents/users/123",
				},
			},
			expected: documentName{},
		},
		{
			name: "empty document path",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "projects/my-project/databases/my-db/documents/",
				},
			},
			expected: documentName{},
		},
		{
			name: "malformed - multiple slashes in project",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "projects/my/project/databases/my-db/documents/users/123",
				},
			},
			expected: documentName{},
		},
		{
			name: "malformed - multiple slashes in database",
			event: &firestoredata.DocumentEventData{
				Value: &firestoredata.Document{
					Name: "projects/my-project/databases/my/db/documents/users/123",
				},
			},
			expected: documentName{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseDocumentName(tt.event)

			if result.ProjectID != tt.expected.ProjectID {
				t.Errorf("parseDocumentName() ProjectID = %q, want %q", result.ProjectID, tt.expected.ProjectID)
			}
			if result.DatabaseID != tt.expected.DatabaseID {
				t.Errorf("parseDocumentName() DatabaseID = %q, want %q", result.DatabaseID, tt.expected.DatabaseID)
			}
			if result.DocumentPath != tt.expected.DocumentPath {
				t.Errorf("parseDocumentName() DocumentPath = %q, want %q", result.DocumentPath, tt.expected.DocumentPath)
			}
			if result.IsOld != tt.expected.IsOld {
				t.Errorf("parseDocumentName() IsOld = %v, want %v", result.IsOld, tt.expected.IsOld)
			}
		})
	}
}

func TestDocumentNameString(t *testing.T) {
	tests := []struct {
		name     string
		docName  documentName
		expected string
	}{
		{
			name: "simple document name",
			docName: documentName{
				ProjectID:    "my-project",
				DatabaseID:   "my-db",
				DocumentPath: "users/123",
			},
			expected: "projects/my-project/databases/my-db/documents/users/123",
		},
		{
			name: "nested document name",
			docName: documentName{
				ProjectID:    "test-project",
				DatabaseID:   "firestore",
				DocumentPath: "collections/docs/subcollections/subdocs/item",
			},
			expected: "projects/test-project/databases/firestore/documents/collections/docs/subcollections/subdocs/item",
		},
		{
			name: "firesync internal path",
			docName: documentName{
				ProjectID:    "prod-env",
				DatabaseID:   "main",
				DocumentPath: "_firesync/metadata/config",
			},
			expected: "projects/prod-env/databases/main/documents/_firesync/metadata/config",
		},
		{
			name: "empty document name",
			docName: documentName{
				ProjectID:    "",
				DatabaseID:   "",
				DocumentPath: "",
			},
			expected: "projects//databases//documents/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.docName.String()
			if result != tt.expected {
				t.Errorf("documentName.String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestHasAnyFiresyncFields(t *testing.T) {
	tests := []struct {
		name       string
		fieldNames []string
		expected   bool
	}{
		{
			name:       "has _firesync field",
			fieldNames: []string{"name", "_firesync", "email"},
			expected:   true,
		},
		{
			name:       "has _firesync subfield",
			fieldNames: []string{"name", "_firesync.timestamp", "email"},
			expected:   true,
		},
		{
			name:       "has multiple _firesync subfields",
			fieldNames: []string{"_firesync.source", "_firesync.timestamp", "data"},
			expected:   true,
		},
		{
			name:       "no _firesync fields",
			fieldNames: []string{"name", "email", "age", "created_at"},
			expected:   false,
		},
		{
			name:       "empty field names",
			fieldNames: []string{},
			expected:   false,
		},
		{
			name:       "nil field names",
			fieldNames: nil,
			expected:   false,
		},
		{
			name:       "field starting with _firesync but not exact match",
			fieldNames: []string{"_firesynced", "_firesync_data", "name"},
			expected:   false,
		},
		{
			name:       "only _firesync field",
			fieldNames: []string{"_firesync"},
			expected:   true,
		},
		{
			name:       "only _firesync subfield",
			fieldNames: []string{"_firesync.metadata"},
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasAnyFiresyncFields(tt.fieldNames)
			if result != tt.expected {
				t.Errorf("hasAnyFiresyncFields(%v) = %v, want %v", tt.fieldNames, result, tt.expected)
			}
		})
	}
}

func BenchmarkParseDocumentName(t *testing.B) {
	testEvents := []*firestoredata.DocumentEventData{
		{
			Value: &firestoredata.Document{
				Name: "projects/my-project/databases/my-db/documents/users/123",
			},
		},
		{
			Value: &firestoredata.Document{
				Name: "projects/test-project/databases/firestore/documents/collections/docs/subcollections/subdocs/item",
			},
		},
		{
			Value: &firestoredata.Document{
				Name: "projects/prod-env/databases/main/documents/_firesync/metadata/config",
			},
		},
		{
			OldValue: &firestoredata.Document{
				Name: "projects/app/databases/prod/documents/orgs/org1/teams/team1/members/user1/permissions/read",
			},
		},
	}

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		for _, event := range testEvents {
			parseDocumentName(event)
		}
	}
}

func BenchmarkParseDocumentNameSingle(t *testing.B) {
	event := &firestoredata.DocumentEventData{
		Value: &firestoredata.Document{
			Name: "projects/my-project/databases/my-db/documents/users/123",
		},
	}

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		parseDocumentName(event)
	}
}

func BenchmarkHasAnyFiresyncFields(t *testing.B) {
	fieldNames := []string{"name", "email", "_firesync.timestamp", "age", "created_at", "_firesync.source"}

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		hasAnyFiresyncFields(fieldNames)
	}
}
