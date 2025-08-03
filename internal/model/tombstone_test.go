package model

import (
	"crypto/sha256"
	"encoding/base64"
	"testing"

	"cloud.google.com/go/firestore"
)

func TestTombstoneID(t *testing.T) {
	path := "users/123"
	got := TombstoneID(path)
	hash := sha256.Sum256([]byte(path))
	want := base64.RawURLEncoding.EncodeToString(hash[:])
	if got != want {
		t.Fatalf("TombstoneID(%q) = %q, want %q", path, got, want)
	}
}

func TestTombstone_ID(t *testing.T) {
	docPath := "projects/p/databases/d/documents/users/123"
	ts := &Tombstone{Document: &firestore.DocumentRef{Path: docPath}}
	got := ts.ID()
	want := TombstoneID("users/123")
	if got != want {
		t.Errorf("Tombstone.ID() = %q, want %q", got, want)
	}
}

func TestTombstone_ID_Panics(t *testing.T) {
	tests := []struct {
		name string
		t    Tombstone
	}{
		{"nil document", Tombstone{}},
		{"invalid path", Tombstone{Document: &firestore.DocumentRef{Path: "projects/p/databases/d/invalid"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic")
				}
			}()
			tt.t.ID()
		})
	}
}
