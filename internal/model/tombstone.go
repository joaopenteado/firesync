package model

import (
	"crypto/sha256"
	"encoding/base64"
	"strings"

	"cloud.google.com/go/firestore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	TombstoneCollection = "_firesync"
)

type Tombstone struct {
	// Document is a reference to the deleted document.
	Document *firestore.DocumentRef `json:"doc" firestore:"doc"`

	// Timestamp is the authoritative deletion timestamp used for LWW conflict
	// resolution.
	Timestamp *timestamppb.Timestamp `json:"ts" firestore:"ts"`

	// Source is the source database of the delete
	// (e.g. projects/$ID/databases/$DB)
	Source string `json:"src" firestore:"src"`

	// Trace is the top-level trace/span ID that issued the delete
	// Lets you follow the full causal chain in distributed-trace tools.
	// It is only set if the trace was sampled.
	Trace string `json:"trace,omitempty" firestore:"trace,omitempty"`

	// Expiration is the when the tombstone will be deleted by the TTL sweeper
	Expiration *timestamppb.Timestamp `json:"exp" firestore:"exp"`
}

func (t *Tombstone) ID() string {
	if t.Document == nil {
		panic("ID() called on nil tombstone")
	}

	const prefix = "/documents/"

	idx := strings.Index(t.Document.Path, prefix)
	if idx == -1 {
		panic("TombstoneID() called on tombstone with invalid path: " + t.Document.Path)
	}

	return TombstoneID(t.Document.Path[idx+len(prefix):])
}

// TombstoneID generates a unique ID for a tombstone document.
// Path is the raw path of the document, without the project or database prefixes.
// Example: users/123
func TombstoneID(path string) string {
	hash := sha256.Sum256([]byte(path))
	return base64.RawURLEncoding.EncodeToString(hash[:])
}
