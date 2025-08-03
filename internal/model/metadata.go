package model

import "google.golang.org/protobuf/types/known/timestamppb"

type Metadata struct {
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
}
