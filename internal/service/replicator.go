package service

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

type replicationMetrics struct {
	ReplicationEventCount metric.Int64Counter
	ReplicationLatency    metric.Int64Histogram
}

func newReplicationMetrics(meter metric.Meter) replicationMetrics {
	ReplicationEventCount, err := meter.Int64Counter("firesync.replication.event_count",
		metric.WithDescription("The total number of changes replicated to the target database."),
		metric.WithUnit("1"),
	)
	if err != nil {
		log.Warn().Err(err).
			Str("metric", "firesync.replication.event_count").
			Msg("failed to create metric")
		ReplicationEventCount = noop.Int64Counter{}
	}

	ReplicationLatency, err := meter.Int64Histogram("firesync.replication.latency",
		metric.WithDescription("The latency of replicating changes to the target database, from the moment the change happened in the source database to the moment the change was replicated to the target database."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		log.Warn().Err(err).
			Str("metric", "firesync.replication.latency").
			Msg("failed to create metric")
		ReplicationLatency = noop.Int64Histogram{}
	}

	return replicationMetrics{
		ReplicationEventCount: ReplicationEventCount,
		ReplicationLatency:    ReplicationLatency,
	}
}

type replicator struct {
	metrics   replicationMetrics
	firestore *firestore.Client
}

type ReplicationResult uint8

const (
	ReplicationResultSuccess ReplicationResult = iota
	ReplicationResultSkipped
	ReplicationResultError
)

func (r ReplicationResult) String() string {
	switch r {
	case ReplicationResultSuccess:
		return "success"
	case ReplicationResultSkipped:
		return "skipped"
	case ReplicationResultError:
		return "error"
	default:
		return fmt.Sprintf("unknown (%d)", r)
	}
}

func NewReplicator(meter metric.Meter, firestore *firestore.Client) *replicator {
	return &replicator{
		metrics:   newReplicationMetrics(meter),
		firestore: firestore,
	}
}

func (svc *replicator) Replicate(ctx context.Context) (ReplicationResult, error) {
	return ReplicationResultSkipped, nil
}
