package simulator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/googleapis/google-cloudevents-go/cloud/firestoredata"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Watcher struct {
	projectID    string
	databaseID   string
	collection   *firestore.CollectionRef
	topic        *pubsub.Topic
	subscription *pubsub.Subscription
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

func WatchFirestoreCollection(ctx context.Context, pubsubClient *pubsub.Client, projectID, databaseID string, collection *firestore.CollectionRef, propagateURL string) (*Watcher, error) {
	topicID := generateRandomNameID("eventarc")
	topic, err := pubsubClient.CreateTopic(ctx, topicID)
	if err != nil {
		// try to get topic if it already exists
		topic = pubsubClient.Topic(topicID)
		if _, err := topic.Exists(ctx); err != nil {
			return nil, fmt.Errorf("failed to create or get pubsub topic: %w", err)
		}
	}

	subID := generateRandomNameID("sub")
	subscription, err := pubsubClient.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic: topic,
		PushConfig: pubsub.PushConfig{
			Endpoint: propagateURL,
			Wrapper:  &pubsub.NoWrapper{WriteMetadata: true},
		},
	})
	if err != nil {
		// try to get subscription if it already exists
		subscription = pubsubClient.Subscription(subID)
		if _, err := subscription.Exists(ctx); err != nil {
			return nil, fmt.Errorf("failed to create or get pubsub subscription: %w", err)
		}
	}

	return &Watcher{
		projectID:    projectID,
		databaseID:   databaseID,
		collection:   collection,
		topic:        topic,
		subscription: subscription,
	}, nil
}

func (w *Watcher) Close(ctx context.Context) error {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()

	if err := w.subscription.Delete(ctx); err != nil {
		log.Warn().Err(err).Msg("failed to delete subscription")
	}
	if err := w.topic.Delete(ctx); err != nil {
		log.Warn().Err(err).Msg("failed to delete topic")
	}
	return nil
}

func (w *Watcher) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.watch(ctx)
	}()
	return nil
}

func (w *Watcher) watch(ctx context.Context) {
	log.Debug().Str("collection", w.collection.Path).Msg("watching firestore collection")
	it := w.collection.Snapshots(ctx)
	defer it.Stop()

	for {
		snap, err := it.Next()
		if err == iterator.Done {
			log.Debug().Msg("firestore snapshot iterator done")
			break
		}
		if err != nil {
			log.Error().Err(err).Msg("firestore snapshot error")
			return
		}

		for _, change := range snap.Changes {
			if err := w.publishChange(ctx, change, snap.ReadTime); err != nil {
				log.Error().Err(err).Msg("failed to publish change")
			}
		}
	}
}

func (w *Watcher) publishChange(ctx context.Context, change firestore.DocumentChange, eventTime time.Time) error {
	var err error

	docPath := ""
	if change.Doc != nil {
		docPath = change.Doc.Ref.Path
	}

	eventData := firestoredata.DocumentEventData{}

	if change.Kind == firestore.DocumentAdded || change.Kind == firestore.DocumentModified {
		eventData.Value = &firestoredata.Document{
			Name:       docPath,
			CreateTime: timestamppb.New(change.Doc.CreateTime),
			UpdateTime: timestamppb.New(change.Doc.UpdateTime),
		}
		eventData.Value.Fields, err = toFirestoreValueMap(change.Doc.Data())
		if err != nil {
			return fmt.Errorf("failed to convert new data to proto: %w", err)
		}
	}

	if change.Kind == firestore.DocumentModified || change.Kind == firestore.DocumentRemoved {
		eventData.OldValue = &firestoredata.Document{
			Name: docPath,
		}
		// For DocumentRemoved, we don't have update times, but we can get create time.
		if change.Kind == firestore.DocumentRemoved {
			// This is a bit of a hack, the old value is not available in the snapshot for removed documents
			eventData.OldValue.CreateTime = timestamppb.New(change.Doc.CreateTime)
		}
	}

	payload, err := protojson.Marshal(&eventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	log.Debug().Msg("publishing firestore change to pubsub")

	var shortPath string
	if idx := strings.Index(change.Doc.Ref.Path, "/documents/"); idx != -1 {
		shortPath = change.Doc.Ref.Path[idx+1:]
	}

	res := w.topic.Publish(ctx, &pubsub.Message{
		Data: payload,
		Attributes: map[string]string{
			"content-type":       "application/json",
			"ce-database":        w.databaseID,
			"ce-datacontenttype": "application/json",
			"ce-dataschema":      "https://github.com/googleapis/google-cloudevents/blob/main/proto/google/events/cloud/firestore/v1/data.proto",
			"ce-document":        shortPath,
			"ce-id":              uuid.NewString(),
			"ce-location":        "local",
			"ce-namespace":       w.databaseID,
			"ce-project":         w.projectID,
			"ce-source":          fmt.Sprintf("//firestore.googleapis.com/projects/%s/databases/%s/documents/%s", w.projectID, w.databaseID, change.Doc.Ref.Path),
			"ce-specversion":     "1.0",
			"ce-subject":         "documents/" + shortPath,
			"ce-time":            eventTime.Format(time.RFC3339Nano),
			"ce-type":            getCloudEventType(change.Kind),
		},
	})

	_, err = res.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Debug().
		Str("document_id", change.Doc.Ref.ID).
		Str("kind", changeKindString(change.Kind)).
		Msg("published firestore change to pubsub")

	return nil
}

func changeKindString(kind firestore.DocumentChangeKind) string {
	switch kind {
	case firestore.DocumentAdded:
		return "DocumentAdded"
	case firestore.DocumentModified:
		return "DocumentModified"
	case firestore.DocumentRemoved:
		return "DocumentRemoved"
	default:
		return "Unknown"
	}
}

func getCloudEventType(kind firestore.DocumentChangeKind) string {
	switch kind {
	case firestore.DocumentAdded:
		return "google.cloud.firestore.document.v1.created"
	case firestore.DocumentModified:
		return "google.cloud.firestore.document.v1.updated"
	case firestore.DocumentRemoved:
		return "google.cloud.firestore.document.v1.deleted"
	default:
		return "google.cloud.firestore.document.v1.written"
	}
}

func toFirestoreValueMap(data map[string]interface{}) (map[string]*firestoredata.Value, error) {
	fields := make(map[string]*firestoredata.Value)
	for k, v := range data {
		val, err := toFirestoreValue(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %q: %w", k, err)
		}
		fields[k] = val
	}
	return fields, nil
}

func toFirestoreValue(v interface{}) (*firestoredata.Value, error) {
	switch val := v.(type) {
	case nil:
		return &firestoredata.Value{ValueType: &firestoredata.Value_NullValue{}}, nil
	case bool:
		return &firestoredata.Value{ValueType: &firestoredata.Value_BooleanValue{BooleanValue: val}}, nil
	case int64:
		return &firestoredata.Value{ValueType: &firestoredata.Value_IntegerValue{IntegerValue: val}}, nil
	case float64:
		return &firestoredata.Value{ValueType: &firestoredata.Value_DoubleValue{DoubleValue: val}}, nil
	case string:
		return &firestoredata.Value{ValueType: &firestoredata.Value_StringValue{StringValue: val}}, nil
	case []byte:
		return &firestoredata.Value{ValueType: &firestoredata.Value_BytesValue{BytesValue: val}}, nil
	case time.Time:
		return &firestoredata.Value{ValueType: &firestoredata.Value_TimestampValue{TimestampValue: timestamppb.New(val)}}, nil
	case map[string]interface{}:
		subFields, err := toFirestoreValueMap(val)
		if err != nil {
			return nil, err
		}
		return &firestoredata.Value{ValueType: &firestoredata.Value_MapValue{MapValue: &firestoredata.MapValue{Fields: subFields}}}, nil
	case []interface{}:
		var values []*firestoredata.Value
		for _, item := range val {
			fv, err := toFirestoreValue(item)
			if err != nil {
				return nil, err
			}
			values = append(values, fv)
		}
		return &firestoredata.Value{ValueType: &firestoredata.Value_ArrayValue{ArrayValue: &firestoredata.ArrayValue{Values: values}}}, nil
	default:
		// Other types like GeoPoint, DocumentRef are not handled here for simplicity.
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
}
