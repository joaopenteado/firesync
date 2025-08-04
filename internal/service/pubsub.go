package service

import (
	"cloud.google.com/go/pubsub"
	"context"
)

// PublishResult represents the result of a Pub/Sub publish operation.
type PublishResult interface {
	Get(context.Context) (string, error)
}

// PubSubTopic abstracts a Pub/Sub topic.
type PubSubTopic interface {
	Publish(context.Context, *pubsub.Message) PublishResult
}

// NewPubSubTopicAdapter wraps a pubsub.Topic so it satisfies the PubSubTopic
// interface.
func NewPubSubTopicAdapter(t *pubsub.Topic) PubSubTopic {
	if t == nil {
		return nil
	}
	return &pubsubTopicAdapter{t}
}

type pubsubTopicAdapter struct{ *pubsub.Topic }

func (t *pubsubTopicAdapter) Publish(ctx context.Context, msg *pubsub.Message) PublishResult {
	return t.Topic.Publish(ctx, msg)
}
