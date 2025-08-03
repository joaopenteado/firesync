package simulator

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type Topic struct {
	client        *pubsub.Client
	topic         *pubsub.Topic
	subscriptions []*pubsub.Subscription
}

type Option func(ctx context.Context, topic *Topic) error

func WithPushSubscription(pushURL string) Option {
	return func(ctx context.Context, t *Topic) error {
		id := generateRandomNameID("sub")
		log.Debug().Str("subscription_id", id).Str("url", pushURL).Msg("creating pubsub push subscription")
		subscription, err := t.client.CreateSubscription(ctx, id, pubsub.SubscriptionConfig{
			Topic: t.topic,
			PushConfig: pubsub.PushConfig{
				Endpoint: pushURL,
				Wrapper:  &pubsub.NoWrapper{WriteMetadata: true},
			},
		})
		if err != nil {
			return err
		}
		t.subscriptions = append(t.subscriptions, subscription)
		return nil
	}
}

func NewPubSub(ctx context.Context, client *pubsub.Client, topicID string, opts ...Option) (*Topic, error) {
	if topicID == "" {
		topicID = generateRandomNameID("topic")
	}

	topic := &Topic{
		client: client,
		topic:  client.Topic(topicID),
	}

	log.Debug().Str("topic_id", topicID).Msg("initializing pubsub topic")
	exists, err := topic.topic.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		log.Debug().Str("topic_id", topicID).Msg("topic does not exist, creating")
		topic.topic, err = topic.client.CreateTopic(ctx, topicID)
		if err != nil {
			return nil, err
		}
		log.Debug().Str("topic_id", topicID).Msg("pubsub topic created")
	}

	log.Debug().Str("topic_id", topicID).Msg("initializing pubsub subscriptions")
	for _, opt := range opts {
		if err := opt(ctx, topic); err != nil {
			return nil, err
		}
	}
	return topic, nil
}

func (t *Topic) Close(ctx context.Context) error {
	var retErr error
	log.Debug().Str("topic_id", t.topic.ID()).Msg("deleting pubsub topic")
	for _, subscription := range t.subscriptions {
		log.Debug().Str("subscription_id", subscription.ID()).Msg("deleting pubsub subscription")
		if err := subscription.Delete(ctx); err != nil {
			log.Err(err).Str("subscription_id", subscription.ID()).Msg("failed to delete pubsub subscription")
			retErr = errors.Join(retErr, err)
		}
	}
	log.Debug().Str("topic_id", t.topic.ID()).Msg("deleting pubsub topic")
	if err := t.topic.Delete(ctx); err != nil {
		log.Err(err).Str("topic_id", t.topic.ID()).Msg("failed to delete pubsub topic")
		retErr = errors.Join(retErr, err)
	}
	return retErr
}

func generateRandomNameID(prefix string) string {
	id := uuid.New().String()
	id = strings.ReplaceAll(id, "-", "")
	return fmt.Sprintf("%s-%s", prefix, id)
}
