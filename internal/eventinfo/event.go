package eventinfo

import "github.com/googleapis/google-cloudevents-go/cloud/firestoredata"

type EventType int

const (
	EventTypeCreated EventType = iota
	EventTypeUpdated
	EventTypeDeleted
)

func (e EventType) String() string {
	switch e {
	case EventTypeCreated:
		return "created"
	case EventTypeUpdated:
		return "updated"
	case EventTypeDeleted:
		return "deleted"
	}

	return "unknown"
}

func GetEventType(event *firestoredata.DocumentEventData) EventType {
	if event.Value == nil {
		return EventTypeDeleted
	}

	if event.OldValue == nil {
		return EventTypeCreated
	}

	return EventTypeUpdated
}
