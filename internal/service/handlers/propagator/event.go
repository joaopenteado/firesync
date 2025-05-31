package propagator

import "github.com/googleapis/google-cloudevents-go/cloud/firestoredata"

type eventType int

const (
	eventTypeCreate eventType = iota
	eventTypeUpdate
	eventTypeDelete
)

func getEventType(event *firestoredata.DocumentEventData) eventType {
	if event.Value == nil {
		return eventTypeDelete
	}

	if event.OldValue == nil {
		return eventTypeCreate
	}

	return eventTypeUpdate
}
