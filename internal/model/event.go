package model

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/googleapis/google-cloudevents-go/cloud/firestoredata"
)

type EventType uint

const (
	EventTypeUnknown EventType = iota
	EventTypeCreated
	EventTypeUpdated
	EventTypeDeleted
	EventTypeReplicated
	EventTypeTombstone
)

func (e EventType) String() string {
	switch e {
	case EventTypeUnknown:
		return "unknown"
	case EventTypeCreated:
		return "created"
	case EventTypeUpdated:
		return "updated"
	case EventTypeDeleted:
		return "deleted"
	case EventTypeReplicated:
		return "replicated"
	case EventTypeTombstone:
		return "tombstone"
	default:
		return fmt.Sprintf("unknown (%d)", e)
	}
}

type Event struct {
	Type      EventType
	Name      DocumentName
	Timestamp time.Time
	Data      *firestoredata.DocumentEventData
}

func ParseEvent(event *firestoredata.DocumentEventData, eventTime time.Time) (*Event, error) {
	doc := event.GetValue()
	if doc == nil {
		// If no (new) value, this is a delete event
		doc = event.GetOldValue()
		if doc == nil {
			return nil, errors.New("no value nor old value")
		}

		docName := NewDocumentFromPath(doc.Name)
		if docName == nil {
			return nil, errors.New("invalid old document name format")
		}

		// check if this is a tombstone
		if strings.HasPrefix(docName.Path, TombstoneCollection+"/") {
			return &Event{
				Type:      EventTypeTombstone,
				Name:      *docName,
				Timestamp: eventTime,
				Data:      event,
			}, nil
		}

		return &Event{
			Type:      EventTypeDeleted,
			Name:      *docName,
			Timestamp: eventTime,
			Data:      event,
		}, nil
	}

	docName := NewDocumentFromPath(doc.Name)
	if docName == nil {
		return nil, errors.New("invalid document name format")
	}

	// check if this is a tombstone
	if strings.HasPrefix(docName.Path, TombstoneCollection+"/") {
		return &Event{
			Type:      EventTypeTombstone,
			Name:      *docName,
			Timestamp: doc.UpdateTime.AsTime(),
			Data:      event,
		}, nil
	}

	if event.GetOldValue() == nil {
		// if no old value, this is a create event

		// check if this is a replicated create event
		if _, ok := doc.GetFields()["_firesync"]; ok {
			return &Event{
				Type:      EventTypeReplicated,
				Name:      *docName,
				Timestamp: doc.UpdateTime.AsTime(),
				Data:      event,
			}, nil
		}

		return &Event{
			Type:      EventTypeCreated,
			Name:      *docName,
			Timestamp: doc.UpdateTime.AsTime(),
			Data:      event,
		}, nil
	}

	// if both old and new values exist, this is an update event
	updateMask := event.GetUpdateMask()
	if updateMask == nil {
		return nil, errors.New("no update mask in update event")
	}

	// check if this is a replicated update event
	if hasAnyFiresyncFields(updateMask.GetFieldPaths()) {
		return &Event{
			Type:      EventTypeReplicated,
			Name:      *docName,
			Timestamp: doc.UpdateTime.AsTime(),
			Data:      event,
		}, nil
	}

	return &Event{
		Type:      EventTypeUpdated,
		Name:      *docName,
		Timestamp: doc.UpdateTime.AsTime(),
		Data:      event,
	}, nil
}

func hasAnyFiresyncFields(fieldNames []string) bool {
	for _, fieldName := range fieldNames {
		if fieldName == "_firesync" || strings.HasPrefix(fieldName, "_firesync.") {
			return true
		}
	}
	return false
}
