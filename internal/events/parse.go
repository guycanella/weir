// Package events holds the pure decision core for S3 event notification
// parsing and routing (ADR-003: functional core / imperative shell). These
// functions take raw bytes/domain values and return domain values; SQS
// polling and delivery live in the WR-016+ shell.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"
)

// Event is the domain representation of a single S3 object-creation
// notification, extracted from an SNS-wrapped S3 event record.
type Event struct {
	Bucket    string
	Key       string
	Size      int64
	ETag      string
	VersionID string
	EventName string
	EventTime time.Time
}

// ErrNotNotification is returned when the SNS envelope's Type is not
// "Notification" (e.g. a SubscriptionConfirmation handshake). It is a
// recognized, non-error outcome distinct from malformed input, so callers can
// tell "skip this" from "this is broken" via errors.Is.
var ErrNotNotification = errors.New("events: SNS message is not a Notification")

// snsEnvelope is the outer SNS message. Only the fields the parser needs are
// modeled; the real envelope carries more (MessageId, TopicArn, Timestamp...).
type snsEnvelope struct {
	Type    string `json:"Type"`
	Message string `json:"Message"`
}

// s3EventRecord is a single record within an s3EventPayload's Records array.
type s3EventRecord struct {
	EventName string `json:"eventName"`
	EventTime string `json:"eventTime"`
	S3        struct {
		Bucket struct {
			Name string `json:"name"`
		} `json:"bucket"`
		Object struct {
			Key       string `json:"key"`
			Size      int64  `json:"size"`
			ETag      string `json:"eTag"`
			VersionID string `json:"versionId"`
		} `json:"object"`
	} `json:"s3"`
}

// s3EventPayload is the inner (double-encoded) S3 event-notification payload
// carried in the SNS envelope's Message field.
//
// Records is a pointer to a slice, not a plain slice, so the parser can tell
// "the Records key was present in the JSON, even as an empty array" apart
// from "the Records key was entirely absent" — encoding/json unmarshals both
// cases to the same zero value for a plain slice field, but a *[]T is nil
// only in the latter case. That distinction is what separates a legitimate
// empty batch ({"Records":[]}) or the documented s3:TestEvent shape (no
// Records key at all) from genuine unrecognized/garbage payloads.
type s3EventPayload struct {
	Records *[]s3EventRecord `json:"Records"`
	Event   string           `json:"Event"`
}

// s3TestEvent is the "Event" marker AWS documents for the S3 test
// notification sent when an S3 event-notification configuration is first
// created. It carries no Records key at all.
const s3TestEvent = "s3:TestEvent"

// parseS3Events parses raw bytes off the queue into domain events. The wire
// format is always SNS-wrapped (S3 -> SNS -> SQS, ADR-001): the SNS envelope
// check happens before touching the inner payload so a bare S3 event or an
// SNS handshake never gets misread as a parse failure.
func parseS3Events(raw []byte) ([]Event, error) {
	var env snsEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("events: unmarshal SNS envelope: %w", err)
	}

	switch env.Type {
	case "Notification":
		// falls through to payload parsing below
	case "SubscriptionConfirmation", "UnsubscribeConfirmation":
		// Recognized SNS handshake messages carry no S3 payload: skip, don't fail.
		return nil, fmt.Errorf("events: SNS Type %q: %w", env.Type, ErrNotNotification)
	default:
		return nil, fmt.Errorf("events: unrecognized SNS Type %q", env.Type)
	}

	if env.Message == "" {
		return nil, errors.New("events: Notification has empty Message")
	}

	var inner s3EventPayload
	if err := json.Unmarshal([]byte(env.Message), &inner); err != nil {
		return nil, fmt.Errorf("events: unmarshal S3 payload: %w", err)
	}

	if inner.Records == nil {
		// The Records key was entirely absent from the JSON (as opposed to
		// present-but-empty). The only documented shape for that is the S3
		// test-event notification sent when an event-notification config is
		// first created; anything else with no Records key is an
		// unrecognized/garbage payload, not a legitimate zero-event outcome.
		if inner.Event == s3TestEvent {
			return nil, nil
		}
		return nil, fmt.Errorf("events: S3 payload has no Records field and is not %q", s3TestEvent)
	}

	records := *inner.Records
	out := make([]Event, 0, len(records))
	for _, r := range records {
		eventTime, err := time.Parse(time.RFC3339, r.EventTime)
		if err != nil {
			return nil, fmt.Errorf("events: parse eventTime %q: %w", r.EventTime, err)
		}

		// S3 URL-encodes object keys in event notifications (spaces arrive as
		// "+" or "%20"); QueryUnescape decodes both forms to a literal space,
		// which PathUnescape does not do for "+".
		key, err := url.QueryUnescape(r.S3.Object.Key)
		if err != nil {
			return nil, fmt.Errorf("events: decode key %q: %w", r.S3.Object.Key, err)
		}

		// encoding/json zero-fills omitted fields, so a record missing an
		// identity field or carrying a nonsensical size must be rejected
		// explicitly rather than silently becoming a hollow Event.
		if r.EventName == "" {
			return nil, errors.New("events: record missing eventName")
		}
		if r.S3.Bucket.Name == "" {
			return nil, errors.New("events: record missing bucket name")
		}
		if key == "" {
			return nil, errors.New("events: record missing object key")
		}
		if r.S3.Object.Size < 0 {
			return nil, fmt.Errorf("events: record has negative object size %d", r.S3.Object.Size)
		}

		out = append(out, Event{
			Bucket:    r.S3.Bucket.Name,
			Key:       key,
			Size:      r.S3.Object.Size,
			ETag:      r.S3.Object.ETag,
			VersionID: r.S3.Object.VersionID,
			EventName: r.EventName,
			EventTime: eventTime,
		})
	}

	return out, nil
}
