package events

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// This file pins the pure S3-notification parsing core for WR-007 (ADR-003:
// functional core / imperative shell). The functions under test are pure: they
// take the raw bytes a worker will eventually read off SQS and return domain
// events, with no I/O, no globals and no AWS SDK dependency (SQS polling is
// WR-016+). The real wire format is an SNS envelope whose "Message" field is a
// *double-encoded* JSON string carrying the standard AWS S3 event notification
// (DOCUMENTATION.md §3.1, ADR-001: Upload -> S3 -> SNS -> SQS -> workers).
//
// Design decisions this suite pins down for the implementer (Julia):
//
//   - parseS3Events(raw []byte) ([]Event, error) parses the SNS envelope, then
//     the inner (double-encoded) S3 payload, and returns ONE domain Event per
//     element of the inner Records array (S3 batches multiple records into one
//     notification). Order is preserved.
//   - Event is the exported domain type. It carries what a downstream worker
//     and WR-008 (idempotency key from bucket/key/version/etag) will need:
//     Bucket, Key, Size, ETag, VersionID, EventName, EventTime.
//   - Object keys are URL-decoded. S3 URL-encodes keys in event notifications
//     (spaces arrive as "+" or "%20"); the worker needs the literal key to GET
//     the object, so decoding is the parser's job, not the caller's.
//   - EventTime is parsed from the S3 eventTime (RFC3339) into a time.Time.
//   - SNS control messages (SubscriptionConfirmation / UnsubscribeConfirmation)
//     are RECOGNIZED-but-not-applicable: they carry no S3 payload, so the parser
//     returns the exported sentinel ErrNotNotification (checkable via
//     errors.Is), NOT a generic parse error. This lets the WR-016+ shell treat
//     "skip this, it's an SNS handshake" distinctly from "this is broken".
//   - Malformed input (truncated outer JSON; a Notification whose Message is
//     missing or is not valid JSON; a bare S3 payload with no SNS envelope /
//     unknown Type) returns a non-nil error that is NOT ErrNotNotification.
//   - A well-formed Notification whose inner payload parses but has no object
//     records (empty or missing Records, e.g. an "s3:TestEvent") returns zero
//     events and a nil error: extracting zero events is a valid outcome, not a
//     failure. The caller simply routes nothing.

const (
	watchedBucket = "uploads"
	eventTimeStr  = "2026-07-24T12:00:00.000Z"
)

// mustTime parses an RFC3339 timestamp for use in expected values, using the
// same code path the implementation is expected to use, so time zones/precision
// line up for comparison.
func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("mustTime(%q): %v", s, err)
	}
	return ts
}

// snsNotification wraps an inner S3-notification JSON string in a realistic SNS
// "Notification" envelope. Marshaling a struct whose Message field is the inner
// JSON *string* reproduces the real double-encoding (the inner braces/quotes
// are escaped inside a JSON string value) exactly as it arrives off SQS — no
// shortcut that skips the double-encode.
func snsNotification(t *testing.T, innerS3JSON string) []byte {
	t.Helper()
	env := struct {
		Type      string `json:"Type"`
		MessageId string `json:"MessageId"`
		TopicArn  string `json:"TopicArn"`
		Message   string `json:"Message"`
		Timestamp string `json:"Timestamp"`
	}{
		Type:      "Notification",
		MessageId: "22b81b1a-0000-0000-0000-000000000000",
		TopicArn:  "arn:aws:sns:us-east-2:000000000000:weir-uploads",
		Message:   innerS3JSON,
		Timestamp: eventTimeStr,
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("snsNotification marshal: %v", err)
	}
	return b
}

// s3Record renders a single S3 event-notification record as JSON.
func s3Record(eventName, bucket, key string, size int64, etag, versionID string) string {
	rec := map[string]any{
		"eventVersion": "2.1",
		"eventSource":  "aws:s3",
		"awsRegion":    "us-east-2",
		"eventTime":    eventTimeStr,
		"eventName":    eventName,
		"s3": map[string]any{
			"bucket": map[string]any{"name": bucket},
			"object": map[string]any{
				"key":       key,
				"size":      size,
				"eTag":      etag,
				"versionId": versionID,
			},
		},
	}
	b, _ := json.Marshal(rec)
	return string(b)
}

// s3Notification renders an inner S3 payload with the given records.
func s3Notification(records ...string) string {
	inner := `{"Records":[`
	for i, r := range records {
		if i > 0 {
			inner += ","
		}
		inner += r
	}
	inner += `]}`
	return inner
}

func assertEventEqual(t *testing.T, idx int, got, want Event) {
	t.Helper()
	if got.Bucket != want.Bucket {
		t.Errorf("event[%d].Bucket = %q, want %q", idx, got.Bucket, want.Bucket)
	}
	if got.Key != want.Key {
		t.Errorf("event[%d].Key = %q, want %q", idx, got.Key, want.Key)
	}
	if got.Size != want.Size {
		t.Errorf("event[%d].Size = %d, want %d", idx, got.Size, want.Size)
	}
	if got.ETag != want.ETag {
		t.Errorf("event[%d].ETag = %q, want %q", idx, got.ETag, want.ETag)
	}
	if got.VersionID != want.VersionID {
		t.Errorf("event[%d].VersionID = %q, want %q", idx, got.VersionID, want.VersionID)
	}
	if got.EventName != want.EventName {
		t.Errorf("event[%d].EventName = %q, want %q", idx, got.EventName, want.EventName)
	}
	if !got.EventTime.Equal(want.EventTime) {
		t.Errorf("event[%d].EventTime = %s, want %s", idx, got.EventTime, want.EventTime)
	}
}

// TestParseS3Events_Success covers the happy paths: single record, field
// extraction (including VersionID for WR-008), URL-decoded keys, multi-record
// batch splitting, and the well-formed-but-no-records case.
func TestParseS3Events_Success(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		want []Event
	}{
		{
			name: "single ObjectCreated:Put record extracts all fields",
			raw: snsNotification(t, s3Notification(
				s3Record("ObjectCreated:Put", watchedBucket, "raw/video1.mp4", 123456, "abc123", "v42"),
			)),
			want: []Event{
				{
					Bucket:    "uploads",
					Key:       "raw/video1.mp4",
					Size:      123456,
					ETag:      "abc123",
					VersionID: "v42",
					EventName: "ObjectCreated:Put",
					EventTime: mustTime(t, eventTimeStr),
				},
			},
		},
		{
			name: "object key is URL-decoded (plus and percent-encoding become spaces)",
			raw: snsNotification(t, s3Notification(
				s3Record("ObjectCreated:Put", watchedBucket, "raw/my+file%20name.mp4", 10, "e1", ""),
			)),
			want: []Event{
				{
					Bucket:    "uploads",
					Key:       "raw/my file name.mp4",
					Size:      10,
					ETag:      "e1",
					VersionID: "",
					EventName: "ObjectCreated:Put",
					EventTime: mustTime(t, eventTimeStr),
				},
			},
		},
		{
			name: "multi-record batch splits into one event per record, order preserved",
			raw: snsNotification(t, s3Notification(
				s3Record("ObjectCreated:Put", watchedBucket, "raw/a.mp4", 1, "ta", "va"),
				s3Record("ObjectCreated:Post", watchedBucket, "raw/b.mp4", 2, "tb", "vb"),
			)),
			want: []Event{
				{Bucket: "uploads", Key: "raw/a.mp4", Size: 1, ETag: "ta", VersionID: "va", EventName: "ObjectCreated:Put", EventTime: mustTime(t, eventTimeStr)},
				{Bucket: "uploads", Key: "raw/b.mp4", Size: 2, ETag: "tb", VersionID: "vb", EventName: "ObjectCreated:Post", EventTime: mustTime(t, eventTimeStr)},
			},
		},
		{
			name: "well-formed notification with empty Records yields zero events, no error",
			raw:  snsNotification(t, `{"Records":[]}`),
			want: []Event{},
		},
		{
			name: "notification whose inner payload has no Records field yields zero events (e.g. s3:TestEvent)",
			raw:  snsNotification(t, `{"Service":"Amazon S3","Event":"s3:TestEvent","Bucket":"uploads","Time":"2026-07-24T12:00:00.000Z"}`),
			want: []Event{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseS3Events(tc.raw)
			if err != nil {
				t.Fatalf("parseS3Events returned unexpected error: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("parseS3Events returned %d events, want %d: %+v", len(got), len(tc.want), got)
			}
			for i := range tc.want {
				assertEventEqual(t, i, got[i], tc.want[i])
			}
		})
	}
}

// TestParseS3Events_SubscriptionConfirmation pins the recognized-but-skippable
// contract: SNS handshake messages return the ErrNotNotification sentinel, not
// a generic error, and no events. These carry no S3 payload; a naive parser
// crashes trying to unmarshal a missing Message as S3 JSON.
func TestParseS3Events_SubscriptionConfirmation(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
	}{
		{
			name: "SubscriptionConfirmation is recognized and skipped via ErrNotNotification",
			raw: []byte(`{
				"Type": "SubscriptionConfirmation",
				"Token": "2336412f37...",
				"TopicArn": "arn:aws:sns:us-east-2:000000000000:weir-uploads",
				"Message": "You have chosen to subscribe to the topic ...",
				"SubscribeURL": "https://sns.us-east-2.amazonaws.com/?Action=ConfirmSubscription",
				"Timestamp": "2026-07-24T12:00:00.000Z"
			}`),
		},
		{
			name: "UnsubscribeConfirmation is recognized and skipped via ErrNotNotification",
			raw: []byte(`{
				"Type": "UnsubscribeConfirmation",
				"Token": "2336412f37...",
				"TopicArn": "arn:aws:sns:us-east-2:000000000000:weir-uploads",
				"SubscribeURL": "https://sns.us-east-2.amazonaws.com/?Action=ConfirmSubscription",
				"Timestamp": "2026-07-24T12:00:00.000Z"
			}`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseS3Events(tc.raw)
			if !errors.Is(err, ErrNotNotification) {
				t.Fatalf("parseS3Events error = %v, want errors.Is(..., ErrNotNotification)", err)
			}
			if len(got) != 0 {
				t.Errorf("parseS3Events returned %d events, want 0 on a non-notification", len(got))
			}
		})
	}
}

// TestParseS3Events_Malformed covers unrecognized/broken input. Each must be a
// non-nil error that is explicitly NOT the ErrNotNotification skip sentinel, so
// the shell can distinguish "broken" from "recognized handshake".
func TestParseS3Events_Malformed(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
	}{
		{
			name: "truncated outer SNS JSON",
			raw:  []byte(`{"Type":"Notification","Message":"{\"Records\":[`),
		},
		{
			name: "empty input",
			raw:  []byte(``),
		},
		{
			name: "notification with missing Message field",
			raw:  []byte(`{"Type":"Notification","TopicArn":"arn:aws:sns:...","Timestamp":"2026-07-24T12:00:00.000Z"}`),
		},
		{
			name: "notification whose Message is not valid JSON",
			raw:  snsNotification(t, `this is not json {{{`),
		},
		{
			name: "unknown SNS Type",
			raw:  []byte(`{"Type":"SomethingElse","Message":"{}"}`),
		},
		{
			name: "missing Type (not an SNS envelope at all)",
			raw:  []byte(`{"foo":"bar"}`),
		},
		{
			name: "bare S3 payload with no SNS envelope (wire format is always SNS-wrapped)",
			raw:  []byte(s3Notification(s3Record("ObjectCreated:Put", watchedBucket, "raw/x.mp4", 1, "t", ""))),
		},
		{
			// s3Record hardcodes a valid eventTime, so this one record is built
			// inline to inject a non-RFC3339 eventTime and exercise the
			// time.Parse error branch.
			name: "record with non-RFC3339 eventTime",
			raw: snsNotification(t, s3Notification(
				`{"eventName":"ObjectCreated:Put","eventTime":"not-a-time","s3":{"bucket":{"name":"uploads"},"object":{"key":"raw/x.mp4","size":1,"eTag":"t","versionId":""}}}`,
			)),
		},
		{
			// "%zz" is a "%" not followed by two hex digits: valid JSON, but
			// invalid percent-encoding, so url.QueryUnescape fails.
			name: "object key with invalid percent-encoding",
			raw: snsNotification(t, s3Notification(
				s3Record("ObjectCreated:Put", watchedBucket, "raw/%zz", 1, "t", ""),
			)),
		},
		{
			// A well-formed SNS Notification whose inner payload is arbitrary
			// JSON: it has no "Records" key AND is not the documented
			// s3:TestEvent shape (no Event:"s3:TestEvent" marker). Unlike the
			// legitimate empty-batch ({"Records":[]}) and s3:TestEvent cases —
			// which correctly yield zero events, nil error — genuine garbage
			// must NOT be silently swallowed as a successful zero-event parse,
			// or the shell (WR-016+) would ack/delete a misrouted or malformed
			// SQS message and hide the fault. It must be a distinct error, not
			// the ErrNotNotification skip sentinel.
			name: "arbitrary JSON object with no Records key and no s3:TestEvent marker is an error, not a silent success",
			raw:  snsNotification(t, `{"garbage":true}`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseS3Events(tc.raw)
			if err == nil {
				t.Fatalf("parseS3Events(%s) = nil error, want a parse error", tc.name)
			}
			if errors.Is(err, ErrNotNotification) {
				t.Fatalf("parseS3Events(%s) returned the skip sentinel; malformed input must be a distinct error", tc.name)
			}
		})
	}
}
