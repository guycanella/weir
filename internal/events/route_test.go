package events

import (
	"testing"
	"time"
)

// This file pins the pure routing decision for WR-007: given a parsed domain
// Event and the pipeline's watch configuration (bucket + prefix, from the CR's
// spec.source in DOCUMENTATION.md §3.4), decide whether this pipeline cares
// about the event.
//
// Design decisions this suite pins down for the implementer (Julia):
//
//   - matches(evt Event, cfg WatchConfig) bool is the single routing gate. It
//     returns true iff ALL of:
//       (a) evt.Bucket == cfg.Bucket        (exact, case-sensitive),
//       (b) evt.Key has the prefix cfg.Prefix (literal string prefix), and
//       (c) evt.EventName denotes an object creation (ObjectCreated:*).
//   - Event-name filtering lives INSIDE the routing decision, not in the
//     parser. Rationale: routing answers "does this pipeline care about this
//     event?", and a delete event is something an upload->process pipeline does
//     not care about, exactly like a wrong-bucket event. The DOCUMENTATION.md
//     flow is "Upload -> S3 -> ... -> workers", so only creation events are in
//     scope; ObjectRemoved:* and other event types are routed out. Keeping it
//     in one predicate gives the WR-016+ caller a single yes/no gate.
//   - Prefix matching is a LITERAL string prefix, matching S3's own semantics
//     (S3 prefixes are not path-segment aware). So prefix "raw" matches key
//     "rawdata/x". Users who want a directory boundary include the trailing
//     slash ("raw/"). This is pinned deliberately so the behavior is faithful
//     to how S3 notification filters actually work.
//   - An empty prefix watches the whole bucket (every key matches).
//   - Bucket and key comparisons are case-sensitive (S3 bucket/key identity).

func evt(bucket, key, eventName string) Event {
	return Event{
		Bucket:    bucket,
		Key:       key,
		EventName: eventName,
		EventTime: time.Unix(0, 0).UTC(),
	}
}

func TestMatches(t *testing.T) {
	// The canonical watch config from the example CR: bucket "uploads", prefix
	// "raw/" (DOCUMENTATION.md §3.4).
	docCfg := WatchConfig{Bucket: "uploads", Prefix: "raw/"}

	cases := []struct {
		name string
		evt  Event
		cfg  WatchConfig
		want bool
	}{
		// --- in scope ---
		{
			name: "matching bucket, matching prefix, ObjectCreated:Put",
			evt:  evt("uploads", "raw/video1.mp4", "ObjectCreated:Put"),
			cfg:  docCfg,
			want: true,
		},
		{
			name: "ObjectCreated:Post is in scope",
			evt:  evt("uploads", "raw/form-upload.bin", "ObjectCreated:Post"),
			cfg:  docCfg,
			want: true,
		},
		{
			name: "ObjectCreated:CompleteMultipartUpload is in scope",
			evt:  evt("uploads", "raw/big.mp4", "ObjectCreated:CompleteMultipartUpload"),
			cfg:  docCfg,
			want: true,
		},
		{
			name: "key exactly equal to prefix still matches (prefix is inclusive)",
			evt:  evt("uploads", "raw/", "ObjectCreated:Put"),
			cfg:  docCfg,
			want: true,
		},
		{
			name: "empty prefix watches the whole bucket",
			evt:  evt("uploads", "anything/at/all.txt", "ObjectCreated:Put"),
			cfg:  WatchConfig{Bucket: "uploads", Prefix: ""},
			want: true,
		},
		{
			name: "literal (non-path-segment) prefix: 'raw' matches 'rawdata/x'",
			evt:  evt("uploads", "rawdata/x.mp4", "ObjectCreated:Put"),
			cfg:  WatchConfig{Bucket: "uploads", Prefix: "raw"},
			want: true,
		},

		// --- out of scope: wrong bucket ---
		{
			name: "different bucket is out of scope even with matching prefix and event",
			evt:  evt("other-bucket", "raw/video1.mp4", "ObjectCreated:Put"),
			cfg:  docCfg,
			want: false,
		},
		{
			name: "bucket comparison is case-sensitive",
			evt:  evt("Uploads", "raw/video1.mp4", "ObjectCreated:Put"),
			cfg:  docCfg,
			want: false,
		},

		// --- out of scope: wrong prefix ---
		{
			name: "right bucket but key under a different prefix is out of scope",
			evt:  evt("uploads", "processed/output.mp4", "ObjectCreated:Put"),
			cfg:  docCfg,
			want: false,
		},
		{
			name: "prefix match is case-sensitive",
			evt:  evt("uploads", "RAW/video1.mp4", "ObjectCreated:Put"),
			cfg:  docCfg,
			want: false,
		},
		{
			name: "prefix appearing later in the key does not count as a prefix match",
			evt:  evt("uploads", "incoming/raw/video1.mp4", "ObjectCreated:Put"),
			cfg:  docCfg,
			want: false,
		},

		// --- out of scope: wrong event type (in-scope bucket/prefix) ---
		{
			name: "ObjectRemoved:Delete is out of scope",
			evt:  evt("uploads", "raw/video1.mp4", "ObjectRemoved:Delete"),
			cfg:  docCfg,
			want: false,
		},
		{
			name: "ObjectRemoved:DeleteMarkerCreated is out of scope",
			evt:  evt("uploads", "raw/video1.mp4", "ObjectRemoved:DeleteMarkerCreated"),
			cfg:  docCfg,
			want: false,
		},
		{
			name: "empty event name is out of scope",
			evt:  evt("uploads", "raw/video1.mp4", ""),
			cfg:  docCfg,
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := matches(tc.evt, tc.cfg)
			if got != tc.want {
				t.Errorf("matches(%+v, %+v) = %v, want %v", tc.evt, tc.cfg, got, tc.want)
			}
		})
	}
}
