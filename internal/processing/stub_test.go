package processing

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/guycanella/weir/internal/events"
)

// This file pins the two PURE helpers of the package (ADR-003: functional
// core). Both are total — no I/O, no clock, no randomness — so they are
// exhaustively table-testable, and both are exported so the WR-026
// end-to-end check can compute what it should find in the output bucket
// instead of hardcoding it.

// --- OutputKey -------------------------------------------------------------

// TestOutputKeyMirrorsTheSourceKeyWithTheResultSuffix pins the derivation:
// the result object is namespaced under the SOURCE BUCKET, keeps the source
// object's full path, and gains ResultSuffix — i.e.
// "<bucket>/<key>" + ResultSuffix.
//
// Why this convention, over the alternatives:
//
//   - Namespacing by source bucket is what makes the derivation injective over
//     (bucket, key). Deriving from the key alone would let two DIFFERENT source
//     buckets that happen to hold the same object key silently overwrite each
//     other's results in one shared output bucket — a data-integrity bug, not a
//     cosmetic one, and invisible from the output bucket afterwards.
//   - Mirroring the path makes provenance obvious to a human browsing the
//     output bucket, and makes two different source objects structurally
//     incapable of colliding on one result object.
//   - Appending a suffix (rather than reusing the key verbatim) means that if
//     someone ever misconfigures OutputBucket to equal the input bucket, the
//     result at least never overwrites the very object it was derived from.
//     It is NOT a defense against the notification loop that misconfiguration
//     would cause — preventing that belongs to the infra layer's prefix/
//     suffix filters — just a cheap way to avoid destroying the input.
//   - It is a pure function of (Bucket, Key) alone, so it is stable across
//     redeliveries: the second delivery of an event derives the same output
//     key, which is what makes the write idempotent even in the window where
//     the dedup store has not yet answered.
func TestOutputKeyMirrorsTheSourceKeyWithTheResultSuffix(t *testing.T) {
	cases := []struct {
		name string
		key  string
		want string
	}{
		{
			name: "nested key keeps its full path",
			key:  "raw/video1.mp4",
			want: inputBucket + "/raw/video1.mp4" + ResultSuffix,
		},
		{
			name: "top-level key",
			key:  "video1.mp4",
			want: inputBucket + "/video1.mp4" + ResultSuffix,
		},
		{
			name: "deeply nested key",
			key:  "raw/2026/07/24/video1.mp4",
			want: inputBucket + "/raw/2026/07/24/video1.mp4" + ResultSuffix,
		},
		{
			name: "key with spaces (already URL-decoded by the parser) is used literally",
			key:  "raw/my file name.mp4",
			want: inputBucket + "/raw/my file name.mp4" + ResultSuffix,
		},
		{
			name: "key with no extension",
			key:  "raw/video1",
			want: inputBucket + "/raw/video1" + ResultSuffix,
		},
		{
			name: "non-ASCII key is preserved byte for byte",
			key:  "raw/vídeo–1.mp4",
			want: inputBucket + "/raw/vídeo–1.mp4" + ResultSuffix,
		},
		{
			name: "the suffix is appended unconditionally, even to a key that already ends in it",
			key:  "raw/video1.mp4" + ResultSuffix,
			want: inputBucket + "/raw/video1.mp4" + ResultSuffix + ResultSuffix,
		},
		{
			name: "empty key (out of contract for the parser, but the function stays total)",
			key:  "",
			want: inputBucket + "/" + ResultSuffix,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := OutputKey(events.Event{Bucket: inputBucket, Key: tc.key})
			if got != tc.want {
				t.Errorf("OutputKey(bucket=%q, key=%q) = %q, want %q", inputBucket, tc.key, got, tc.want)
			}
		})
	}
}

// TestOutputKeyDependsOnBucketAndKeyOnly pins both halves of the derivation's
// contract, which pull in opposite directions and are therefore easy to get
// half-right:
//
//   - It DEPENDS on Bucket as well as Key. Two events carrying the same object
//     key from different source buckets are different objects and must resolve
//     to different result objects; collapsing them would mean one silently
//     overwriting the other's result in a shared output bucket.
//   - It IGNORES every other field. A redelivery may legitimately carry a
//     different EventTime (and a re-upload a different Size/ETag/VersionID),
//     and all of those must still resolve to the same output object, so even a
//     double write is an overwrite rather than a duplicate result.
func TestOutputKeyDependsOnBucketAndKeyOnly(t *testing.T) {
	t.Run("a different source bucket yields a different output key", func(t *testing.T) {
		const sharedKey = "raw/video1.mp4"

		a := OutputKey(events.Event{Bucket: "bucket-a", Key: sharedKey})
		b := OutputKey(events.Event{Bucket: "bucket-b", Key: sharedKey})

		if a == b {
			t.Errorf("OutputKey collapsed two source buckets onto one output key (%q) for the shared object "+
				"key %q; the derivation must namespace by Bucket, or one bucket's result silently overwrites "+
				"the other's in a shared output bucket", a, sharedKey)
		}
	})

	t.Run("every field besides bucket and key is ignored", func(t *testing.T) {
		a := events.Event{
			Bucket:    inputBucket,
			Key:       "raw/video1.mp4",
			Size:      1,
			ETag:      "etag-a",
			VersionID: "v1",
			EventName: "ObjectCreated:Put",
			EventTime: mustTime(t, eventTimeStr),
		}
		b := events.Event{
			Bucket:    inputBucket,
			Key:       "raw/video1.mp4",
			Size:      999,
			ETag:      "etag-b",
			VersionID: "v2",
			EventName: "ObjectCreated:CompleteMultipartUpload",
			EventTime: mustTime(t, eventTimeStr).Add(time.Hour),
		}

		if got, want := OutputKey(b), OutputKey(a); got != want {
			t.Errorf("OutputKey differs (%q vs %q) for two events sharing a bucket and object key; the "+
				"derivation must depend on Bucket and Key alone, so redeliveries and re-writes land on the "+
				"same output object", got, want)
		}
	})

	t.Run("a zero-value event is still total", func(t *testing.T) {
		// Out of contract for the parser, but the function must not panic or
		// depend on either field being non-empty.
		if got, want := OutputKey(events.Event{}), "/"+ResultSuffix; got != want {
			t.Errorf("OutputKey(zero value) = %q, want %q — the derivation must stay total", got, want)
		}
	})
}

// TestOutputKeyIsDeterministic states the property directly: repeated calls
// on the same event never differ. A derivation that reached for a timestamp
// or a random suffix would produce a new result object per delivery and make
// the Done-when unverifiable.
func TestOutputKeyIsDeterministic(t *testing.T) {
	evt := putEvent(t, objectPut{key: "raw/video1.mp4", size: 5, etag: "t", versionID: "v"})

	first := OutputKey(evt)
	for i := 0; i < 8; i++ {
		if got := OutputKey(evt); got != first {
			t.Fatalf("call #%d: OutputKey = %q, want %q — the derivation must be deterministic", i+2, got, first)
		}
	}
	if !strings.HasSuffix(first, ResultSuffix) {
		t.Errorf("OutputKey = %q, want it to end in ResultSuffix (%q)", first, ResultSuffix)
	}
	if !strings.HasPrefix(first, inputBucket+"/") {
		t.Errorf("OutputKey = %q, want it namespaced under the source bucket (prefix %q)", first, inputBucket+"/")
	}
	if first == "" {
		t.Error("OutputKey returned an empty key")
	}
}

// --- DefaultStub -----------------------------------------------------------

// TestDefaultStubProducesEventMetadataAsJSON pins the demo's default
// processing body.
//
// It derives the result from event METADATA ONLY, and it has no choice:
// awsclient.S3Client exposes no GetObject, so the uploaded object's content
// is structurally unreachable from here. That is the deliberate scope
// boundary for the demo — a real deployment supplies its own Stub.
//
// The shape pinned is a JSON object carrying the event's identity, i.e.
//
//	{"bucket":"...","key":"...","size":123,"etag":"...","eventName":"..."}
//
// with additional fields allowed. It is asserted structurally (unmarshal,
// then compare fields) rather than as a golden byte string, so key ORDER or
// added fields do not break the suite, while the fields the demo's
// verification step reads stay guaranteed.
func TestDefaultStubProducesEventMetadataAsJSON(t *testing.T) {
	evt := events.Event{
		Bucket:    inputBucket,
		Key:       "raw/my file name.mp4",
		Size:      123456,
		ETag:      "abc123",
		VersionID: "v42",
		EventName: "ObjectCreated:Put",
		EventTime: mustTime(t, eventTimeStr),
	}

	body, err := DefaultStub(evt)
	if err != nil {
		t.Fatalf("DefaultStub returned error %v, want nil: the default body is total, it cannot fail", err)
	}
	if len(body) == 0 {
		t.Fatal("DefaultStub returned an empty body; the result object must not be empty, or the demo has " +
			"nothing to show for the work")
	}

	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("DefaultStub output is not a JSON object (%q): %v — DefaultContentType is %q, so the "+
			"bytes must actually be JSON", body, err, DefaultContentType)
	}

	wantStrings := map[string]string{
		"bucket":    evt.Bucket,
		"key":       evt.Key,
		"etag":      evt.ETag,
		"eventName": evt.EventName,
	}
	for field, want := range wantStrings {
		raw, ok := got[field]
		if !ok {
			t.Errorf("DefaultStub output has no %q field; got %v", field, got)
			continue
		}
		if s, ok := raw.(string); !ok || s != want {
			t.Errorf("DefaultStub output %q = %v, want the event's value %q", field, raw, want)
		}
	}

	raw, ok := got["size"]
	if !ok {
		t.Errorf("DefaultStub output has no %q field; got %v", "size", got)
	} else if n, ok := raw.(float64); !ok || int64(n) != evt.Size {
		t.Errorf("DefaultStub output %q = %v, want %d", "size", raw, evt.Size)
	}
}

// TestDefaultStubIsDeterministic pins that the same event always yields
// byte-identical output. A default body that embedded time.Now() or a random
// id would make "the result was written exactly once" impossible to check by
// content, and would turn any accidental double write into two differing
// objects instead of one idempotent overwrite.
func TestDefaultStubIsDeterministic(t *testing.T) {
	evt := putEvent(t, objectPut{key: "raw/video1.mp4", size: 123456, etag: "abc123", versionID: "v42"})

	first, err := DefaultStub(evt)
	if err != nil {
		t.Fatalf("DefaultStub returned error %v, want nil", err)
	}
	for i := 0; i < 8; i++ {
		got, err := DefaultStub(evt)
		if err != nil {
			t.Fatalf("call #%d: DefaultStub returned error %v, want nil", i+2, err)
		}
		if string(got) != string(first) {
			t.Fatalf("call #%d: DefaultStub = %q, want %q — the default body must not depend on a clock "+
				"or on randomness", i+2, got, first)
		}
	}
}

// TestDefaultStubIsTotal pins that it never errors, for any event it could
// conceivably be handed — including a zero value, which the parser would
// never produce but which a future caller might.
func TestDefaultStubIsTotal(t *testing.T) {
	cases := map[string]events.Event{
		"zero value": {},
		"unversioned object (no VersionID, as on a non-versioned bucket)": {Bucket: inputBucket, Key: "raw/a.mp4", Size: 1, ETag: "t", EventName: "ObjectCreated:Put"},
		"zero size object":                        {Bucket: inputBucket, Key: "raw/empty.txt", Size: 0, ETag: "t", EventName: "ObjectCreated:Put"},
		"key with a double quote and a backslash": {Bucket: inputBucket, Key: `raw/we"ird\path.mp4`, Size: 1, ETag: "t", EventName: "ObjectCreated:Put"},
		"key with a newline":                      {Bucket: inputBucket, Key: "raw/two\nlines.mp4", Size: 1, ETag: "t", EventName: "ObjectCreated:Put"},
	}

	for name, evt := range cases {
		t.Run(name, func(t *testing.T) {
			body, err := DefaultStub(evt)
			if err != nil {
				t.Fatalf("DefaultStub(%+v) returned error %v, want nil", evt, err)
			}
			// The escaping cases are the point here: a hand-rolled string
			// concatenation would emit invalid JSON for a key containing a
			// quote, a backslash or a newline.
			var sink map[string]any
			if err := json.Unmarshal(body, &sink); err != nil {
				t.Errorf("DefaultStub output %q is not valid JSON: %v — build it with encoding/json rather "+
					"than string concatenation, so object keys are escaped", body, err)
			}
		})
	}
}
