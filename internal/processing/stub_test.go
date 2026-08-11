package processing

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/guycanella/weir/internal/events"
	"github.com/guycanella/weir/internal/idempotency"
)

// This file pins the two PURE helpers of the package (ADR-003: functional
// core). Both are total — no I/O, no clock, no randomness — so they are
// exhaustively table-testable, and both are exported so the WR-026
// end-to-end check can compute what it should find in the output bucket
// instead of hardcoding it.

// --- OutputKey -------------------------------------------------------------

// TestOutputKeyMirrorsTheSourceKeyWithTheResultSuffix pins the derivation:
// the result object is namespaced under the SOURCE BUCKET, keeps the source
// object's full path, then gains the event's IDENTITY KEY as one final path
// segment and ResultSuffix — i.e.
//
//	"<bucket>/<key>/" + idempotency.Key(bucket, key, versionID, etag) + ResultSuffix
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
//   - Embedding the identity key (WR-008's idempotency.Key over bucket, key,
//     versionID and etag) extends that injectivity from (bucket, key) to the
//     full WRITE identity, which is the unit the dedup store already treats as
//     distinct. Without it, two genuinely different writes to one object key
//     are two fresh events that both land on ONE output object — and because
//     the queue is standard rather than FIFO, delivery order is not guaranteed,
//     so an older write's event can arrive last and silently overwrite the
//     newer result. Reusing the existing hash keeps this to a single opaque,
//     already-tested segment rather than new key-derivation logic here.
//   - Appending a suffix (rather than reusing the key verbatim) means that if
//     someone ever misconfigures OutputBucket to equal the input bucket, the
//     result at least never overwrites the very object it was derived from.
//     It is NOT a defense against the notification loop that misconfiguration
//     would cause — preventing that belongs to the infra layer's prefix/
//     suffix filters — just a cheap way to avoid destroying the input.
//   - It is a pure function of the four identity fields alone, so it is stable
//     across redeliveries: a redelivery carries the same bucket, key, version
//     and etag by definition, so the second delivery derives the same output
//     key. That is what makes the write idempotent even in the window where the
//     dedup store has not yet answered.
//
// The path-shaped half of that contract is spelled out literally per case; the
// hash segment is computed by calling idempotency.Key, since re-deriving
// SHA-256 by hand here would test that package, not this one. What is under
// test is OutputKey's STRUCTURE — bucket, then the mirrored key path, then the
// identity segment, then the suffix.
func TestOutputKeyMirrorsTheSourceKeyWithTheResultSuffix(t *testing.T) {
	cases := []struct {
		name string
		key  string
		// wantPath is the "<bucket>/<key>" prefix the result must sit under,
		// written out literally so a change to the path-mirroring half of the
		// derivation is caught by an explicit expectation.
		wantPath string
	}{
		{
			name:     "nested key keeps its full path",
			key:      "raw/video1.mp4",
			wantPath: inputBucket + "/raw/video1.mp4",
		},
		{
			name:     "top-level key",
			key:      "video1.mp4",
			wantPath: inputBucket + "/video1.mp4",
		},
		{
			name:     "deeply nested key",
			key:      "raw/2026/07/24/video1.mp4",
			wantPath: inputBucket + "/raw/2026/07/24/video1.mp4",
		},
		{
			name:     "key with spaces (already URL-decoded by the parser) is used literally",
			key:      "raw/my file name.mp4",
			wantPath: inputBucket + "/raw/my file name.mp4",
		},
		{
			name:     "key with no extension",
			key:      "raw/video1",
			wantPath: inputBucket + "/raw/video1",
		},
		{
			name:     "non-ASCII key is preserved byte for byte",
			key:      "raw/vídeo–1.mp4",
			wantPath: inputBucket + "/raw/vídeo–1.mp4",
		},
		{
			name:     "the suffix is appended unconditionally, even to a key that already ends in it",
			key:      "raw/video1.mp4" + ResultSuffix,
			wantPath: inputBucket + "/raw/video1.mp4" + ResultSuffix,
		},
		{
			name:     "empty key (out of contract for the parser, but the function stays total)",
			key:      "",
			wantPath: inputBucket + "/",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// VersionID and ETag are left zero here: this test is about the
			// path shape, and TestOutputKeyDependsOnTheWriteIdentityOnly
			// covers their effect.
			evt := events.Event{Bucket: inputBucket, Key: tc.key}
			want := tc.wantPath + "/" + idempotency.Key(inputBucket, tc.key, "", "") + ResultSuffix

			got := OutputKey(evt)
			if got != want {
				t.Errorf("OutputKey(bucket=%q, key=%q) = %q, want %q", inputBucket, tc.key, got, want)
			}
			if !strings.HasPrefix(got, tc.wantPath+"/") {
				t.Errorf("OutputKey = %q, want it to mirror the source path under prefix %q", got, tc.wantPath+"/")
			}
			if !strings.HasSuffix(got, ResultSuffix) {
				t.Errorf("OutputKey = %q, want it to end in ResultSuffix (%q)", got, ResultSuffix)
			}
		})
	}
}

// TestOutputKeyEmbedsTheIdentityKeyAsOneOpaqueSegment pins the shape of the
// segment between the mirrored source path and the suffix: exactly one path
// segment, exactly the identity key WR-008 defines. Two things would quietly
// break here — reaching for a different hash (which would make the output key
// and the dedup key disagree about what "the same write" is) and emitting a
// segment containing "/" (which would deepen the output prefix tree
// unpredictably and stop the segment from being recognizable as one unit).
func TestOutputKeyEmbedsTheIdentityKeyAsOneOpaqueSegment(t *testing.T) {
	evt := putEvent(t, objectPut{key: "raw/video1.mp4", size: 5, etag: "abc123", versionID: "v42"})

	got := OutputKey(evt)
	prefix := evt.Bucket + "/" + evt.Key + "/"
	if !strings.HasPrefix(got, prefix) || !strings.HasSuffix(got, ResultSuffix) {
		t.Fatalf("OutputKey = %q, want %q + <identity segment> + %q", got, prefix, ResultSuffix)
	}

	segment := strings.TrimSuffix(strings.TrimPrefix(got, prefix), ResultSuffix)
	if want := idempotency.Key(evt.Bucket, evt.Key, evt.VersionID, evt.ETag); segment != want {
		t.Errorf("identity segment = %q, want idempotency.Key(bucket, key, versionID, etag) = %q — the "+
			"output key and the dedup key must agree on what identifies a write, so reuse that function "+
			"rather than hashing again here", segment, want)
	}
	if strings.Contains(segment, "/") {
		t.Errorf("identity segment %q contains a %q; it must be a single path segment", segment, "/")
	}
}

// TestOutputKeyDependsOnTheWriteIdentityOnly pins both halves of the
// derivation's contract, which pull in opposite directions and are therefore
// easy to get half-right:
//
//   - It DEPENDS on all four IDENTITY fields: Bucket, Key, VersionID, ETag.
//     Two events differing in any of them are different writes, and must
//     resolve to different result objects. Collapsing them means one result
//     silently overwriting another's — across buckets that share an object key,
//     or, on a versioned bucket, across two writes to one key where the queue
//     is free to deliver the older event last.
//   - It IGNORES everything that is NOT identity: Size, EventName, EventTime.
//     A redelivery of the SAME write may legitimately carry a different
//     EventTime, and it must still resolve to the same output object, so a
//     double write is an idempotent overwrite rather than a second result.
//     This is the same identity/metadata split idempotency.Key makes, and it
//     must stay in lockstep with it.
func TestOutputKeyDependsOnTheWriteIdentityOnly(t *testing.T) {
	base := events.Event{
		Bucket:    inputBucket,
		Key:       "raw/video1.mp4",
		Size:      1,
		ETag:      "etag-a",
		VersionID: "v1",
		EventName: "ObjectCreated:Put",
		EventTime: mustTime(t, eventTimeStr),
	}

	distinguishing := map[string]struct {
		mutate func(*events.Event)
		why    string
	}{
		"a different source bucket": {
			mutate: func(e *events.Event) { e.Bucket = "another-bucket" },
			why: "two source buckets holding the same object key are different objects; sharing one output " +
				"key means one bucket's result silently overwrites the other's in a shared output bucket",
		},
		"a different object key": {
			mutate: func(e *events.Event) { e.Key = "raw/video2.mp4" },
			why:    "two different objects must never share one result object",
		},
		"a different version id": {
			mutate: func(e *events.Event) { e.VersionID = "v2" },
			why: "two versions of one object key are two distinct writes (the dedup store treats them as " +
				"such, so both are processed); the queue is standard, not FIFO, so the older version's " +
				"event may be delivered LAST and would overwrite the newer result with a stale one",
		},
		"a different etag": {
			mutate: func(e *events.Event) { e.ETag = "etag-b" },
			why: "a differing ETag is a different write even on an unversioned bucket, where VersionID is " +
				"empty and is therefore the only thing distinguishing the two",
		},
	}

	for name, tc := range distinguishing {
		t.Run(name+" yields a different output key", func(t *testing.T) {
			other := base
			tc.mutate(&other)

			if got, want := OutputKey(other), OutputKey(base); got == want {
				t.Errorf("OutputKey collapsed two distinct writes onto one output key (%q): %s", got, tc.why)
			}
		})
	}

	nonIdentity := map[string]func(*events.Event){
		"size":      func(e *events.Event) { e.Size = 999 },
		"eventName": func(e *events.Event) { e.EventName = "ObjectCreated:CompleteMultipartUpload" },
		"eventTime": func(e *events.Event) { e.EventTime = mustTime(t, eventTimeStr).Add(time.Hour) },
	}

	for field, mutate := range nonIdentity {
		t.Run("a different "+field+" yields the same output key", func(t *testing.T) {
			other := base
			mutate(&other)

			if got, want := OutputKey(other), OutputKey(base); got != want {
				t.Errorf("OutputKey differs (%q vs %q) for two events of the SAME write that differ only in "+
					"%s; that field is metadata, not identity — a redelivery can carry a different one, and "+
					"it must still land on the same output object", got, want, field)
			}
		})
	}

	t.Run("a zero-value event is still total", func(t *testing.T) {
		// Out of contract for the parser, but the function must not panic or
		// depend on any field being non-empty. Both separators are still
		// emitted, so the empty bucket and the empty key collapse to "//".
		want := "/" + "/" + idempotency.Key("", "", "", "") + ResultSuffix
		if got := OutputKey(events.Event{}); got != want {
			t.Errorf("OutputKey(zero value) = %q, want %q — the derivation must stay total", got, want)
		}
	})
}

// TestOutputKeyIsDeterministic states the property directly: repeated calls
// on the same event never differ. A derivation that reached for a timestamp
// or a random suffix would produce a new result object per delivery and make
// the Done-when unverifiable. Embedding the identity key does not weaken this:
// idempotency.Key is itself pure and deterministic.
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
