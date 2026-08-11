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

// resultPrefix is the fixed prefix every result object must sit under, stated
// here as a test-side literal so a change to the production prefix has to be
// a deliberate, visible edit on both sides rather than something the suite
// follows silently.
const resultPrefix = "results/"

// wantOutputKeyLen is the exact byte length of every output key OutputKey can
// ever produce: len("results/") + 64 hex characters of SHA-256 +
// len(".result.json") = 8 + 64 + 12 = 84. It is a CONSTANT, independent of the
// source bucket name and object key, which is the whole point of the
// derivation (see TestOutputKeyIsBoundedRegardlessOfSourceKeyLength).
const wantOutputKeyLen = len(resultPrefix) + 64 + len(ResultSuffix)

// TestOutputKeyIsTheFixedPrefixPlusTheIdentityHashPlusTheSuffix pins the
// derivation:
//
//	"results/" + idempotency.Key(bucket, key, versionID, etag) + ResultSuffix
//
// Note what this deliberately does NOT do: it does not mirror the source
// bucket or object key into the output path. An earlier revision did, which
// read nicely (provenance was browsable in the output bucket) but was
// unbounded: S3 allows an object key of up to 1024 bytes and a bucket name of
// up to 63, so "<bucket>/<key>/<hash>.result.json" could exceed S3's own
// 1024-byte object-key limit for a perfectly legitimate source object. That
// failure mode is the worst kind — PutObject rejects the key permanently, so
// the result can never be written at all, no matter how many times the message
// is redelivered.
//
// The fixed prefix removes the possibility rather than bounding it by
// convention: every output key is 84 bytes, always.
//
// What is PRESERVED from the earlier revision is the part that carried the
// correctness weight — the identity-hash component, still exactly
// idempotency.Key over (bucket, key, versionID, etag):
//
//   - Including the bucket keeps the derivation injective over (bucket, key),
//     so two DIFFERENT source buckets holding the same object key cannot
//     silently overwrite each other's results in one shared output bucket.
//   - Including versionID and etag extends that injectivity to the full WRITE
//     identity — the same unit the dedup store treats as distinct. Without it,
//     two genuinely different writes to one object key would land on ONE output
//     object, and since the queue is standard rather than FIFO, the older
//     write's event can arrive last and overwrite the newer result with stale
//     data.
//   - Reusing that function (rather than hashing again here) keeps the output
//     key and the dedup key agreeing on what "the same write" means.
//
// Appending a suffix still means that if someone misconfigures OutputBucket to
// equal the input bucket, the result never overwrites the object it was derived
// from. It is not a defense against the resulting notification loop — that
// belongs to the infra layer's prefix/suffix filters — just a cheap way to
// avoid destroying the input.
//
// The table keeps the awkward source keys the earlier path-mirroring version
// used, but inverts what they prove: those keys must now NOT appear in the
// output at all, and every one of them must produce a key of the same fixed
// length. The hash itself is computed by calling idempotency.Key, since
// re-deriving SHA-256 by hand here would test that package, not this one.
func TestOutputKeyIsTheFixedPrefixPlusTheIdentityHashPlusTheSuffix(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{name: "nested key", key: "raw/video1.mp4"},
		{name: "top-level key", key: "video1.mp4"},
		{name: "deeply nested key", key: "raw/2026/07/24/video1.mp4"},
		{name: "key with spaces (already URL-decoded by the parser)", key: "raw/my file name.mp4"},
		{name: "key with no extension", key: "raw/video1"},
		{name: "non-ASCII key", key: "raw/vídeo–1.mp4"},
		{name: "key that already ends in ResultSuffix", key: "raw/video1.mp4" + ResultSuffix},
		{name: "empty key (out of contract for the parser, but the function stays total)", key: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// VersionID and ETag are left zero here; their effect on the hash is
			// covered by TestOutputKeyDependsOnTheWriteIdentityOnly.
			evt := events.Event{Bucket: inputBucket, Key: tc.key}
			want := resultPrefix + idempotency.Key(inputBucket, tc.key, "", "") + ResultSuffix

			got := OutputKey(evt)
			if got != want {
				t.Errorf("OutputKey(bucket=%q, key=%q) = %q, want %q", inputBucket, tc.key, got, want)
			}
			if !strings.HasPrefix(got, resultPrefix) {
				t.Errorf("OutputKey = %q, want it under the fixed prefix %q", got, resultPrefix)
			}
			if !strings.HasSuffix(got, ResultSuffix) {
				t.Errorf("OutputKey = %q, want it to end in ResultSuffix (%q)", got, ResultSuffix)
			}
			if len(got) != wantOutputKeyLen {
				t.Errorf("len(OutputKey) = %d for source key %q, want the fixed %d bytes: the output key's "+
					"length must not depend on the source key's", len(got), tc.key, wantOutputKeyLen)
			}
			// The anti-assertion that replaces the old path-mirroring one: the
			// source identity must survive only as the opaque hash, never as
			// literal text. Skipped for the empty key, which every string
			// trivially contains.
			if tc.key != "" && strings.Contains(got, tc.key) {
				t.Errorf("OutputKey = %q still contains the source key %q verbatim; the derivation must not "+
					"mirror the source path, or a long-but-legal source key can push the output key past "+
					"S3's 1024-byte object-key limit", got, tc.key)
			}
			if strings.Contains(got, inputBucket) {
				t.Errorf("OutputKey = %q still contains the source bucket %q verbatim; the bucket must reach "+
					"the output key only through the identity hash", got, inputBucket)
			}
		})
	}
}

// TestOutputKeyIsBoundedRegardlessOfSourceKeyLength is the boundary case the
// fixed-prefix derivation exists for. S3's own limits are 1024 bytes for an
// object key and 63 for a bucket name, so a source object at the very top of
// its allowance is legal input that Weir must handle — and the path-mirroring
// derivation this replaced could not: it would have produced an output key of
// roughly bucket + key + 1 + 64 + 12 bytes, over the 1024-byte limit, making
// PutObject fail permanently and the result unwritable at any number of
// retries.
//
// Asserting an actual byte bound (not merely "it did not panic") is the point.
// The 1025-byte case is one byte over S3's limit — input the parser should
// never see, included to show the bound holds even then, since the derivation
// makes no assumption about the source key's length at all.
func TestOutputKeyIsBoundedRegardlessOfSourceKeyLength(t *testing.T) {
	// s3MaxKeyLen is S3's documented maximum object-key length in bytes; the
	// output key must stay comfortably under it for every input.
	const s3MaxKeyLen = 1024

	// maxBucket is a 63-byte bucket name, S3's maximum.
	maxBucket := strings.Repeat("b", 63)

	cases := []struct {
		name string
		evt  events.Event
	}{
		{
			name: "source key at exactly S3's 1024-byte maximum",
			evt: events.Event{
				Bucket: inputBucket,
				Key:    "raw/" + strings.Repeat("a", s3MaxKeyLen-len("raw/.mp4")) + ".mp4",
				ETag:   "abc123",
			},
		},
		{
			name: "source key one byte over S3's maximum (out of contract, still bounded)",
			evt: events.Event{
				Bucket: inputBucket,
				Key:    strings.Repeat("a", s3MaxKeyLen+1),
				ETag:   "abc123",
			},
		},
		{
			name: "maximum-length bucket AND maximum-length key together, plus a version id",
			evt: events.Event{
				Bucket:    maxBucket,
				Key:       strings.Repeat("a", s3MaxKeyLen),
				VersionID: strings.Repeat("v", 128),
				ETag:      strings.Repeat("e", 128),
			},
		},
		{
			name: "deeply nested maximum-length key",
			evt: events.Event{
				Bucket: inputBucket,
				Key:    strings.TrimSuffix(strings.Repeat("segment/", s3MaxKeyLen/8), "/"),
				ETag:   "abc123",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Guard the fixture: if the source identity did NOT exceed the limit
			// when concatenated, this case would prove nothing about the bound.
			if naive := len(tc.evt.Bucket) + 1 + len(tc.evt.Key) + 1 + 64 + len(ResultSuffix); naive <= s3MaxKeyLen {
				t.Fatalf("fixture is too short to be a boundary case: a path-mirroring derivation would have "+
					"produced %d bytes, within S3's %d-byte limit", naive, s3MaxKeyLen)
			}

			got := OutputKey(tc.evt)

			if len(got) != wantOutputKeyLen {
				t.Errorf("len(OutputKey) = %d, want the fixed %d bytes; the derivation must not grow with the "+
					"source key (got %q)", len(got), wantOutputKeyLen, got)
			}
			if len(got) >= 200 {
				t.Errorf("len(OutputKey) = %d for a %d-byte source key, want it well under S3's %d-byte "+
					"object-key limit (a 200-byte ceiling leaves ample headroom); PutObject rejects an "+
					"over-long key permanently, so the result could never be written at all",
					len(got), len(tc.evt.Key), s3MaxKeyLen)
			}
			// Still the real derivation, not a truncation of it: the identity
			// hash must be intact, or bounding the length would have cost
			// collision-freedom.
			want := resultPrefix + idempotency.Key(tc.evt.Bucket, tc.evt.Key, tc.evt.VersionID, tc.evt.ETag) + ResultSuffix
			if got != want {
				t.Errorf("OutputKey = %q, want %q — the bound must come from dropping the mirrored path, not "+
					"from truncating the identity hash", got, want)
			}
		})
	}

	// Two maximum-length source keys differing in their LAST byte must still
	// get different output keys: a derivation that bounded its length by
	// truncating the source (rather than hashing it) would collapse them and
	// silently overwrite one result with the other.
	t.Run("two maximum-length keys differing only at the end stay distinct", func(t *testing.T) {
		base := strings.Repeat("a", s3MaxKeyLen-1)
		first := OutputKey(events.Event{Bucket: inputBucket, Key: base + "1", ETag: "t"})
		second := OutputKey(events.Event{Bucket: inputBucket, Key: base + "2", ETag: "t"})
		if first == second {
			t.Errorf("OutputKey collapsed two distinct 1024-byte source keys onto %q; bounding the output "+
				"key's length must not cost injectivity over the source identity", first)
		}
	})
}

// TestOutputKeyEmbedsTheIdentityKeyAsOneOpaqueSegment pins the shape of the
// segment between the fixed prefix and the suffix: exactly one path segment,
// exactly the identity key WR-008 defines. Two things would quietly break here
// — reaching for a different hash (which would make the output key and the
// dedup key disagree about what "the same write" is) and emitting a segment
// containing "/" (which would deepen the output prefix tree unpredictably and
// stop the segment from being recognizable as one unit).
func TestOutputKeyEmbedsTheIdentityKeyAsOneOpaqueSegment(t *testing.T) {
	evt := putEvent(t, objectPut{key: "raw/video1.mp4", size: 5, etag: "abc123", versionID: "v42"})

	got := OutputKey(evt)
	if !strings.HasPrefix(got, resultPrefix) || !strings.HasSuffix(got, ResultSuffix) {
		t.Fatalf("OutputKey = %q, want %q + <identity segment> + %q", got, resultPrefix, ResultSuffix)
	}

	segment := strings.TrimSuffix(strings.TrimPrefix(got, resultPrefix), ResultSuffix)
	if want := idempotency.Key(evt.Bucket, evt.Key, evt.VersionID, evt.ETag); segment != want {
		t.Errorf("identity segment = %q, want idempotency.Key(bucket, key, versionID, etag) = %q — the "+
			"output key and the dedup key must agree on what identifies a write, so reuse that function "+
			"rather than hashing again here", segment, want)
	}
	if strings.Contains(segment, "/") {
		t.Errorf("identity segment %q contains a %q; it must be a single path segment, so every result "+
			"sits directly under %q rather than deepening the prefix tree unpredictably",
			segment, "/", resultPrefix)
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
		// depend on any field being non-empty. With no mirrored path there is
		// nothing to collapse: the empty identity still hashes, so even the zero
		// value yields a well-formed, full-length key.
		want := resultPrefix + idempotency.Key("", "", "", "") + ResultSuffix
		got := OutputKey(events.Event{})
		if got != want {
			t.Errorf("OutputKey(zero value) = %q, want %q — the derivation must stay total", got, want)
		}
		if len(got) != wantOutputKeyLen {
			t.Errorf("len(OutputKey(zero value)) = %d, want the fixed %d bytes", len(got), wantOutputKeyLen)
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
	if !strings.HasPrefix(first, resultPrefix) {
		t.Errorf("OutputKey = %q, want it under the fixed prefix %q", first, resultPrefix)
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
